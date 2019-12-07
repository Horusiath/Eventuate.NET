#region copyright
// -----------------------------------------------------------------------
//  <copyright file="EventLog.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using Akka.Actor;
using Akka.Dispatch;
using Akka.Event;
using Eventuate.EventsourcingProtocol;
using Eventuate.ReplicationProtocol;
using Eventuate.Snapshots;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Eventuate.EventLogs
{
    internal readonly struct RecoverStateSuccess<T> where T : IEventLogState
    {
        public RecoverStateSuccess(T state)
        {
            State = state;
        }

        public T State { get; }
    }

    internal readonly struct RecoverStateFailure
    {
        public RecoverStateFailure(Exception cause)
        {
            Cause = cause;
        }

        public Exception Cause { get; }
    }

    internal sealed class WriteBatchesSuccess
    {
        public IEnumerable<Write> UpdatedWrites { get; }
        public IEnumerable<DurableEvent> UpdatedEvents { get; }
        public EventLogClock Clock { get; }

        public WriteBatchesSuccess(IEnumerable<Write> upatedWrites, IEnumerable<DurableEvent> updatedEvents, EventLogClock clock)
        {
            this.UpdatedWrites = upatedWrites;
            this.UpdatedEvents = updatedEvents;
            this.Clock = clock;
        }
    }

    internal sealed class WriteBatchesFailure
    {
        public Exception Cause { get; }
        public Write[] Writes { get; }

        public WriteBatchesFailure(Exception cause, Write[] writes)
        {
            this.Cause = cause;
            this.Writes = writes;
        }
    }

    internal sealed class WriteReplicatedBatchesSuccess
    {
        public IEnumerable<ReplicationWrite> UpdatedWrites { get; }
        public IEnumerable<DurableEvent> UpdatedEvents { get; }
        public EventLogClock Clock { get; }

        public WriteReplicatedBatchesSuccess(IEnumerable<ReplicationWrite> upatedWrites, IEnumerable<DurableEvent> updatedEvents, EventLogClock clock)
        {
            this.UpdatedWrites = upatedWrites;
            this.UpdatedEvents = updatedEvents;
            this.Clock = clock;
        }
    }

    internal sealed class WriteReplicatedBatchesFailure
    {
        public Exception Cause { get; }
        public ReplicationWrite[] Writes { get; }

        public WriteReplicatedBatchesFailure(Exception cause, ReplicationWrite[] writes)
        {
            this.Cause = cause;
            this.Writes = writes;
        }
    }

    /// <summary>
    /// Periodically sent to an <see cref="EventLog"/> after reception of a <see cref="Delete"/>-command to
    /// instruct the log to physically delete logically deleted events that are alreday replicated.
    /// </summary>
    /// <seealso cref="DeletionMetadata"/>
    internal readonly struct PhysicalDelete { }

    /// <summary>
    /// Internally sent to an <see cref="EventLog"/> after successful physical deletion
    /// </summary>
    internal readonly struct PhysicalDeleteSuccess
    {
        public PhysicalDeleteSuccess(long deletedTo)
        {
            DeletedTo = deletedTo;
        }

        public long DeletedTo { get; }
    }

    /// <summary>
    /// Internally sent to an <see cref="EventLog"/> after failed physical deletion
    /// </summary>
    internal readonly struct PhysicalDeleteFailure
    {
        public PhysicalDeleteFailure(Exception cause)
        {
            Cause = cause;
        }

        public Exception Cause { get; }
    }

    /// <summary>
    /// An abstract event log that handles eventsourcing and replication protocol messages and
    /// translates them to read and write operations declared on the <see cref="IEventLog{TSettings, TState}"/> trait. Storage providers
    /// implement an event log by implementing the <see cref="IEventLog{TSettings, TState}"/>.
    /// </summary>
    /// <typeparam name="TSettings">An event log settigns type (must implement <see cref="IEventLogSettings"/>).</typeparam>
    /// <typeparam name="TState">An Event log state type (must implement <see cref="IEventLogState"/>)</typeparam>
    public abstract class EventLog<TSettings, TState> : ActorBase, IEventLog<TSettings, TState>, IWithUnboundedStash
        where TSettings : IEventLogSettings
        where TState : IEventLogState
    {
        private readonly MessageDispatcher readDispatcher;
        private readonly IScheduler scheduler;

        //TODO: only transfer version vector deltas to update replicaVersionVectors

        /// <summary>
        /// The clock that tracks the sequence number and version vector of this event log. The sequence
        /// number is the log's logical time. The version vector is the merge result of vector timestamps
        /// of all events that have been written to this event log. The version vector is used to exclude
        /// events from being written if they are in the event log's causal past (which makes replication
        /// writes idempotent).
        /// </summary>
        private EventLogClock clock;

        /// <summary>
        /// Current <see cref="DeletionMetadata"/>.
        /// </summary>
        private DeletionMetadata deletionMetadata = new DeletionMetadata(0, ImmutableHashSet<string>.Empty);

        /// <summary>
        /// A flag indicating if a physical deletion process is currently running in the background.
        /// </summary>
        private bool physicalDeletionRunning = false;

        /// <summary>
        /// An cache for the remote replication progress.
        /// The remote replication progress is the sequence nr in the local log up to which
        /// a remote log has replicated events. Events with a sequence number less than or equal
        /// the corresponding replication progress are allowed to be physically deleted locally.
        /// </summary>
        private ImmutableDictionary<string, long> remoteReplicationProgress = ImmutableDictionary<string, long>.Empty;

        /// <summary>
        /// Cached version vectors of event log replicas. They are used to exclude redundantly read events from
        /// being transferred to a replication target. This is an optimization to save network bandwidth. Even
        /// without this optimization, redundantly transferred events are reliably excluded at the target site,
        /// using its local version vector. The version vector cache is continuously updated during event
        /// replication.
        /// </summary>
        private ImmutableDictionary<string, VectorTime> replicaVersionVectors = ImmutableDictionary<string, VectorTime>.Empty;

        /// <summary>
        /// Registry for event-sourced actors, views, writers and processors interacting with this event log.
        /// </summary>
        private SubscriberRegistry registry = new SubscriberRegistry();

        /// <summary>
        /// Optional channel to notify <see cref="Replicator"/>s, reading from this event log, about updates.
        /// </summary>
        private readonly IActorRef channel;

        protected EventLog(string id)
        {
            this.Id = id;
            this.readDispatcher = Context.System.Dispatchers.Lookup("eventuate.log.dispatchers.read-dispatcher");
            this.scheduler = Context.System.Scheduler;
            this.channel =
                Context.System.Settings.Config.GetBoolean("eventuate.log.replication.update-notifications") ? Context.ActorOf(Props.Create(() => new NotificationChannel(Id))) : null;
            this.Logger = Context.GetLogger();
        }

        /// <summary>
        /// This event log's logging adapter.
        /// </summary>
        public ILoggingAdapter Logger { get; }

        /// <summary>
        /// This event log's snapshot store.
        /// </summary>
        protected abstract ISnapshotStore SnapshotStore { get; }

        public IStash Stash { get; set; }

        public TSettings Settings { get; protected set; }
        public string Id { get; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected sealed override bool Receive(object message) => Initializing(message);

        private bool Initializing(object message)
        {
            switch (message)
            {
                case RecoverStateSuccess<TState> s:
                    var state = s.State;
                    this.clock = state.EventLogClock;
                    this.deletionMetadata = state.DeletionMetadata;
                    if (deletionMetadata.ToSequenceNr > 0)
                        Self.Tell(new PhysicalDelete());
                    RecoverStateSuccess(state);
                    Stash.UnstashAll();
                    Context.Become(Initialized);
                    return true;

                case RecoverStateFailure f:
                    Logger.Error(f.Cause, "Cannot recover event log state");
                    RecoverStateFailure(f.Cause);
                    Context.Stop(Self);
                    return true;

                default:
                    Stash.Stash();
                    return true;
            }
        }

        private bool Initialized(object message)
        {
            void OnGetReplicationProgresses()
            {
                ReadReplicationProgresses().PipeTo(Sender,
                    success: data => new GetReplicationProgressesSuccess(data),
                    failure: cause => new GetReplicationProgressesFailure(cause));
            }

            void OnGetReplicationProgress(string sourceLogId)
            {
                var version = clock.VersionVector;
                ReadReplicationProgress(sourceLogId).PipeTo(Sender,
                    success: data => new GetReplicationProgressSuccess(sourceLogId, data, version),
                    failure: cause => new GetReplicationProgressFailure(cause));
            }

            void OnSetReplicationProgress(SetReplicationProgress m)
            {
                WriteReplicationProgresses(ImmutableDictionary<string, long>.Empty.Add(m.SourceLogId, m.ReplicationProgress))
                    .PipeTo(Sender,
                        success: () => new SetReplicationProgressSuccess(m.SourceLogId, m.ReplicationProgress),
                        failure: cause => new SetReplicationProgressFailure(cause));
            }

            void OnReplay(Replay replay)
            {
                if (replay.AggregateId is null)
                {
                    if (!(replay.Subscriber is null))
                        registry = registry.RegisterDefaultSubscriber(replay.Subscriber);

                    var from = replay.FromSequenceNr;
                    var iid = replay.InstanceId;
                    Read(AdjustFromSequenceNr(from), clock.SequenceNr, replay.Max)
                        .PipeTo(Sender,
                            success: data => new ReplaySuccess(data.Events, data.To, iid),
                            failure: cause => new ReplayFailure(cause, from, iid));
                }
                else
                {
                    var emitterAggregateId = replay.AggregateId;
                    if (!(replay.Subscriber is null))
                        registry = registry.RegisterAggregateSubscriber(replay.Subscriber, emitterAggregateId);

                    var from = replay.FromSequenceNr;
                    var iid = replay.InstanceId;
                    Read(AdjustFromSequenceNr(from), clock.SequenceNr, replay.Max, emitterAggregateId)
                        .PipeTo(Sender,
                            success: data => new ReplaySuccess(data.Events, data.To, iid),
                            failure: cause => new ReplayFailure(cause, from, iid));
                }
            }

            void OnReplicationRead(ReplicationRead read)
            {
                if (!(channel is null)) channel.Tell(read);

                var from = read.FromSequenceNr;
                var targetLogId = read.TargetLogId;
                remoteReplicationProgress = remoteReplicationProgress.SetItem(targetLogId, Math.Max(0, from - 1));
                ReplicationRead(from, clock.SequenceNr, read.Max, read.ScanLimit, e => e.IsReplicable(read.CurrentTargetVersionVector, read.Filter))
                    .PipeTo(Self, Sender,
                        success: data => new ReplicationReadSuccess(data.Events.ToArray(), from, data.To, targetLogId, null),
                        failure: cause => new ReplicationReadFailure(new ReplicationReadSourceException(cause.Message), targetLogId));
            }

            void OnReplicationReadSuccess(ReplicationReadSuccess success)
            {
                // Post-exclude events using a possibly updated version vector received from the
                // target. This is an optimization to save network bandwidth. If omitted, events
                // are still excluded at target based on the current local version vector at the
                // target (for correctness).
                var targetLogId = success.TargetLogId;
                var currentTargetVersionVector = this.replicaVersionVectors.GetValueOrDefault(targetLogId, VectorTime.Zero);
                var updated = new List<DurableEvent>(success.Events.Count);
                foreach (var e in success.Events)
                {
                    if (!e.IsBefore(currentTargetVersionVector))
                        updated.Add(e);
                }
                var reply = new ReplicationReadSuccess(updated, success.FromSequenceNr, success.ReplicationProgress, success.TargetLogId, clock.VersionVector);
                Sender.Tell(reply);
                if (!(channel is null)) channel.Tell(reply);
                LogFilterStatistics("source", success.Events, updated);
            }

            void OnReplicationReadFailure(ReplicationReadFailure failure)
            {
                Sender.Tell(failure);
                if (!(channel is null)) channel.Tell(failure);
            }

            void OnDelete(Delete delete)
            {
                var actualDeletedToSeqNr = Math.Max(Math.Min(delete.ToSequenceNr, clock.SequenceNr), deletionMetadata.ToSequenceNr);
                if (actualDeletedToSeqNr > deletionMetadata.ToSequenceNr)
                {
                    var updatedDeletionMetadata = new DeletionMetadata(actualDeletedToSeqNr, delete.RemoteLogIds);
                    WriteDeletionMetadata(updatedDeletionMetadata).PipeTo(Self, Sender,
                        success: () => new DeleteSuccess(updatedDeletionMetadata.ToSequenceNr, updatedDeletionMetadata.RemoteLogIds),
                        failure: cause => new DeleteFailure(cause));
                }
            }

            void OnDeleteSuccess(DeleteSuccess success)
            {
                this.deletionMetadata = new DeletionMetadata(success.DeletedTo, success.RemoteLogIds ?? ImmutableHashSet<string>.Empty);
                Self.Tell(new PhysicalDelete());
                Sender.Tell(success);
            }

            void OnPhysicalDelete()
            {
                if (!physicalDeletionRunning)
                {
                    // Becomes Long.MaxValue in case of an empty-set to indicate that all event are replicated as required
                    var replicatedSeqNr = deletionMetadata.RemoteLogIds.IsEmpty ? long.MaxValue : deletionMetadata.RemoteLogIds.Select(id => remoteReplicationProgress.GetValueOrDefault(id, 0)).Min();
                    var deleteTo = Math.Min(deletionMetadata.ToSequenceNr, replicatedSeqNr);
                    physicalDeletionRunning = true;
                    Delete(deleteTo).PipeTo(Self,
                        success: data => new PhysicalDeleteSuccess(data),
                        failure: cause => new PhysicalDeleteFailure(cause));
                }
            }

            void OnPhysicalDeleteSuccess(long deletedTo)
            {
                physicalDeletionRunning = false;
                if (deletionMetadata.ToSequenceNr > deletedTo)
                {
                    scheduler.ScheduleTellOnce(Settings.DeletionRetryDelay, Self, new PhysicalDelete(), Self);
                }
            }

            void OnPhysicalDeleteFailure(Exception cause)
            {
                if (!(cause is PhysicalDeletionNotSupportedException))
                {
                    var delay = Settings.DeletionRetryDelay;
                    Logger.Error(cause, "Physical deletion of events failed. Retry in {0}", delay);
                    physicalDeletionRunning = false;
                    scheduler.ScheduleTellOnce(delay, Self, new PhysicalDelete(), Self);
                }
            }

            void OnLoadSnapshot(LoadSnapshot load)
            {
                var emitterId = load.EmitterId;
                var iid = load.InstanceId;
                SnapshotStore.Load(emitterId).PipeTo(Sender,
                    success: data => new LoadSnapshotSuccess(data, iid),
                    failure: cause => new LoadSnapshotFailure(cause, iid));
            }

            void OnSaveSnapshot(SaveSnapshot save)
            {
                var snapshot = save.Snapshot;
                var iid = save.InstanceId;
                SnapshotStore.Save(snapshot).PipeTo(Sender, save.Initiator,
                    success: () => new SaveSnapshotSuccess(snapshot.Metadata, iid),
                    failure: cause => new SaveSnapshotFailure(snapshot.Metadata, cause, iid));
            }

            void OnDeleteSnapshots(long lowerSequenceNr)
            {
                SnapshotStore.Delete(lowerSequenceNr).PipeTo(Sender,
                    success: () => new DeleteSnapshotsSuccess(),
                    failure: cause => new DeleteSnapshotsFailure(cause));
            }

            void OnAdjustEventLogClock()
            {
                clock = clock.AdjustSequenceNrToProcessTime(Id);
                WriteEventLogClockSnapshot(clock, Context).PipeTo(Sender,
                    success: () => new AdjustEventLogClockSuccess(clock),
                    failure: cause => new AdjustEventLogClockFailure(cause));
            }

            void OnWriteBatchesSuccess(WriteBatchesSuccess success)
            {
                this.clock = success.Clock;
                foreach (var write in success.UpdatedWrites)
                {
                    write.ReplyTo.Tell(new WriteSuccess(write.Events, write.CorrelationId, write.InstanceId), write.Initiator);
                    registry.NotifySubscribers(write.Events, s => !s.Equals(write.ReplyTo));
                }

                if (!(channel is null)) channel.Tell(new NotificationChannel.Updated(success.UpdatedEvents));
            }

            void OnWriteBatchesFailure(WriteBatchesFailure failure)
            {
                foreach (var write in failure.Writes)
                {
                    write.ReplyTo.Tell(new WriteFailure(write.Events, failure.Cause, write.CorrelationId, write.InstanceId), write.Initiator);
                }
            }

            void OnWriteReplicatedBatchesSuccess(WriteReplicatedBatchesSuccess success)
            {
                this.clock = success.Clock;
                foreach (var w in success.UpdatedWrites)
                {
                    var builder = ImmutableDictionary.CreateBuilder<string, ReplicationMetadata>();
                    foreach (var (k,v) in w.Metadata)
                    {
                        builder[k] = v.WithVersionVector(success.Clock.VersionVector);
                    }

                    var ws = new ReplicationWriteSuccess(w.Events, builder.ToImmutable(), w.ContinueReplication);
                    registry.NotifySubscribers(w.Events);
                    if (!(channel is null)) channel.Tell(w);

                    // Write failure of replication progress can be ignored. Using a stale
                    // progress to resume replication will redundantly read events from a
                    // source log but these events will be successfully identified as
                    // duplicates, either at source or latest at target.
                    WriteReplicationProgresses(w.ReplicationProgresses.ToImmutableDictionary()).PipeTo(w.ReplyTo,
                        success: () => ws,
                        failure: cause =>
                        {
                            Logger.Warning("Writing of replication progress failed: {0}", cause);
                            return new ReplicationWriteFailure(cause);
                        });
                }

                if (!(channel is null)) channel.Tell(new NotificationChannel.Updated(success.UpdatedEvents));
            }

            void OnWriteReplicatedBatchesFailure(WriteReplicatedBatchesFailure failure)
            {
                foreach (var write in failure.Writes)
                {
                    write.ReplyTo.Tell(new ReplicationWriteFailure(failure.Cause));
                }
            }

            switch (message)
            {
                case GetEventLogClock _: Sender.Tell(new GetEventLogClockSuccess(this.clock)); return true;
                case GetReplicationProgresses _: OnGetReplicationProgresses(); return true;
                case GetReplicationProgress _: OnGetReplicationProgress(((GetReplicationProgress)message).SourceLogId); return true;
                case SetReplicationProgress _: OnSetReplicationProgress((SetReplicationProgress)message); return true;
                case Replay _: OnReplay((Replay)message); return true;
                case ReplicationRead _: OnReplicationRead((ReplicationRead)message); return true;
                case ReplicationReadSuccess _: OnReplicationReadSuccess((ReplicationReadSuccess)message); return true;
                case ReplicationReadFailure _: OnReplicationReadFailure((ReplicationReadFailure)message); return true;
                case Write _: ProcessWrites(((Write)message).WithReplyToDefault(Sender)); return true;
                case WriteMany _:
                    ProcessWrites(((WriteMany)message).Writes.ToArray());
                    Sender.Tell(WriteManyComplete.Instance);
                    return true;
                case WriteBatchesSuccess _: OnWriteBatchesSuccess((WriteBatchesSuccess)message); return true;
                case WriteBatchesFailure _: OnWriteBatchesFailure((WriteBatchesFailure)message); return true;
                case ReplicationWrite _: ProcessReplicationWrites(((ReplicationWrite)message).WithReplyToDefault(Sender)); return true;
                case WriteReplicatedBatchesSuccess _: OnWriteReplicatedBatchesSuccess((WriteReplicatedBatchesSuccess)message); return true;
                case WriteReplicatedBatchesFailure _: OnWriteReplicatedBatchesFailure((WriteReplicatedBatchesFailure)message); return true;
                case ReplicationWriteMany _:
                    ProcessReplicationWrites(((ReplicationWriteMany)message).Writes.ToArray());
                    Sender.Tell(new ReplicationWriteManyComplete());
                    return true;
                case Delete _: OnDelete((Delete)message); return true;
                case DeleteSuccess _: OnDeleteSuccess((DeleteSuccess)message); return true;
                case DeleteFailure _: Sender.Tell(message); return true;
                case PhysicalDelete _: OnPhysicalDelete(); return true;
                case PhysicalDeleteSuccess _: OnPhysicalDeleteSuccess(((PhysicalDeleteSuccess)message).DeletedTo); return true;
                case PhysicalDeleteFailure _: OnPhysicalDeleteFailure(((PhysicalDeleteFailure)message).Cause); return true;
                case LoadSnapshot _: OnLoadSnapshot((LoadSnapshot)message); return true;
                case SaveSnapshot _: OnSaveSnapshot((SaveSnapshot)message); return true;
                case DeleteSnapshots _: OnDeleteSnapshots(((DeleteSnapshots)message).LowerSequenceNr); return true;
                case AdjustEventLogClock _: OnAdjustEventLogClock(); return true;
                case Terminated _:
                    registry = registry.UnregisterSubscriber(((Terminated)message).ActorRef);
                    return true;
                default: return false;
            }
        }

        protected override void PreStart()
        {
            var self = Self;
            Task.Run(async () =>
            {
                var remaining = Settings.InitRetryMax;
                while (true)
                {
                    try
                    {
                        return await RecoverState();
                    }
                    catch when (remaining > 0)
                    {
                        remaining--;
                        await Task.Delay(Settings.InitRetryDelay);
                    }
                }
            }).PipeTo(Self,
                success: s => new RecoverStateSuccess<TState>(s),
                failure: cause => new RecoverStateFailure(cause));
        }

        private void ProcessWrites(params Write[] writes)
        {
            WriteBatches(writes, (events, clock) => PrepareEvents(events, clock, CurrentSystemTime), Context).PipeTo(Self, Sender,
                success: tuple => new WriteBatchesSuccess(tuple.Item1, tuple.Item2, tuple.Item3),
                failure: cause => new WriteBatchesFailure(cause, writes));
        }

        private void ProcessReplicationWrites(params ReplicationWrite[] writes)
        {
            foreach (var w in writes)
                foreach (var (id, m) in w.Metadata)
                {
                    this.replicaVersionVectors = replicaVersionVectors.SetItem(id, m.CurrentVersionVector);
                }

            WriteBatches(writes, (events, clock) => PrepareReplicatedEvents(events.ToArray(), clock, CurrentSystemTime), Context).PipeTo(Self, Sender,
                success: tuple => new WriteReplicatedBatchesSuccess(writes, tuple.Item2, tuple.Item3),
                failure: cause => new WriteReplicatedBatchesFailure(cause, writes));
        }
        
        protected virtual DateTime CurrentSystemTime => DateTime.UtcNow;

        protected virtual long AdjustFromSequenceNr(long sequenceNr) => Math.Max(sequenceNr, deletionMetadata.ToSequenceNr + 1);

        public virtual Task<long> Delete(long toSequenceNr) => throw new PhysicalDeletionNotSupportedException();

        public abstract Task<BatchReadResult> Read(long fromSequenceNr, long toSequenceNr, int max);

        public abstract Task<BatchReadResult> Read(long fromSequenceNr, long toSequenceNr, int max, string aggregateId);

        public abstract Task<long> ReadReplicationProgress(string logId);

        public abstract Task<ImmutableDictionary<string, long>> ReadReplicationProgresses();

        public abstract Task<TState> RecoverState();

        public virtual void RecoverStateFailure(Exception cause)
        {
        }

        public virtual void RecoverStateSuccess(TState state)
        {
        }

        public abstract Task<BatchReadResult> ReplicationRead(long fromSequenceNr, long toSequenceNr, int max, int scanLimit, Func<DurableEvent, bool> filter);

        public abstract Task Write(IReadOnlyCollection<DurableEvent> events, long partition, EventLogClock clock, IActorContext context);

        public abstract Task WriteDeletionMetadata(DeletionMetadata metadata);

        public abstract Task WriteEventLogClockSnapshot(EventLogClock clock, IActorContext context);

        public abstract Task WriteReplicationProgresses(ImmutableDictionary<string, long> progresses);

        private async Task<(IEnumerable<T>, IEnumerable<DurableEvent>, EventLogClock)> WriteBatches<T>(IReadOnlyCollection<T> writes, Func<IEnumerable<DurableEvent>, EventLogClock, (IEnumerable<DurableEvent>, EventLogClock)> prepare, IActorContext context)
            where T : IUpdateableEventBatch<T>
        {
            var totalSize = 0;
            foreach (var w in writes) totalSize += w.Count;
            var (partition, clock1) = AdjustSequenceNr(totalSize, Settings.PartitionSize, clock);
            var (updatedWrites, clock2) = PrepareBatches(writes, clock1, prepare);
            var updatedEvents = updatedWrites.SelectMany(w => w.Events).ToImmutableArray();

            await Write(updatedEvents, partition, clock2, context);

            return (updatedWrites, updatedEvents, clock2);
        }

        private (IReadOnlyCollection<T>, EventLogClock) PrepareBatches<T>(IReadOnlyCollection<T> writes, EventLogClock clock, Func<IEnumerable<DurableEvent>, EventLogClock, (IEnumerable<DurableEvent>, EventLogClock)> prepare) 
            where T : IUpdateableEventBatch<T>
        {
            var resultClock = clock;
            var resultBatches = new List<T>();
            foreach (var write in writes)
            {
                var (updated, clock3) = prepare(write.Events, resultClock);
                resultClock = clock3;
                resultBatches.Add(write.Update(updated.ToArray()));
            }

            return (resultBatches, resultClock);
        }

        private (IEnumerable<DurableEvent>, EventLogClock) PrepareEvents(IEnumerable<DurableEvent> events, EventLogClock clock, DateTime systemTimestamp)
        {
            var sequenceNr = clock.SequenceNr;
            var versionVector = clock.VersionVector;
            var updated = new List<DurableEvent>();
            foreach (var e in events)
            {
                sequenceNr++;
                var e2 = e.Prepare(Id, sequenceNr, systemTimestamp);
                versionVector = versionVector.Merge(e2.VectorTimestamp);
                updated.Add(e2);
            }

            return (updated, new EventLogClock(sequenceNr, versionVector));
        }

        private (IEnumerable<DurableEvent>, EventLogClock) PrepareReplicatedEvents(IReadOnlyCollection<DurableEvent> events, EventLogClock clock, DateTime systemTimestamp)
        {
            var sequenceNr = clock.SequenceNr;
            var versionVector = clock.VersionVector;
            var updated = new List<DurableEvent>();
            foreach (var e in events)
            {
                if (!e.IsBefore(clock.VersionVector))
                {
                    // Exclude events from writing that are in the causal past of this event log. Excluding
                    // them at the target is needed for correctness. Events are also excluded at sources
                    // (to save network bandwidth) but this is only an optimization which cannot achieve
                    // 100% filtering coverage for certain replication network topologies.

                    sequenceNr++;
                    var eventSystemTimestamp = e.SystemTimestamp == default ? systemTimestamp : e.SystemTimestamp;
                    var e2 = e.Prepare(Id, sequenceNr, eventSystemTimestamp);
                    versionVector = versionVector.Merge(e2.VectorTimestamp);
                    updated.Add(e2);
                }
            }

            LogFilterStatistics("target", events, updated);
            return (updated, new EventLogClock(sequenceNr, versionVector));
        }

        private void LogFilterStatistics(string location, IReadOnlyCollection<DurableEvent> before, IReadOnlyCollection<DurableEvent> after)
        {
            var bl = before.Count;
            var al = after.Count;
            var diff = bl - al;
            if (diff > 0)
            {
                var perc = diff * 100.0 / bl;
                Logger.Info("[{0}] excluded {1} events ({2} at {3})", Id, diff, perc, location);
            }
        }

        /// <summary>
        /// Partition number for given <paramref name="sequenceNr"/>.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long PartitionOf(long sequenceNr, long partitionSizeMax) =>
            (sequenceNr == 0L) ? -1L : (sequenceNr - 1L) / partitionSizeMax;

        /// <summary>
        /// Remaining partition size given the current <paramref name="sequenceNr"/>.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long RemainingPartitionSize(long sequenceNr, long partitionSizeMax)
        {
            var m = sequenceNr % partitionSizeMax;
            return m == 0L ? m : partitionSizeMax - m;
        }

        /// <summary>
        /// First sequence number of given <paramref name="partition"/>.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long FirstSequenceNr(long partition, long partitionSizeMax) => partition * partitionSizeMax + 1L;

        /// <summary>
        /// Last sequence number of given <paramref name="partition"/>.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long LastSequenceNr(long partition, long partitionSizeMax) => (partition + 1L) * partitionSizeMax;

        /// <summary>
        /// Adjusts <see cref="EventLogClock.SequenceNr"/> if a batch of <paramref name="batchSize"/> doesn't fit in the current partition.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        public static (long, EventLogClock) AdjustSequenceNr(long batchSize, long maxBatchSize, EventLogClock clock)
        {
            if (batchSize > maxBatchSize)
                throw new ArgumentException($"write batch size ({batchSize}) must not be greater than maximum partition size ({maxBatchSize})", nameof(batchSize));

            var currentPartition = PartitionOf(clock.SequenceNr, maxBatchSize);
            var remainingSize = RemainingPartitionSize(clock.SequenceNr, maxBatchSize);
            if (remainingSize < batchSize)
                return (currentPartition + 1L, clock.AdvanceSequenceNr(remainingSize));
            else
                return (currentPartition, clock);
        }
    }
}
