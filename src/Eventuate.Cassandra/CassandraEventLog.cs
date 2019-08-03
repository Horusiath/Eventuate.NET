#region copyright
// -----------------------------------------------------------------------
//  <copyright file="CassandraEventLog.cs" company="Akka.NET Project">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Dispatch;
using Akka.Event;
using Cassandra;
using Eventuate.EventLogs;
using Eventuate.Snapshots;

namespace Eventuate.Cassandra
{
    public sealed class CassandraEventLogState : IEventLogState
    {
        public CassandraEventLogState(EventLogClock eventLogClock, EventLogClock eventLogClockSnapshot, DeletionMetadata deletionMetadata)
        {
            EventLogClock = eventLogClock;
            EventLogClockSnapshot = eventLogClockSnapshot;
            DeletionMetadata = deletionMetadata;
        }

        public EventLogClock EventLogClock { get; }
        public EventLogClock EventLogClockSnapshot { get; }
        public DeletionMetadata DeletionMetadata { get; }
    }
    
    
    /// <summary>
    /// An event log actor with [[http://cassandra.apache.org/ Apache Cassandra]] as storage backend. It uses
    /// the <see cref="Cassandra"/> extension to connect to a Cassandra cluster. Applications should create an instance
    /// of this actor using the <see cref="Props"/> method.
    /// 
    /// <code>
    ///   ActorRefFactory factory = ... // ActorSystem or ActorContext
    ///   string logId = "example"      // Unique log id
    /// 
    ///   IActorRef log = factory.ActorOf(CassandraEventLog.Props(logId))
    /// </code>
    /// 
    /// Each event log actor creates two tables in the configured keyspace (see also <see cref="Cassandra"/>). Assuming
    /// the following table prefix
    /// 
    /// <code>
    ///   eventuate.log.cassandra.table-prefix = "log"
    /// </code>
    /// 
    /// and a log `id` with value `example`, the names of these two tables are
    /// 
    ///  - `log_example` which represents the local event log.
    ///  - `log_example_agg` which is an index of the local event log for those events that have non-empty
    ///    <see cref="DurableEvent.DestinationAggregateIds"/> set. It is used for fast recovery
    ///    of event-sourced actors, views, stateful writers and processors that have an
    ///    <see cref="EventsourcedView.AggregateId"/> defined.
    /// </summary>
    /// <seealso cref="Cassandra"/>
    /// <seealso cref="DurableEvent"/>
    public sealed class CassandraEventLog : EventLog<CassandraEventLogSettings, CassandraEventLogState>
    {
        private static readonly Regex validCassandraIdentifier = new Regex("^[a-zA-Z0-9_]+$", RegexOptions.Compiled);

        /// <summary>
        /// Check whether the specified <paramref name="logId"/> is valid for Cassandra
        /// table, column and/or keyspace name usage.
        /// </summary>
        private static bool IsValidEventLogId(string logId) => validCassandraIdentifier.IsMatch(logId);
        
        /// <summary>
        /// Creates a <see cref="CassandraEventLog"/> configuration object.
        /// </summary>
        /// <param name="logId">unique log id</param>
        /// <param name="batching">`true` if write-batching shall be enabled (recommended)</param>
        /// <param name="aggregateIndexing">
        /// `true` if aggregates should be indexed (recommended)
        /// Turn this off only if you don't use aggregate IDs on this event log!
        /// </param>
        public static Akka.Actor.Props Props(string logId, ISnapshotStore snapshotStore, bool batching = true, bool aggregateIndexing = true)
        {
            var logProps = Akka.Actor.Props.Create(() => new CassandraEventLog(logId, aggregateIndexing, snapshotStore))
                .WithDispatcher("eventuate.log.dispatchers.write-dispatcher");
            return Akka.Actor.Props.Create(() => new CircuitBreaker(logProps, batching));
        }
        
        private Cassandra cassandra;
        private CassandraReplicationProgressStore progressStore;
        private CassandraDeletedToStore deletedToStore;
        private CassandraEventLogStore eventLogStore;
        private CassandraIndexStore indexStore;

        private IActorRef index = null;
        private long indexSeqNr = 0L;
        private long updateCount = 0L;
        private readonly bool aggregateIndexing;

        /// <summary>
        /// Creates a new instance of <see cref="CassandraEventLog"/>.
        /// </summary>
        /// <param name="id">unique log id</param>
        /// <param name="aggregateIndexing">
        /// `true` if the event log shall process aggregate indexing (recommended).
        /// Turn this off only if you don't use aggregate IDs on this event log!
        /// </param>
        /// <param name="snapshotStore">Store used for snapshotting.</param>
        public CassandraEventLog(string id, bool aggregateIndexing, ISnapshotStore snapshotStore) : base(id)
        {
            if (!IsValidEventLogId(id))
                throw new ArgumentException($"invalid id '{id}' specified - Cassandra allows alphanumeric and underscore characters only");
            
            this.aggregateIndexing = aggregateIndexing;
            this.cassandra = Cassandra.Get(Context.System);
            this.Settings = this.cassandra.Settings;
            this.SnapshotStore = snapshotStore;
        }

        protected override void PreStart()
        {
            ActorTaskScheduler.RunTask(async () =>
            {
                await cassandra.Initialized;

                await cassandra.CreateEventTable(Id);
                await cassandra.CreateAggregateEventTable(Id);

                this.progressStore = CreateReplicationProgressStore(cassandra, Id);
                this.deletedToStore = CreateDeletedToStore(cassandra, Id);
                this.eventLogStore = CreateEventLogStore(cassandra, Id);
                this.indexStore = CreateIndexStore(cassandra, Id);
            });
            
            base.PreStart();
        }

        private static CassandraIndexStore CreateIndexStore(Cassandra c, string id) => new CassandraIndexStore(c, id);
        private static CassandraEventLogStore CreateEventLogStore(Cassandra c, string id) => new CassandraEventLogStore(c, id);
        private static CassandraDeletedToStore CreateDeletedToStore(Cassandra c, string id) => new CassandraDeletedToStore(c, id);
        private static CassandraReplicationProgressStore CreateReplicationProgressStore(Cassandra c, string id) => new CassandraReplicationProgressStore(c, id);

        protected override ISnapshotStore SnapshotStore { get; }
        
        public override async Task WriteReplicationProgresses(ImmutableDictionary<string, long> progresses) => 
            await progressStore.WriteReplicationProgressesAsync(progresses);

        public override async Task<BatchReadResult> Read(long fromSequenceNr, long toSequenceNr, int max) => 
            await eventLogStore.ReadAsync(fromSequenceNr, toSequenceNr, max, max + 1, int.MaxValue, _ => true);

        public override async Task<BatchReadResult> Read(long fromSequenceNr, long toSequenceNr, int max, string aggregateId)
        {
            return await CompositeReadAsync(fromSequenceNr, toSequenceNr, max, max + 1, aggregateId);
        }

        private async Task<BatchReadResult> CompositeReadAsync(long fromSequenceNr,long toSequenceNr, int max, int fetchSize, string aggregateId)
        {
            var enumerator = new CompositeEventEnumerator(indexStore, eventLogStore, aggregateId, fromSequenceNr, indexSeqNr, toSequenceNr, fetchSize);
            var events = new List<DurableEvent>();

            var lastSequenceNr = fromSequenceNr - 1L;
            var scanned = 0;

            while (scanned < max && await enumerator.MoveNextAsync())
            {
                var e = enumerator.Current;
                events.Add(e);
                scanned++;
                lastSequenceNr = e.LocalSequenceNr;
            }
            
            return new BatchReadResult(events, lastSequenceNr);
        }

        public override async Task<BatchReadResult> ReplicationRead(long fromSequenceNr, long toSequenceNr, int max, int scanLimit, Func<DurableEvent, bool> filter) => 
            await eventLogStore.ReadAsync(fromSequenceNr, toSequenceNr, max, QueryOptions.DefaultPageSize, scanLimit, filter);

        public override async Task Write(IReadOnlyCollection<DurableEvent> events, long partition, EventLogClock clock) => 
            await WriteRetry(events, partition, clock);

        public override async Task WriteDeletionMetadata(DeletionMetadata metadata) => 
            await deletedToStore.WriteDeletedTo(metadata.ToSequenceNr);

        public override async Task WriteEventLogClockSnapshot(EventLogClock clock)
        {
            await UpdateIndex(clock);
            await indexStore.WriteEventLogClockSnapshot(clock);
        }

        /**
         * @see [[http://rbmhtechnology.github.io/eventuate/reference/event-sourcing.html#failure-handling Failure handling]]
         */
        private async Task WriteRetry(IReadOnlyCollection<DurableEvent> events, long partition, EventLogClock clock)
        {
            for (int num = 0; num < cassandra.Settings.WriteRetryMax; num++)
            {
                try
                {
                    await WriteBatch(events, partition, clock);
                    Context.Parent.Tell(ServiceEvent.Normal(Id));
                }
                catch (TimeoutException e)
                {
                    Context.Parent.Tell(ServiceEvent.Failed(Id, num, e));
                    Logger.Error(e, "write attempt {0} failed: timeout after {1} - retry now", num,
                        cassandra.Settings.WriteTimeout);
                }
                catch (WriteTimeoutException e)
                {
                    Context.Parent.Tell(ServiceEvent.Failed(Id, num, e));
                    Logger.Error(e, "write attempt {0} failed: retry now", num);
                }
                catch (QueryExecutionException e)
                {
                    Context.Parent.Tell(ServiceEvent.Failed(Id, num, e));
                    Logger.Error(e, "write attempt {0} failed: retry in {1}", num, cassandra.Settings.WriteTimeout);
                    await Task.Delay(cassandra.Settings.WriteTimeout);
                }
                catch (NoHostAvailableException e)
                {
                    Context.Parent.Tell(ServiceEvent.Failed(Id, num, e));
                    Logger.Error(e, "write attempt {0} failed: retry in {1}", num, cassandra.Settings.WriteTimeout);
                    await Task.Delay(cassandra.Settings.WriteTimeout);
                }
                catch (Exception e)
                {
                    Logger.Error(e, "write attempt {0} failed - stop self", num);
                    Context.Stop(Self);
                    throw;
                }
            }
        }

        private async Task WriteBatch(IReadOnlyCollection<DurableEvent> events, long partition, EventLogClock clock)
        {
            await eventLogStore.WriteAsync(events, partition);
            updateCount += events.Count;
            if (updateCount >= cassandra.Settings.IndexUpdateLimit)
            {
                await UpdateIndex(clock);
                updateCount = 0L;
            }
        }

        private async Task<EventLogClock> UpdateIndex(EventLogClock clock)
        {
            if (aggregateIndexing)
            {
                // asynchronously update the index
                var promise = new TaskCompletionSource<CassandraIndex.UpdateIndexSuccess>(TaskCreationOptions.RunContinuationsAsynchronously);
                index.Tell(new CassandraIndex.UpdateIndex(null, clock.SequenceNr, promise));
                await promise.Task.PipeTo(Self);
                return (await promise.Task).Clock;
            }
            else
            {
                // otherwise update the event log clock snapshot only
                return await indexStore.WriteEventLogClockSnapshot(clock);
            }
        }

        protected sealed override void Unhandled(object message)
        {
            if (message is CassandraIndex.UpdateIndexSuccess u)
            {
                indexSeqNr = u.Clock.SequenceNr;    
            }
            else base.Unhandled(message);
        }

        public override async Task<long> ReadReplicationProgress(string logId) => 
            await progressStore.ReadReplicationProgressAsync(logId);

        public override async Task<ImmutableDictionary<string, long>> ReadReplicationProgresses() => 
            await progressStore.ReadReplicationProgressesAsync();

        public override async Task<CassandraEventLogState> RecoverState()
        {
            var dmtask = deletedToStore.ReadDeletedTo();
            var sctask = indexStore.ReadEventLogClockSnapshot();

            await Task.WhenAll(dmtask, sctask);

            var rc = await RecoverEventLogClock(sctask.Result);
            
            return new CassandraEventLogState(rc, sctask.Result, new DeletionMetadata(dmtask.Result, ImmutableHashSet<string>.Empty));
        }

        public override void RecoverStateSuccess(CassandraEventLogState state)
        {
            // if we are not using `aggregateIndexing` set the index' clock to the empty clock
            // so the index will replay the whole event log in case it is requested
            // (which shouldn't happen in the first place if you don't want to use `aggregateIndexing`)

            var indexClock = this.aggregateIndexing ? state.EventLogClockSnapshot : EventLogClock.Empty;
            this.index = CreateIndex(cassandra, indexClock, indexStore, eventLogStore, Id);
            this.indexSeqNr = indexClock.SequenceNr;
            this.updateCount = state.EventLogClock.SequenceNr - indexSeqNr;
            Context.Parent.Tell(ServiceEvent.Initialized(Id));
        }

        private IActorRef CreateIndex(Cassandra c, EventLogClock clock, CassandraIndexStore indexStore, CassandraEventLogStore logStore, string id) => 
            Context.ActorOf(CassandraIndex.Props(c, clock, logStore, indexStore, id));

        private async Task<EventLogClock> RecoverEventLogClock(EventLogClock clock)
        {
            var enumerator = eventLogStore.EventIterator(clock.SequenceNr + 1L, long.MaxValue, cassandra.Settings.IndexUpdateLimit);
            while (await enumerator.MoveNextAsync())
            {
                clock = clock.Update(enumerator.Current);
            }

            return clock;
        }

        sealed class CompositeEventEnumerator : IAsyncEnumerator<DurableEvent>
        {
            private readonly CassandraEventLogStore logStore;
            private readonly string aggregateId;
            private readonly long indexSequenceNr;
            private readonly long toSequenceNr;
            private long last;
            private bool idxr = true;
            private readonly int fetchSize;
            private IAsyncEnumerator<DurableEvent> enumerator;
            private Func<DurableEvent, bool> filter;

            public CompositeEventEnumerator(CassandraIndexStore indexStore, CassandraEventLogStore eventLogStore, string aggregateId, long fromSequenceNr, long indexSequenceNr, long toSequenceNr, int fetchSize)
            {
                logStore = eventLogStore;
                this.aggregateId = aggregateId;
                this.indexSequenceNr = indexSequenceNr;
                this.toSequenceNr = toSequenceNr;
                this.fetchSize = fetchSize;
                this.last = fromSequenceNr - 1L;
                this.filter = _ => true;
                this.enumerator = indexStore.AggregateEventIterator(aggregateId, fromSequenceNr, toSequenceNr, fetchSize);
            }

            public async ValueTask<bool> MoveNextAsync()
            {
                while (true)
                {
                    if (idxr)
                    {
                        if (await enumerator.MoveNextAsync() && filter(enumerator.Current))
                        {
                            last = enumerator.Current.LocalSequenceNr;
                            return true;
                        }
                        else
                        {
                            idxr = false;
                            enumerator = logStore.EventIterator(Math.Max(indexSequenceNr, last) + 1L, toSequenceNr,
                                fetchSize);
                            filter = e => e.DestinationAggregateIds.Contains(aggregateId);
                        }
                    }
                    else if (await enumerator.MoveNextAsync() && filter(enumerator.Current))
                    {
                        last = enumerator.Current.LocalSequenceNr;
                        return true;
                    }
                    else return false;
                }
            }

            public DurableEvent Current => enumerator.Current;
        }
    }
}