using System;
using System.Collections.Immutable;
using System.Runtime.CompilerServices;
using Akka.Actor;
using Akka.Configuration;
using Akka.Event;
using Akka.Util.Internal;
using Eventuate.EventsourcingProtocol;

namespace Eventuate
{
    internal sealed class EventsourcedViewSettings
    {
        public EventsourcedViewSettings(Config config)
        {
            var logConfig = config.GetConfig("eventuate.log");
            var snapshotConfig = config.GetConfig("eventuate.snapshot");

            this.ReplayBatchSize = logConfig.GetInt("replay-batch-size");
            this.ReplayRetryMax = logConfig.GetInt("replay-retry-max");
            this.ReplayRetryDelay = logConfig.GetTimeSpan("replay-retry-delay");
            this.ReadTimeout = logConfig.GetTimeSpan("read-timeout");
            this.LoadTimeout = snapshotConfig.GetTimeSpan("load-timeout");
            this.SaveTimeout = snapshotConfig.GetTimeSpan("save-timeout");
        }

        public int ReplayBatchSize { get; }
        public int ReplayRetryMax { get; }
        public TimeSpan ReplayRetryDelay { get; }
        public TimeSpan ReadTimeout { get; }
        public TimeSpan LoadTimeout { get; }
        public TimeSpan SaveTimeout { get; }
    }

    /// <summary>
    /// An actor that derives internal state from events stored in an event log. Events are pushed from
    /// the `eventLog` actor to this actor and handled with the `onEvent` event handler. An event handler
    /// defines how internal state is updated from events.
    /// 
    /// An `EventsourcedView` can also store snapshots of internal state with its `save` method. During
    /// (re-)start the latest snapshot saved by this actor (if any) is passed as argument to the `onSnapshot`
    /// handler, if the handler is defined at that snapshot. If the `onSnapshot` handler is not defined at
    /// that snapshot or is not overridden at all, event replay starts from scratch. Newer events that are
    /// not covered by the snapshot are handled by `onEvent` after `onSnapshot` returns.
    /// 
    /// By default, an `EventsourcedView` does not define an `aggregateId`. In this case, the `eventLog`
    /// pushes all events to this actor. If it defines an `aggregateId`, the `eventLog` actor only pushes
    /// those events that contain that `aggregateId` value in their `routingDestinations` set.
    /// 
    /// An `EventsourcedView` can only consume events from its `eventLog` but cannot produce new events.
    /// Commands sent to an `EventsourcedView` during recovery are delayed until recovery completes.
    /// 
    /// Event replay is subject to backpressure. After a configurable number of events
    /// (see `eventuate.log.replay-batch-size` configuration parameter), replay is suspended until these
    /// events have been handled by `onEvent` and then resumed again. There's no backpressure mechanism
    /// for live event processing yet (but will come in future releases).
    /// </summary>
    /// <seealso cref="DurableEvent"/>
    /// <seealso cref="EventsourcedActor"/>
    /// <seealso cref="EventsourcedWriter"/>
    /// <seealso cref="EventsourcedProcessor"/>
    public abstract class EventsourcedView : ActorBase, IWithUnboundedStash
    {
        internal static readonly AtomicCounter InstanceIdCounter = new AtomicCounter(0);

        protected readonly int InstanceId = InstanceIdCounter.GetAndIncrement();

        private bool isRecovering = true;
        private bool isEventHandling = false;
        private DurableEvent lastHandledEvent = null;
        private long lastReceivedSequenceNr = 0L;

        private readonly EventsourcedViewSettings settings;
        private readonly Lazy<BehaviorContext> commandContext;
        private readonly Lazy<BehaviorContext> eventContext;
        private readonly Lazy<BehaviorContext> snapshotContext;

        private ImmutableDictionary<SnapshotMetadata, Action<Try<SnapshotMetadata>>> saveRequests = ImmutableDictionary<SnapshotMetadata, Action<Try<SnapshotMetadata>>>.Empty;


        protected EventsourcedView()
        {
            this.settings = new EventsourcedViewSettings(Context.System.Settings.Config);
            this.commandContext = new Lazy<BehaviorContext>(() => new BehaviorContext(OnCommand));
            this.eventContext = new Lazy<BehaviorContext>(() => new BehaviorContext(OnEvent));
            this.snapshotContext = new Lazy<BehaviorContext>(() => new BehaviorContext(OnSnapshot));

            this.ReplayBatchSize = settings.ReplayBatchSize;
            Context.Become(Initiating(settings.ReplayRetryMax));
        }

        public IStash Stash { get; set; }

        /// <summary>
        /// This actor's logging adapter.
        /// </summary>
        public ILoggingAdapter Logger { get; } = Context.GetLogger();

        /// <summary>
        /// Global unique actor id.
        /// </summary>
        public abstract string Id { get; }

        /// <summary>
        /// Event log actor.
        /// </summary>
        public abstract IActorRef EventLog { get; }

        /// <summary>
        /// Optional aggregate id. It is used for routing <see cref="DurableEvent"/>s to event-sourced destinations
        /// which can be <see cref="EventsourcedView"/>s or <see cref="EventsourcedActor"/>s. By default, an event is routed
        /// to an event-sourced destination with an undefined `aggregateId`. If a destination's `aggregateId`
        /// is defined it will only receive events with a matching aggregate id in
        /// <see cref="DurableEvent.DefaultDestinationAggregateId"/>.
        /// </summary>
        public virtual string AggregateId { get; } = null;

        /// <summary>
        /// Maximum number of events to be replayed to this actor before replaying is suspended. A suspended replay
        /// is resumed automatically after all replayed events haven been handled by this actor's event handler
        /// (= backpressure). The default value for the maximum replay batch size is given by configuration item
        /// `eventuate.log.replay-batch-size`. Configured values can be overridden by overriding this method.
        /// </summary>
        public virtual int ReplayBatchSize { get; }

        /// <summary>
        /// Command handler.
        /// </summary>
        protected abstract bool OnCommand(object message);

        /// <summary>
        /// Event handler.
        /// </summary>
        protected abstract bool OnEvent(object message);

        /// <summary>
        /// Snapshot handler.
        /// </summary>
        protected virtual bool OnSnapshot(object message) => true;

        /// <summary>
        /// Recovery completion handler. If called with a <paramref name="failure"/>, the actor will be stopped in
        /// any case, regardless of the action taken by the returned handler. The default handler
        /// implementation does nothing and can be overridden by implementations.
        /// </summary>
        protected virtual void OnRecovery(Exception failure = null) { }

        /// <summary>
        /// Sequence number of the last handled event.
        /// </summary>
        protected long LastSequenceNr
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => lastHandledEvent.LocalSequenceNr;
        }

        /// <summary>
        /// Wall-clock timestamp of the last handled event.
        /// </summary>
        protected DateTime LastSystemTimestamp
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => lastHandledEvent.SystemTimestamp;
        }

        /// <summary>
        /// Vector timestamp of the last handled event.
        /// </summary>
        protected VectorTime LastVectorTimestamp
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => lastHandledEvent.VectorTimestamp;
        }

        /// <summary>
        /// (optional) Emitter aggregate id of the last handled event.
        /// </summary>
        protected string LastEmitterAggregateId
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => lastHandledEvent.EmitterAggregateId;
        }

        /// <summary>
        /// Emitter id of the last handled event.
        /// </summary>
        protected string LastEmitterId
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => lastHandledEvent.EmitterId;
        }

        /// <summary>
        /// Id of the local event log that initially wrote the event.
        /// </summary>
        protected string LastProcessId
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => lastHandledEvent.ProcessId;
        }

        protected BehaviorContext CommandContext
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => commandContext.Value;
        }

        protected BehaviorContext EventContext
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => eventContext.Value;
        }

        protected BehaviorContext SnapshotContext
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => snapshotContext.Value;
        }


        /// <summary>
        /// Returns `true` if this actor is currently recovering internal state by consuming
        /// replayed events from the event log. Returns `false` after recovery completed and
        /// the actor switches to consuming live events.
        /// </summary>
        protected bool IsRecovering
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => this.isRecovering;
        }

        internal bool IsEventHandling
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => this.isEventHandling;
        }

        internal virtual void Recovered()
        {
            isRecovering = false;
            OnRecovery(null);
        }

        internal virtual void ReceiveEvent(DurableEvent e)
        {
            var behavior = eventContext.Value.Current;
            var previous = lastHandledEvent;

            lastHandledEvent = e;
            isEventHandling = true;
            ReceiveEventInternal(e);
            if (behavior(e.Payload))
            {
                if (!isRecovering) VersionChanged(CurrentVersion);
            }
            else
            {
                lastHandledEvent = previous;
            }
            isEventHandling = false;

            lastReceivedSequenceNr = e.LocalSequenceNr;
        }

        internal virtual void ReceiveEventInternal(DurableEvent e) => lastHandledEvent = e;

        internal virtual void ReceiveEventInternal(DurableEvent e, Exception failure) => lastHandledEvent = e;

        internal DurableEvent LastHandledEvent
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => lastHandledEvent;
        }

        public VectorTime CurrentVersion { get; set; } = VectorTime.Zero;

        internal virtual void ConditionalSend(VectorTime condition, object command) =>
            throw new ConditionalRequestException("Actor must extend ConditionalRequests to support ConditionalRequest processing");

        internal virtual void VersionChanged(VectorTime condition) { }

        /// <summary>
        /// Asynchronously saves the given <paramref name="snapshot"/> and calls `handler` with the generated
        /// snapshot metadata. The `handler` can obtain a reference to the initial message
        /// sender with <see cref="ActorBase.Sender"/>.
        /// </summary>
        protected void Save(object snapshot, Action<Try<SnapshotMetadata>> handler)
        {

        }

        /// <summary>
        /// Override to provide an application-defined log sequence number from which event replay will start.
        /// 
        /// If value is returned snapshot loading will be skipped and replay will start from
        /// the given sequence number.
        /// 
        /// If nothing is returned the actor proceeds with the regular snapshot loading procedure.
        /// </summary>
        protected virtual long? ReplayFromSequenceNr { get; } = null;

        internal virtual Snapshot SnapshotCaptured(Snapshot snapshot) => snapshot;

        internal virtual void SnapshotLoaded(Snapshot snapshot) => lastHandledEvent = snapshot.LastEvent;

        internal virtual void UnhandledMessage(object message)
        {
            var behavior = commandContext.Value.Current;
            if (!behavior(message))
                this.Unhandled(message);
        }

        internal virtual void Initialize()
        {
            var sequenceNr = ReplayFromSequenceNr;
            if (sequenceNr.HasValue)
                Replay(sequenceNr.Value, subscribe: true);
            else
                Load();
        }

        internal void Load()
        {
            var iid = InstanceId;

            this.EventLog.Ask(new LoadSnapshot(Id, iid), timeout: settings.LoadTimeout)
                .PipeTo(Self, failure: e => new LoadSnapshotFailure(e, iid));
        }

        internal void Replay(long fromSequenceNr = 1, bool subscribe = false)
        {
            var sub = subscribe ? Self : null;
            var iid = InstanceId;

            this.EventLog.Ask(new Replay(sub, iid, fromSequenceNr, ReplayBatchSize, AggregateId))
                .PipeTo(Self, failure: e => new ReplayFailure(e, fromSequenceNr, iid));
        }

        /// <summary>
        /// Adds the current command to the user's command stash. Must not be used in the event handler.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        public virtual void StashCommand()
        {
            if (isEventHandling)
                throw new StashException($"{nameof(StashCommand)} must not be used in event handler");

            Stash.Stash();
        }

        /// <summary>
        /// Prepends all stashed commands to the actor's mailbox and then clears the command stash.
        /// Has no effect if the actor is recovering i.e. if <see cref="IsRecovering"/> returns `true`.
        /// </summary>
        public virtual void UnstashAll()
        {
            if (!isRecovering)
                Stash.UnstashAll();
        }

        internal virtual Receive Initiating(int replayAttempts) => message =>
        {
            switch (message)
            {
                case LoadSnapshotSuccess s:
                    if (s.InstanceId == InstanceId)
                    {
                        var snapshot = s.Snapshot;
                        if (snapshot is null)
                            Replay(subscribe: true);
                        else
                        {
                            var behavior = snapshotContext.Value.Current;
                            SnapshotLoaded(snapshot);
                            if (behavior(snapshot.Payload))
                            {
                                Replay(snapshot.Metadata.SequenceNr + 1, subscribe: true);
                            }
                            else
                            {
                                Logger.Warning("snapshot loaded (metadata = {0}) but onSnapshot doesn't handle it, replaying from scratch", snapshot.Metadata);
                                Replay(subscribe: true);
                            }
                        }
                    }

                    return true;

                case LoadSnapshotFailure f:
                    if (f.InstanceId == InstanceId)
                        Replay(subscribe: true);
                    return true;

                case ReplaySuccess rs:
                    if (rs.InstanceId == InstanceId)
                    {
                        var finished = true;
                        foreach (var e in rs.Events)
                        {
                            finished = false;
                            ReceiveEvent(e);
                        }

                        if (finished)
                        {
                            Context.Become(Initiated);
                            VersionChanged(CurrentVersion);
                            Recovered();
                            UnstashAll();
                        }
                        else
                        {
                            // reset retry attempts
                            if (replayAttempts != settings.ReplayRetryMax)
                                Context.Become(Initiating(settings.ReplayRetryMax));
                            Replay(rs.ReplayProgress + 1L);
                        }
                    }
                    return true;

                case ReplayFailure rf:
                    if (rf.InstanceId == InstanceId)
                    {
                        var cause = rf.Cause;
                        if (replayAttempts < 1)
                        {
                            Logger.Error(cause, "replay failed (maximum number of {0} replay attempts reached), stopping self", settings.ReplayRetryMax);
                            try { OnRecovery(cause); }
                            catch (Exception e)
                            {
                                Logger.Error(e, "failed while recovering from replay error {0}", cause);
                            }
                            Context.Stop(Self);
                        }
                        else
                        {
                            // retry replay request while decreasing the remaining attempts
                            var attemptsRemaining = replayAttempts - 1;
                            Logger.Warning("replay failed [{0}] ({1} replay attempts remaining), scheduling retry in {2}ms", cause, attemptsRemaining, settings.ReplayRetryDelay.TotalMilliseconds);
                            Context.Become(Initiating(attemptsRemaining));
                            Context.System.Scheduler.ScheduleTellOnce(settings.ReplayRetryDelay, Self, new ReplayRetry(rf.ReplayProgress), Self);
                        }
                    }
                    return true;

                case ReplayRetry rr:
                    Replay(rr.ReplayProgress);
                    return true;

                case Terminated t when t.ActorRef == EventLog:
                    Context.Stop(Self);
                    return true;

                default: StashCommand(); return true;
            }
        };

        internal virtual bool Initiated(object message)
        {
            Action<Try<SnapshotMetadata>> handler;
            switch (message)
            {
                case Written w:
                    if (w.Event.LocalSequenceNr > LastSequenceNr)
                        ReceiveEvent(w.Event);
                    return true;

                case ConditionalRequest cond:
                    ConditionalSend(cond.Condition, cond.Request);
                    return true;

                case SaveSnapshotSuccess success:
                    if (success.InstanceId == InstanceId && this.saveRequests.TryGetValue(success.Metadata, out handler))
                    {
                        handler(Try.Success(success.Metadata));
                        this.saveRequests = this.saveRequests.Remove(success.Metadata);
                    }
                    return true;

                case SaveSnapshotFailure fail:
                    if (fail.InstanceId == InstanceId && this.saveRequests.TryGetValue(fail.Metadata, out handler))
                    {
                        handler(Try.Failure<SnapshotMetadata>(fail.Cause));
                        this.saveRequests = this.saveRequests.Remove(fail.Metadata);
                    }
                    return true;

                case Terminated t when t.ActorRef.Equals(EventLog):
                    Context.Stop(Self);
                    return true;

                default: UnhandledMessage(message); return true;
            }
        }

        protected sealed override bool Receive(object message) => throw new NotImplementedException();

        protected override void PreRestart(Exception reason, object message)
        {
            isRecovering = false;
            base.PreRestart(reason, message);
        }

        /// <summary>
        /// Initiates recovery.
        /// </summary>
        protected override void PreStart()
        {
            lastHandledEvent = new DurableEvent(null, Id);
            Context.Watch(EventLog);
            Initialize();
        }

        /// <summary>
        /// Sets <see cref="IsRecovering"/> to `false` before calling `base.postStop`.
        /// </summary>
        protected override void PostStop()
        {
            isRecovering = false;
            base.PostStop();
        }
    }
}