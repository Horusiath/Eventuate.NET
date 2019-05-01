using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Runtime.CompilerServices;
using Akka.Actor;
using Akka.Configuration;
using Eventuate.EventsourcingProtocol;

namespace Eventuate
{
    internal sealed class EventsourcedActorSettings
    {
        public EventsourcedActorSettings(Config config)
        {
            this.WriteTimeout = config.GetTimeSpan("eventuate.log.write-timeout");
        }

        public TimeSpan WriteTimeout { get; }
    }

    /// <summary>
    /// An <see cref="EventsourcedActor"/> is an <see cref="EventsourcedView"/> that can also write new events to its event log.
    /// New events are written with the asynchronous <see cref="Persist"/> methods. They must only
    /// be used within the <see cref="EventsourcedView.OnCommand(object)"/> command handler. After successful persistence, the <see cref="EventsourcedView.OnEvent(object)"/> handler
    /// is automatically called with the persisted event(s). The <see cref="EventsourcedView.OnEvent(object)"/> handler is the place where actor
    /// state may be updated. The <see cref="EventsourcedView.OnCommand(object)"/> handler should not update actor state but only read it e.g.
    /// for command validation. <see cref="EventsourcedActor"/>s that want to persist new events within the <see cref="EventsourcedView.OnEvent(object)"/>
    /// handler should additionally mixin the <see cref="PersistOnEvent"/> trait and use the
    /// [[PersistOnEvent.persistOnEvent persistOnEvent]] method.
    /// </summary>
    /// <seealso cref="EventsourcedView"/>
    /// <seealso cref="PersistOnEvent"/>
    public abstract class EventsourcedActor : EventsourcedView, IEventsourcedVersion
    {
        #region EventSourcedVersion

        private VectorTime currentVersion = VectorTime.Zero;
        public override VectorTime CurrentVersion => currentVersion;

        /// <summary>
        /// Updates the current version from the given <paramref name="event"/>.
        /// </summary>
        private void UpdateVersion(DurableEvent @event) => currentVersion = currentVersion.Merge(@event.VectorTimestamp);

        internal DurableEvent DurableEvent(object payload, ImmutableHashSet<string> customDestinationAggregateIds, string deliveryId = null, long? persistOnEventSequenceNr = null, EventId? persistOnEventId = null) =>
            new DurableEvent(
                payload: payload,
                emitterId: Id,
                emitterAggregateId: AggregateId,
                customDestinationAggregateIds: customDestinationAggregateIds,
                vectorTimestamp: CurrentVersion,
                deliveryId: deliveryId,
                persistOnEventSequenceNr: persistOnEventSequenceNr,
                persistOnEventId: persistOnEventId);

        internal override void SnapshotLoaded(Snapshot snapshot)
        {
            base.SnapshotLoaded(snapshot);
            currentVersion = snapshot.CurrentTime;
        }

        internal override void ReceiveEventInternal(DurableEvent e)
        {
            base.ReceiveEventInternal(e);
            UpdateVersion(e);
        }

        #endregion

        private readonly EventsourcedActorSettings settings;
        private readonly IStash messageStash = Context.CreateStash<EventsourcedActor>();
        private readonly IStash commandStash = Context.CreateStash<EventsourcedActor>();

        private readonly List<DurableEvent> writeRequests = new List<DurableEvent>();
        private readonly LinkedList<Action<Try<object>>> writeHandlers = new LinkedList<Action<Try<object>>>();

        private int writeRequestCorrelationId = 0;
        private ImmutableHashSet<int> writesInProgress = ImmutableHashSet<int>.Empty;

        private bool writing = false;
        private bool writeReplyHandling = false;

        protected EventsourcedActor()
        {
            this.settings = new EventsourcedActorSettings(Context.System.Settings.Config);

        }

        /// <summary>
        /// State synchronization. If set to `true`, commands see internal state that is consistent
        /// with the event log. This is achieved by stashing new commands if this actor is currently
        /// writing events. If set to `false`, commands see internal state that is eventually
        /// consistent with the event log.
        /// </summary>
        public virtual bool StateSync => true;

        protected bool WritePending => writeRequests.Count != 0;

        /// <summary>
        /// Asynchronously persists a sequence of <paramref name="events"/> and calls <paramref name="handler"/> with the persist result
        /// for each event in the sequence. If persistence was successful, <see cref="EventsourcedView.OnEvent"/> is called with a
        /// persisted event before <paramref name="handler"/> is called. Both, <see cref="EventsourcedView.OnEvent"/> and <paramref name="handler"/>, are called on a
        /// dispatcher thread of this actor, hence, it is safe to modify internal state within them.
        /// The <paramref name="handler"/> can also obtain a reference to the initial command sender via <see cref="ActorBase.Sender"/>. The
        /// <paramref name="onLast"/> handler is additionally called for the last event in the sequence.
        /// 
        /// By default, the event is routed to event-sourced destinations with an undefined <see cref="EventsourcedView.AggregateId"/>.
        /// If this actor's <see cref="EventsourcedView.AggregateId"/> is defined it is additionally routed to all actors with the same
        /// <see cref="EventsourcedView.AggregateId"/>. Further routing destinations can be defined with the <paramref name="customDestinationAggregateIds"/>
        /// parameter.
        /// </summary>
        public void PersistMany<T>(IEnumerable<T> events, Action<Try<T>> handler, Action<Try<T>> onLast = null, ImmutableHashSet<string> customDestinationAggregateIds = null)
        {
            using (var enumerator = events.GetEnumerator())
            {
                if (enumerator.MoveNext())
                {
                loop:
                    var e = enumerator.Current;
                    if (enumerator.MoveNext())
                    {
                        Persist(e, handler, customDestinationAggregateIds);
                        goto loop;
                    }
                    else
                    {
                        // last element must be handled differently
                        Persist(e, attempt =>
                        {
                            handler(attempt);
                            onLast(attempt);
                        }, customDestinationAggregateIds);
                    }
                }
            }
        }

        /// <summary>
        /// Asynchronously persists the given <paramref name="domainEvent"/> and calls <paramref name="handler"/> with the persist result. If
        /// persistence was successful, <see cref="EventsourcedView.OnEvent"/> is called with the persisted event before <paramref name="handler"/>
        /// is called. Both, <see cref="EventsourcedView.OnEvent"/> and <paramref name="handler"/>, are called on a dispatcher thread of this actor,
        /// hence, it is safe to modify internal state within them. The <paramref name="handler"/> can also obtain a
        /// reference to the initial command sender via <see cref="ActorBase.Sender"/>.
        /// 
        /// By default, the event is routed to event-sourced destinations with an undefined <see cref="EventsourcedView.AggregateId"/>.
        /// If this actor's <see cref="EventsourcedView.AggregateId"/> is defined it is additionally routed to all actors with the same
        /// <see cref="EventsourcedView.AggregateId"/>. Further routing destinations can be defined with the <paramref name="customDestinationAggregateIds"/>
        /// parameter.
        /// </summary>
        public void Persist<T>(T domainEvent, Action<Try<T>> handler, ImmutableHashSet<string> customDestinationAggregateIds = null)
        {
            PersistDurableEvent(DurableEvent(domainEvent, customDestinationAggregateIds), attempt => handler(attempt.Cast<T>()));
        }

        internal void PersistDurableEvent(DurableEvent durableEvent, Action<Try<object>> handler)
        {
            writeRequests.Add(durableEvent);
            writeHandlers.AddLast(handler);
        }

        internal override void UnhandledMessage(object message)
        {
            switch (message)
            {
                case WriteSuccess ws:
                    if (writesInProgress.Contains(ws.CorrelationId) && ws.InstanceId == InstanceId)
                    {
                        writeReplyHandling = true;
                        try
                        {
                            foreach (var e in ws.Events)
                            {
                                ReceiveEventInternal(e);
                                writeHandlers.First.Value(Try.Success(e.Payload));
                                writeHandlers.RemoveFirst();
                            }

                            if (StateSync)
                            {
                                writing = false;
                                messageStash.Unstash();
                            }
                        }
                        finally
                        {
                            writeReplyHandling = false;
                            writesInProgress = writesInProgress.Remove(ws.CorrelationId);
                        }
                    }
                    break;

                case WriteFailure wf:
                    if (writesInProgress.Contains(wf.CorrelationId) && wf.InstanceId == InstanceId)
                    {
                        writeReplyHandling = true;
                        try
                        {
                            foreach (var e in wf.Events)
                            {
                                ReceiveEventInternal(e);
                                writeHandlers.First.Value(Try.Failure<object>(wf.Cause));
                                writeHandlers.RemoveFirst();
                            }

                            if (StateSync)
                            {
                                writing = false;
                                messageStash.Unstash();
                            }
                        }
                        finally
                        {
                            writeReplyHandling = false;
                            writesInProgress = writesInProgress.Remove(wf.CorrelationId);
                        }
                    }
                    break;

                case PersistOnEventRequest poer:
                    if (poer.InstanceId == InstanceId)
                    {
                        WriteOrDelay(() =>
                        {
                            writeHandlers.Clear();
                            writeRequests.Clear();
                            foreach (var invocation in poer.Invocations)
                            {
                                writeHandlers.AddLast(PersistOnEventActor.DefaultHandler);
                                writeRequests.Add(DurableEvent(invocation.Event, invocation.CustomDestinationAggregateIds, null, poer.PersistOnEventSequenceNr, poer.PersistOnEventId));
                            }
                        });
                    }
                    break;

                default:
                    WriteOrDelay(() => base.UnhandledMessage(message));
                    break;
            }
        }

        private void WriteOrDelay(Action writeRequestProducer)
        {
            if (writing)
                messageStash.Stash();
            else
            {
                writeRequestProducer();

                var wPending = WritePending;
                if (wPending) Write(NextCorrelationId());
                if (wPending && StateSync)
                    writing = true;
                else if (StateSync)
                    messageStash.Unstash();
            }
        }

        private void Write(int correlationId)
        {
            EventLog.Tell(new Write(writeRequests.ToArray(), Sender, Self, correlationId, InstanceId));
            writesInProgress = writesInProgress.Add(correlationId);
            writeRequests.Clear();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int NextCorrelationId() => (++writeRequestCorrelationId);

        /// <summary>
        /// Adds the current command to the user's command stash. Must not be used in the event handler
        /// or <see cref="Persist{T}(T, Action{Try{T}}, ImmutableHashSet{string})"/> handler.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        public sealed override void StashCommand()
        {
            if (writeReplyHandling || IsEventHandling)
                throw new StashException($"{nameof(Stash)} must not be used in event handler or persist handler");
            else
                commandStash.Stash();
        }

        /// <summary>
        /// Prepends all stashed commands to the actor's mailbox and then clears the command stash.
        /// Has no effect if the actor is recovering i.e. if <see cref="IsRecovering"/> returns `true`.
        /// </summary>
        public sealed override void UnstashAll()
        {
            if (!IsRecovering)
            {
                messageStash.Prepend(commandStash.ClearStash());
                messageStash.UnstashAll();
            }
        }
    }
}