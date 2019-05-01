using Akka.Actor;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace Eventuate
{
    /// <summary>
    /// Records a <see cref="PersistOnEvent"/> invocation.
    /// </summary>
    public readonly struct PersistOnEventInvocation
    {
        public PersistOnEventInvocation(object @event, ImmutableHashSet<string> customDestinationAggregateIds)
        {
            Event = @event;
            CustomDestinationAggregateIds = customDestinationAggregateIds;
        }

        public object Event { get; }
        public ImmutableHashSet<string> CustomDestinationAggregateIds { get; }
    }

    /// <summary>
    /// A request sent by <see cref="PersistOnEvent"/> instances to <see cref="IActorContext.Self"/> in order to persist events recorded by <see cref="Invocations"/>.
    /// </summary>
    public sealed class PersistOnEventRequest
    {
        public PersistOnEventRequest(long persistOnEventSequenceNr, EventId? persistOnEventId, ImmutableArray<PersistOnEventInvocation> invocations, int instanceId)
        {
            PersistOnEventSequenceNr = persistOnEventSequenceNr;
            PersistOnEventId = persistOnEventId;
            Invocations = invocations;
            InstanceId = instanceId;
        }

        /// <summary>
        /// The sequence number of the event that caused this request.
        /// </summary>
        public long PersistOnEventSequenceNr { get; }

        /// <summary>
        /// <see cref="EventId"/> of the event that caused this request. This is optional for backwards
        /// compatibility, as old snapshots might contain <see cref="PersistOnEventRequest"/>s
        /// without this field being defined.
        /// </summary>
        public EventId? PersistOnEventId { get; }
        public ImmutableArray<PersistOnEventInvocation> Invocations { get; }
        public int InstanceId { get; }
    }

    /// <summary>
    /// Thrown to indicate that an asynchronous <see cref="PersistOnEventActor.PersistOnEvent"/> operation failed.
    /// </summary>
    public class PersistOnEventException : Exception
    {
        public PersistOnEventException(string message, System.Exception inner) : base(message, inner) { }
        protected PersistOnEventException(
          System.Runtime.Serialization.SerializationInfo info,
          System.Runtime.Serialization.StreamingContext context) : base(info, context) { }
    }

    /// <summary>
    /// Can be mixed into <see cref="EventsourcedActor"/> for writing new events within the <see cref="EventsourcedView.OnEvent(object)"/> handler. New events are
    /// written with the asynchronous <see cref="PersistOnEvent{T}"/> method. In contrast to <see cref="EventsourcedActor.Persist{T}(T, Action{Try{T}}, ImmutableHashSet{string})"/>,
    /// one can '''not''' prevent command processing from running concurrently to <see cref="PersistOnEvent"/> by setting
    /// <see cref="EventsourcedActor.StateSync"/> to `true`.
    /// 
    /// A <see cref="PersistOnEvent{T}"/> operation is reliable and idempotent. Once the event has been successfully written, a repeated
    /// <see cref="PersistOnEvent{T}"/> call for that event during event replay has no effect. A failed <see cref="PersistOnEvent{T}"/> operation will
    /// restart the actor by throwing a <see cref="PersistOnEventException"/>. After restart, failed <see cref="PersistOnEvent{T}"/> operations
    /// are automatically re-tried.
    /// </summary>
    public abstract class PersistOnEventActor : EventsourcedActor
    {
        /// <summary>
        /// Default <see cref="EventsourcedActor.Persist"/> handler to use when processing <see cref="PersistOnEventRequest"/>s in <see cref="EventsourcedActor"/>.
        /// </summary>
        public static Action<Try<object>> DefaultHandler { get; } = attempt =>
        {
            if (attempt.IsFailure)
                throw new PersistOnEventException($"Failed to persist [{nameof(PersistOnEventRequest)}]", attempt.Exception);
        };

        private readonly List<PersistOnEventInvocation> invocations = new List<PersistOnEventInvocation>();

        /// <summary>
        /// <see cref="PersistOnEventRequest"/> by sequence number of the event that caused the persist on event request.
        /// 
        /// This map keeps the requests in the order they were submitted.
        /// </summary>
        private readonly SortedDictionary<long, PersistOnEventRequest> requestsBySequenceNr = new SortedDictionary<long, PersistOnEventRequest>();

        /// <summary>
        /// <see cref="PersistOnEventRequest"/> by <see cref="EventId"/> of the event that caused the persist on event request.
        /// 
        /// This map ensures that requests can be confirmed properly even if the sequence number of the event
        /// that caused the request changed its local sequence number due to a disaster recovery.
        /// </summary>
        /// <seealso cref="https://github.com/RBMHTechnology/eventuate/issues/385"/>
        private readonly Dictionary<EventId, PersistOnEventRequest> requestsByEventId = new Dictionary<EventId, PersistOnEventRequest>();

        internal IEnumerable<long> UnconfirmedRequests => requestsBySequenceNr.Keys;


        /// <summary>
        /// Asynchronously persists the given <paramref name="domainEvent"/>. Applications that want to handle the persisted event should define
        /// the event handler at that event. By default, the event is routed to event-sourced destinations with an undefined
        /// <see cref="EventsourcedView.AggregateId"/>. If this actor's <see cref="EventsourcedView.AggregateId"/> is defined it is additionally routed to all actors with the same
        /// <see cref="EventsourcedView.AggregateId"/>. Further routing destinations can be defined with the <paramref name="customDestinationAggregateIds"/> parameter.
        /// </summary>
        public void PersistOnEvent<T>(T domainEvent, ImmutableHashSet<string> customDestinationAggregateIds)
        {
            invocations.Add(new PersistOnEventInvocation(domainEvent, customDestinationAggregateIds));
        }

        internal override void ReceiveEvent(DurableEvent e)
        {
            base.ReceiveEvent(e);
            if (e.EmitterId == Id && TryFindPersistOnEventRequest(e, out var request))
            {
                ConfirmRequest(request);
            }             
            
            if (invocations.Count != 0)
            {
                DeliverRequest(new PersistOnEventRequest(LastSequenceNr, LastHandledEvent.Id, invocations.ToImmutableArray(), InstanceId));
                invocations.Clear();
            }
        }

        internal override Snapshot SnapshotCaptured(Snapshot snapshot)
        {
            var acc = base.SnapshotCaptured(snapshot);
            foreach (var (_, request) in requestsBySequenceNr)
            {
                acc = acc.AddPersistOnEventRequest(request);
            }
            return acc;
        }

        internal override void SnapshotLoaded(Snapshot snapshot)
        {
            base.SnapshotLoaded(snapshot);
            foreach (var pr in snapshot.PersistOnEventRequests)
            {
                var requestWithUpdatedInstanceId = new PersistOnEventRequest(pr.PersistOnEventSequenceNr, pr.PersistOnEventId, pr.Invocations, InstanceId);
                requestsBySequenceNr[pr.PersistOnEventSequenceNr] = requestWithUpdatedInstanceId;
                if (pr.PersistOnEventId.HasValue)
                    requestsByEventId[pr.PersistOnEventId.Value] = requestWithUpdatedInstanceId;
            }
        }

        internal override void Recovered()
        {
            base.Recovered();
            RedeliverUnconfirmedRequests();
        }

        private void DeliverRequest(PersistOnEventRequest request)
        {
            this.requestsBySequenceNr[request.PersistOnEventSequenceNr] = request;
            if (request.PersistOnEventId.HasValue)
                this.requestsByEventId[request.PersistOnEventId.Value] = request;

            if (!IsRecovering)
                Self.Tell(request);
        }

        private void ConfirmRequest(PersistOnEventRequest request)
        {
            if (request.PersistOnEventId.HasValue)
                this.requestsByEventId.Remove(request.PersistOnEventId.Value);

            this.requestsBySequenceNr.Remove(request.PersistOnEventSequenceNr);
        }

        private bool TryFindPersistOnEventRequest(DurableEvent e, out PersistOnEventRequest request)
        {
            if (e.PersistOnEventId.HasValue && this.requestsByEventId.TryGetValue(e.PersistOnEventId.Value, out request))
                return true;

            if (e.PersistOnEventSequenceNr.HasValue && this.requestsBySequenceNr.TryGetValue(e.PersistOnEventSequenceNr.Value, out request))
                return true;

            request = default;
            return false;
        }

        private void RedeliverUnconfirmedRequests()
        {
            foreach (var (_, request) in this.requestsBySequenceNr)
            {
                Self.Tell(request);
            }
        }
    }
}