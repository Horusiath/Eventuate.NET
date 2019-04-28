using System;
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
        public PersistOnEventRequest(long persistOnEventSequenceNr, EventId persistOnEventId, ImmutableArray<PersistOnEventInvocation> invocations, int instanceId)
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
        public EventId PersistOnEventId { get; }
        public ImmutableArray<PersistOnEventInvocation> Invocations { get; }
        public int InstanceId { get; }
    }

    /// <summary>
    /// Thrown to indicate that an asynchronous <see cref="PersistOnEvent"/> operation failed.
    /// </summary>
    public class PersistOnEventException : Exception
    {
        public PersistOnEventException(string message, System.Exception inner) : base(message, inner) { }
        protected PersistOnEventException(
          System.Runtime.Serialization.SerializationInfo info,
          System.Runtime.Serialization.StreamingContext context) : base(info, context) { }
    }

    /// <summary>
    /// Can be mixed into [[EventsourcedActor]] for writing new events within the `onEvent` handler. New events are
    /// written with the asynchronous [[persistOnEvent]] method. In contrast to [[EventsourcedActor.persist persist]],
    /// one can '''not''' prevent command processing from running concurrently to [[persistOnEvent]] by setting
    /// [[EventsourcedActor.stateSync stateSync]] to `true`.
    /// 
    /// A `persistOnEvent` operation is reliable and idempotent. Once the event has been successfully written, a repeated
    /// `persistOnEvent` call for that event during event replay has no effect. A failed `persistOnEvent` operation will
    /// restart the actor by throwing a [[PersistOnEventException]]. After restart, failed `persistOnEvent` operations
    /// are automatically re-tried.
    /// </summary>
    public abstract class PersistOnEvent : EventsourcedActor
    {

    }
}