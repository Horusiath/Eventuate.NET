#region copyright
// -----------------------------------------------------------------------
//  <copyright file="IEventSourcedVersion.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System.Collections.Immutable;

namespace Eventuate
{
    /// <summary>
    /// Maintains the current version of an event-sourced component. The current version is updated by merging
    /// <see cref="DurableEvent.VectorTimestamp"/>'s of handled events.
    /// </summary>
    public interface IEventsourcedVersion
    {
        VectorTime CurrentVersion { get; set; }
    }

    public static class EventsourcedVersion
    {
        /// <summary>
        /// Updates the current version from the given <paramref name="event"/>.
        /// </summary>
        public static void UpdateVersion<T>(this T actor, DurableEvent @event) where T : EventsourcedView, IEventsourcedVersion
        {
            actor.CurrentVersion = actor.CurrentVersion.Merge(@event.VectorTimestamp);
        }

        public static DurableEvent DurableEvent<T>(this T actor, object payload, ImmutableHashSet<string> customDestinationAggregateIds, string deliveryId = null, long? persistOnEventSequenceNr = null, EventId? persistOnEventId = null)
            where T : EventsourcedView, IEventsourcedVersion
        {
            return new DurableEvent(
                payload: payload,
                emitterId: actor.Id,
                emitterAggregateId: actor.AggregateId,
                customDestinationAggregateIds: customDestinationAggregateIds,
                vectorTimestamp: actor.CurrentVersion,
                deliveryId: deliveryId,
                persistOnEventSequenceNr: persistOnEventSequenceNr,
                persistOnEventId: persistOnEventId);
        }
    }
}