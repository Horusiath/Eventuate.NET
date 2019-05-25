#region copyright
// -----------------------------------------------------------------------
//  <copyright file="ConfirmedDelivery.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using Akka.Actor;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace Eventuate
{
    public sealed class DeliveryAttempt : IEquatable<DeliveryAttempt>
    {
        public DeliveryAttempt(string deliveryId, object message, ActorPath destination) {
            DeliveryId = deliveryId;
            Message = message;
            Destination = destination;
        }

        public string DeliveryId { get; }
        public object Message { get; }
        public ActorPath Destination { get; }

        public bool Equals(DeliveryAttempt other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return string.Equals(DeliveryId, other.DeliveryId) && Equals(Message, other.Message) && Equals(Destination, other.Destination);
        }

        public override bool Equals(object obj)
        {
            return ReferenceEquals(this, obj) || obj is DeliveryAttempt other && Equals(other);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (DeliveryId != null ? DeliveryId.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Message != null ? Message.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Destination != null ? Destination.GetHashCode() : 0);
                return hashCode;
            }
        }
    }

    /// <summary>
    /// Supports the reliable delivery of messages to destinations by enabling applications to
    /// redeliver messages until they are confirmed by their destinations. The correlation
    /// identifier between a reliable message and its confirmation message is an
    /// application-defined `deliveryId`. Reliable messages are delivered by calling `deliver` in
    /// an <see cref="EventsourcedActor"/>'s event handler. When the destination replies with a confirmation
    /// message, the event-sourced actor must persist an application-defined confirmation event
    /// together with the `deliveryId` using the [[persistConfirmation]] method. Until successful
    /// persistence of the confirmation event, delivered messages are tracked as ''unconfirmed''
    /// messages. Unconfirmed messages can be redelivered by calling `redeliverUnconfirmed`. This
    /// is usually done within a command handler by processing scheduler messages. Redelivery
    /// occurs automatically when the event-sourced actor successfully recovered after initial
    /// start or a re-start.
    /// </summary>
    public abstract class ConfirmedDelivery : EventsourcedActor
    {
        private ImmutableSortedDictionary<string, DeliveryAttempt> unconfirmed = ImmutableSortedDictionary<string, DeliveryAttempt>.Empty;

        /// <summary>
        /// Same semantics as <see cref="EventsourcedActor.Persist{T}(T, Action{Try{T}}, ImmutableHashSet{string})"/> 
        /// plus additional storage of a <paramref name="deliveryId"/> together with the persistent <paramref name="domainEvent"/>.
        /// </summary>
        public void PersistConfirmation<T>(T domainEvent, string deliveryId, Action<Try<T>> handler, ImmutableHashSet<string> customDestinationAggregateIds = null)
        {
            var e = this.DurableEvent(domainEvent, customDestinationAggregateIds ?? ImmutableHashSet<string>.Empty, deliveryId);
            this.PersistDurableEvent(e, attempt => handler(attempt.Cast<T>()));
        }

        /// <summary>
        /// Delivers the given <paramref name="message"/> to a <paramref name="destination"/>. The delivery of <paramref name="message"/> is identified by
        /// the given <paramref name="deliveryId"/> which must be unique in context of the sending actor. The message is
        /// tracked as unconfirmed message until delivery is confirmed by persisting a confirmation event
        /// with `persistConfirmation`, using the same `deliveryId`.
        /// </summary>
        public void Deliver(string deliveryId, object message, ActorPath destination)
        {
            unconfirmed = unconfirmed.SetItem(deliveryId, new DeliveryAttempt(deliveryId, message, destination));
            if (!IsRecovering) Send(message, destination);
        }

        /// <summary>
        /// Redelivers all unconfirmed messages.
        /// </summary>
        public void RedeliverUnconfirmed()
        {
            foreach (var (_, delivery) in unconfirmed)
                Send(delivery.Message, delivery.Destination);
        }

        /// <summary>
        /// Delivery ids of unconfirmed messages.
        /// </summary>
        public IEnumerable<string> Unconfirmed => unconfirmed.Keys;

        private void Send(object message, ActorPath destination) => Context.ActorSelection(destination).Tell(message);

        private void Confirm(string deliveryId) => unconfirmed = unconfirmed.Remove(deliveryId);

        internal override void ReceiveEvent(DurableEvent e)
        {
            base.ReceiveEvent(e);
            if (!(e.DeliveryId is null) && e.EmitterId == this.Id)
                Confirm(e.DeliveryId);
        }

        internal override Snapshot SnapshotCaptured(Snapshot snapshot)
        {
            var s = base.SnapshotCaptured(snapshot);
            foreach (var entry in unconfirmed)
            {
                s = s.AddDeliveryAttempt(entry.Value);
            }
            return s;
        }

        internal override bool SnapshotLoaded(Snapshot snapshot, Receive behavior)
        {
            var previouslyUnconfirmed = this.unconfirmed;
            var builder = this.unconfirmed.ToBuilder();
            foreach (var delivery in snapshot.DeliveryAttempts)
            {
                builder[delivery.DeliveryId] = delivery;
            }
            this.unconfirmed = builder.ToImmutable();
            var handled = base.SnapshotLoaded(snapshot, behavior);
            if (!handled)
                this.unconfirmed = previouslyUnconfirmed;
            return handled;
        }

        internal override void Recovered()
        {
            base.Recovered();
            RedeliverUnconfirmed();
        }
    }
}