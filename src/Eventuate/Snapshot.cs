using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Text;

namespace Eventuate
{
    /// <summary>
    /// Snapshot metadata.
    /// </summary>
    public readonly struct SnapshotMetadata : System.IEquatable<SnapshotMetadata>
    {
        public SnapshotMetadata(string emitterId, long sequenceNr)
        {
            EmitterId = emitterId;
            SequenceNr = sequenceNr;
        }

        /// <summary>
        /// Id of the <see cref="EventsourcedActor"/>, <see cref="EventsourcedView"/>, 
        /// stateful <see cref="EventsourcedWriter"/> or <see cref="EventsourcedProcessor"/> 
        /// that saves the snapshot.
        /// </summary>
        public string EmitterId { get; }

        /// <summary>
        /// The highest event sequence number covered by the snapshot.
        /// </summary>
        public long SequenceNr { get; }

        public bool Equals(SnapshotMetadata other) => Equals(EmitterId, other.EmitterId) && Equals(SequenceNr, other.SequenceNr);

        public override bool Equals(object other) => other is SnapshotMetadata ? Equals((SnapshotMetadata)other) : false;

        public override int GetHashCode()
        {
            var hashCode = 17;
            hashCode = hashCode * 23 + (EmitterId?.GetHashCode() ?? 0);
            hashCode = hashCode * 23 + SequenceNr.GetHashCode();
            return hashCode;
        }
    }

    /// <summary>
    /// Provider API.
    /// 
    /// Snapshot storage format. <see cref="EventsourcedActor"/>s, <see cref="EventsourcedView"/>s, stateful <see cref="EventsourcedWriter"/>s
    /// and <see cref="EventsourcedProcessor"/>s can save snapshots of internal state by calling the (inherited)
    /// <see cref="EventsourcedView.Save"/> method.
    /// </summary>
    public sealed class Snapshot
    {
        public Snapshot(
            object payload, 
            string emitterId, 
            DurableEvent lastEvent, 
            VectorTime currentTime, 
            long sequenceNr, 
            ImmutableHashSet<DeliveryAttempt> deliveryAttempts = null, 
            ImmutableHashSet<PersistOnEventRequest> persistOnEventRequests = null)
        {
            Payload = payload;
            EmitterId = emitterId;
            LastEvent = lastEvent;
            CurrentTime = currentTime;
            SequenceNr = sequenceNr;
            DeliveryAttempts = deliveryAttempts ?? ImmutableHashSet<DeliveryAttempt>.Empty;
            PersistOnEventRequests = persistOnEventRequests ?? ImmutableHashSet<PersistOnEventRequest>.Empty;
        }

        /// <summary>
        /// Application-specific snapshot.
        /// </summary>
        public object Payload { get; }

        /// <summary>
        /// Id of the event-sourced actor, view, stateful writer or processor that saved the snapshot.
        /// </summary>
        public string EmitterId { get; }

        /// <summary>
        /// Last handled event before the snapshot was saved.
        /// </summary>
        public DurableEvent LastEvent { get; }

        /// <summary>
        /// Current vector time when the snapshot was saved.
        /// </summary>
        public VectorTime CurrentTime { get; }

        /// <summary>
        /// Sequence number of the last *received* event when the snapshot was saved.
        /// </summary>
        public long SequenceNr { get; }

        /// <summary>
        /// Unconfirmed [[ConfirmedDelivery.DeliveryAttempt DeliveryAttempt]]s when the snapshot was
        /// saved (can only be non-empty if the actor implements [[ConfirmedDelivery]]).
        /// </summary>
        public ImmutableHashSet<DeliveryAttempt> DeliveryAttempts { get; }

        /// <summary>
        /// Unconfirmed [[PersistOnEvent.PersistOnEventRequest PersistOnEventRequest]]s when the
        /// snapshot was saved (can only be non-empty if the actor implements [[PersistOnEvent]]).
        /// </summary>
        public ImmutableHashSet<PersistOnEventRequest> PersistOnEventRequests { get; }

        public SnapshotMetadata Metadata => new SnapshotMetadata(EmitterId, SequenceNr);

        public Snapshot AddDeliveryAttempt(DeliveryAttempt deliveryAttempt) =>
            new Snapshot(Payload, EmitterId, LastEvent, CurrentTime, SequenceNr, DeliveryAttempts.Add(deliveryAttempt), PersistOnEventRequests);

        public Snapshot AddPersistOnEventRequest(PersistOnEventRequest persistOnEventRequest) =>
            new Snapshot(Payload, EmitterId, LastEvent, CurrentTime, SequenceNr, DeliveryAttempts, PersistOnEventRequests.Add(persistOnEventRequest));
    }
}
