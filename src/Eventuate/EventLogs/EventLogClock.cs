using System;
using System.Collections.Generic;
using System.Text;

namespace Eventuate.EventLogs
{
    /// <summary>
    /// A clock that tracks the current sequence number and the version vector of an event log.
    /// The version vector is the merge result of vector timestamps of all events that have been
    /// written to that event log.
    /// </summary>
    public sealed class EventLogClock : IEquatable<EventLogClock>
    {
        public EventLogClock(long sequenceNr = 0, VectorTime versionVector = null)
        {
            SequenceNr = sequenceNr;
            VersionVector = versionVector ?? VectorTime.Zero;
        }

        public long SequenceNr { get; }
        public VectorTime VersionVector { get; }

        /// <summary>
        /// Advances <see cref="SequenceNr"/> by given <paramref name="delta"/>.
        /// </summary>
        public EventLogClock AdvanceSequenceNr(long delta = 1) => new EventLogClock(SequenceNr + delta, VersionVector);

        /// <summary>
        /// Sets <see cref="SequenceNr"/> to the event's local sequence number and merges <see cref="VersionVector"/> with
        /// the event's vector timestamp.
        /// </summary>
        public EventLogClock Update(DurableEvent e) => new EventLogClock(e.LocalSequenceNr, VersionVector.Merge(e.VectorTimestamp));

        /// <summary>
        /// Sets <see cref="SequenceNr"/> to max of <see cref="SequenceNr"/> and <paramref name="processId"/>s entry in <see cref="VersionVector"/>.
        /// </summary>
        /// <param name="processId"></param>
        /// <returns></returns>
        public EventLogClock AdjustSequenceNrToProcessTime(string processId) => new EventLogClock(Math.Max(SequenceNr, VersionVector[processId]), VersionVector);

        public bool Equals(EventLogClock other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return SequenceNr == other.SequenceNr && Equals(VersionVector, other.VersionVector);
        }

        public override bool Equals(object obj) => ReferenceEquals(this, obj) || obj is EventLogClock other && Equals(other);

        public override int GetHashCode()
        {
            unchecked
            {
                return (SequenceNr.GetHashCode() * 397) ^ (VersionVector != null ? VersionVector.GetHashCode() : 0);
            }
        }
    }
}
