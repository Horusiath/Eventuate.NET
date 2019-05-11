#region copyright
// -----------------------------------------------------------------------
//  <copyright file="EventId.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;

namespace Eventuate
{
    /// <summary>
    /// Unique id of a <see cref="DurableEvent"/>.
    /// 
    /// This is a stable id of an event across all replicated logs.
    /// </summary>
    public readonly struct EventId : IEquatable<EventId>
    {
        public EventId(string processId, long sequenceNr)
        {
            ProcessId = processId;
            SequenceNr = sequenceNr;
        }

        /// <summary>
        /// The id of the event log the initially wrote the event.
        /// </summary>
        public string ProcessId { get; }

        /// <summary>
        /// The initial sequence number in this log.
        /// </summary>
        public long SequenceNr { get; }

        public bool Equals(EventId other) => ProcessId == other.ProcessId && SequenceNr == other.SequenceNr;

        public override bool Equals(object other) => other is EventId id && Equals(id);

        public override int GetHashCode()
        {
            var hashCode = 17;
            hashCode = hashCode * 23 + (ProcessId.GetHashCode());
            hashCode = hashCode * 23 + SequenceNr.GetHashCode();
            return hashCode;
        }
    }
}
