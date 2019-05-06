using Akka.Actor;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Text;

namespace Eventuate.EventsourcingProtocol
{
    /// <summary>
    /// Instructs an event log to write the given <see cref="Events"/>.
    /// </summary>
    public sealed class Write : IUpdateableEventBatch<Write>
    {
        public Write(IReadOnlyCollection<DurableEvent> events, IActorRef initiator, IActorRef replyTo, int correlationId, int instanceId)
        {
            Events = events;
            Initiator = initiator;
            ReplyTo = replyTo;
            CorrelationId = correlationId;
            InstanceId = instanceId;
        }

        public IReadOnlyCollection<DurableEvent> Events { get; }
        public IActorRef Initiator { get; }
        public IActorRef ReplyTo { get; }
        public int CorrelationId { get; }
        public int InstanceId { get; }

        public int Count => Events.Count;
        IEnumerable<DurableEvent> IDurableEventBatch.Events => Events;
        public Write Update(params DurableEvent[] events) => new Write(events, Initiator, ReplyTo, CorrelationId, InstanceId);
        public Write WithReplyToDefault(IActorRef replyTo) =>
            this.ReplyTo is null ? new Write(Events, Initiator, replyTo, CorrelationId, InstanceId) : this;
    }

    /// <summary>
    /// Instructs an event log to batch-execute the given <see cref="Writes"/>.
    /// </summary>
    public sealed class WriteMany : IEquatable<WriteMany>
    {
        public WriteMany(IEnumerable<Write> writes)
        {
            Writes = writes;
        }

        public IEnumerable<Write> Writes { get; }

        public bool Equals(WriteMany other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;

            using (var e1 = this.Writes.GetEnumerator())
            using (var e2 = other.Writes.GetEnumerator())
            {
                do
                {
                    if (e1.MoveNext())
                    {
                        if (e2.MoveNext())
                        {
                            if (!e1.Current.Equals(e2.Current)) return false;
                        }
                        else return false;
                    }
                    else if (e2.MoveNext()) return false;
                    else return true; 
                } while (true);
            }
        }

        public override bool Equals(object obj) => obj is WriteMany m && Equals(m);

        public override int GetHashCode()
        {
            unchecked
            {
                var hash = 0;
                foreach (var write in Writes)
                {
                    hash ^= (23 * write.GetHashCode());
                }

                return hash;
            }
        }
    }

    /// <summary>
    /// Completion reply after a <see cref="WriteMany"/>.
    /// </summary>
    public sealed class WriteManyComplete
    {
        public static readonly WriteManyComplete Instance = new WriteManyComplete();
        private WriteManyComplete() { }
    }

    /// <summary>
    /// Success reply after a <see cref="Write"/>.
    /// </summary>
    public sealed class WriteSuccess : IEquatable<WriteSuccess>
    {
        public WriteSuccess(IEnumerable<DurableEvent> events, int correlationId, int instanceId)
        {
            Events = events;
            CorrelationId = correlationId;
            InstanceId = instanceId;
        }

        public IEnumerable<DurableEvent> Events { get; }
        public int CorrelationId { get; }
        public int InstanceId { get; }

        public bool Equals(WriteSuccess other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;

            if (InstanceId != other.InstanceId) return false;
            if (CorrelationId != other.CorrelationId) return false;
            
            using (var e1 = this.Events.GetEnumerator())
            using (var e2 = other.Events.GetEnumerator())
            {
                do
                {
                    if (e1.MoveNext())
                    {
                        if (e2.MoveNext())
                        {
                            if (!e1.Current.Equals(e2.Current)) return false;
                        }
                        else return false;
                    }
                    else if (e2.MoveNext()) return false;
                    else return true; 
                } while (true);
            }
        }

        public override bool Equals(object obj) => obj is WriteSuccess ws && Equals(ws);

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = CorrelationId;
                hashCode = (hashCode * 397) ^ InstanceId;
                foreach (var durableEvent in Events)
                {
                    hashCode = (hashCode * 397) ^ durableEvent.GetHashCode();
                }
                return hashCode;
            }
        }
    }

    /// <summary>
    /// Failure reply after a <see cref="Write"/>.
    /// </summary>
    public sealed class WriteFailure : IFailure<Exception>, IEquatable<WriteFailure>
    {
        public WriteFailure(IEnumerable<DurableEvent> events, Exception cause, int correlationId, int instanceId)
        {
            Events = events;
            Cause = cause;
            CorrelationId = correlationId;
            InstanceId = instanceId;
        }

        public IEnumerable<DurableEvent> Events { get; }
        public Exception Cause { get; }
        public int CorrelationId { get; }
        public int InstanceId { get; }

        public bool Equals(WriteFailure other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            if (InstanceId != other.InstanceId) return false;
            if (CorrelationId != other.CorrelationId) return false;
            if (Cause != other.Cause) return false;
            
            using (var e1 = this.Events.GetEnumerator())
            using (var e2 = other.Events.GetEnumerator())
            {
                do
                {
                    if (e1.MoveNext())
                    {
                        if (e2.MoveNext())
                        {
                            if (!e1.Current.Equals(e2.Current)) return false;
                        }
                        else return false;
                    }
                    else if (e2.MoveNext()) return false;
                    else return true;
                } while (true);
            }
        }

        public override bool Equals(object obj) => obj is WriteFailure wf && Equals(wf);

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = Cause.GetHashCode();
                hashCode = (hashCode * 397) ^ CorrelationId;
                hashCode = (hashCode * 397) ^ InstanceId;
                foreach (var durableEvent in Events)
                {
                    hashCode = (hashCode * 397) ^ durableEvent.GetHashCode();
                }
                return hashCode;
            }
        }
    }

    /// <summary>
    /// Sent by an event log to all registered participants, if `event` has been successfully written.
    /// This message is not sent to a participant if that participant has sent a corresponding <see cref="Write"/>.
    /// </summary>
    public readonly struct Written : IEquatable<Written>
    {
        public Written(DurableEvent @event)
        {
            Event = @event;
        }

        public DurableEvent Event { get; }

        public bool Equals(Written other) => Equals(Event, other.Event);
        public override bool Equals(object obj) => obj is Written other && Equals(other);
        public override int GetHashCode() => Event.GetHashCode();
    }

    /// <summary>
    /// Instructs an event log to read up to <see cref="Max"/> events starting at <see cref="FromSequenceNr"/>. 
    /// If <see cref="AggregateId"/> is defined, only those events that have the aggregate id in their 
    /// `destinationAggregateIds` are returned.
    /// </summary>
    public sealed class Replay : IEquatable<Replay>
    {
        public Replay(IActorRef subscriber, int instanceId, long fromSequenceNr, int max = 4096, string aggregateId = null)
        {
            FromSequenceNr = fromSequenceNr;
            Max = max;
            Subscriber = subscriber;
            AggregateId = aggregateId;
            InstanceId = instanceId;
        }

        public long FromSequenceNr { get; }
        public IActorRef Subscriber { get; }
        public string AggregateId { get; }
        public int Max { get; }
        public int InstanceId { get; }

        public bool Equals(Replay other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return FromSequenceNr == other.FromSequenceNr 
                   && Equals(Subscriber, other.Subscriber) 
                   && string.Equals(AggregateId, other.AggregateId) 
                   && Max == other.Max 
                   && InstanceId == other.InstanceId;
        }

        public override bool Equals(object obj) => obj is Replay r && Equals(r);

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = FromSequenceNr.GetHashCode();
                hashCode = (hashCode * 397) ^ (Subscriber != null ? Subscriber.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (AggregateId != null ? AggregateId.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ Max;
                hashCode = (hashCode * 397) ^ InstanceId;
                return hashCode;
            }
        }
    }

    /// <summary>
    /// Success reply after a <see cref="Replay"/>.
    /// </summary>
    public sealed class ReplaySuccess : IEquatable<ReplaySuccess>
    {
        public ReplaySuccess(IEnumerable<DurableEvent> events, long replayProgress, int instanceId)
        {
            Events = events;
            ReplayProgress = replayProgress;
            InstanceId = instanceId;
        }

        public IEnumerable<DurableEvent> Events { get; }
        public long ReplayProgress { get; }
        public int InstanceId { get; }

        public bool Equals(ReplaySuccess other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            if (InstanceId != other.InstanceId) return false;
            if (ReplayProgress != other.ReplayProgress) return false;
            
            using (var e1 = this.Events.GetEnumerator())
            using (var e2 = other.Events.GetEnumerator())
            {
                do
                {
                    if (e1.MoveNext())
                    {
                        if (e2.MoveNext())
                        {
                            if (!e1.Current.Equals(e2.Current)) return false;
                        }
                        else return false;
                    }
                    else if (e2.MoveNext()) return false;
                    else return true;
                } while (true);
            }
        }

        public override bool Equals(object obj) => obj is ReplaySuccess rs && Equals(rs);

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = ReplayProgress.GetHashCode();
                hashCode = (hashCode * 397) ^ InstanceId;
                foreach (var durableEvent in Events)
                {
                    hashCode = (hashCode * 397) ^ durableEvent.GetHashCode();
                }
                return hashCode;
            }
        }
    }

    /// <summary>
    /// Failure reply after a <see cref="Replay"/>.
    /// </summary>
    public sealed class ReplayFailure : IFailure<Exception>, IEquatable<ReplayFailure>
    {
        public ReplayFailure(Exception cause, long replayProgress, int instanceId)
        {
            Cause = cause;
            ReplayProgress = replayProgress;
            InstanceId = instanceId;
        }

        public Exception Cause { get; }
        public long ReplayProgress { get; }
        public int InstanceId { get; }

        public bool Equals(ReplayFailure other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Equals(Cause, other.Cause) && ReplayProgress == other.ReplayProgress && InstanceId == other.InstanceId;
        }

        public override bool Equals(object obj) => obj is ReplayFailure rf && Equals(rf);

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (Cause != null ? Cause.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ ReplayProgress.GetHashCode();
                hashCode = (hashCode * 397) ^ InstanceId;
                return hashCode;
            }
        }
    }

    /// <summary>
    /// Internal message to trigger a new <see cref="Replay"/> attempt.
    /// </summary>
    internal readonly struct ReplayRetry : IEquatable<ReplayRetry>
    {
        public ReplayRetry(long replayProgress)
        {
            ReplayProgress = replayProgress;
        }

        public long ReplayProgress { get; }

        public bool Equals(ReplayRetry other) => ReplayProgress == other.ReplayProgress;
        public override bool Equals(object obj) => obj is ReplayRetry other && Equals(other);
        public override int GetHashCode() => ReplayProgress.GetHashCode();
    }

    /// <summary>
    /// Instructs an event log to delete events with a sequence nr less or equal a given one.
    /// Deleted events are not replayed any more, however depending on the log implementation
    /// and <see cref="RemoteLogIds"/> they might still be replicated.
    /// </summary>
    public readonly struct Delete : IEquatable<Delete>
    {
        public Delete(long toSequenceNr, ImmutableHashSet<string> remoteLogIds = null)
        {
            ToSequenceNr = toSequenceNr;
            RemoteLogIds = remoteLogIds ?? ImmutableHashSet<string>.Empty;
        }

        /// <summary>
        /// All events with a less or equal sequence nr are not replayed any more.
        /// </summary>
        public long ToSequenceNr { get; }

        /// <summary>
        /// A set of remote log ids that must have replicated events before they 
        /// are allowed to be physically deleted.
        /// </summary>
        public ImmutableHashSet<string> RemoteLogIds { get; }

        public bool Equals(Delete other) => 
            ToSequenceNr == other.ToSequenceNr && RemoteLogIds.SetEquals(other.RemoteLogIds);

        public override bool Equals(object obj) => obj is Delete other && Equals(other);

        public override int GetHashCode()
        {
            unchecked
            {
                var hash = ToSequenceNr.GetHashCode();
                foreach (var logId in RemoteLogIds)
                {
                    hash = (hash * 397) ^ logId.GetHashCode();
                }

                return hash;
            }
        }
    }

    /// <summary>
    /// Success reply after a <see cref="Delete"/>.
    /// </summary>
    /// @param deletedTo 
    /// 
    public readonly struct DeleteSuccess : IEquatable<DeleteSuccess>
    {
        public DeleteSuccess(long deletedTo, ImmutableHashSet<string> remoteLogIds = null)
        {
            DeletedTo = deletedTo;
            RemoteLogIds = remoteLogIds;
        }

        /// <summary>
        /// The actually written deleted to marker. Minimum of <see cref="Delete.ToSequenceNr"/> 
        /// and the current sequence nr.
        /// </summary>
        public long DeletedTo { get; }
        public ImmutableHashSet<string> RemoteLogIds { get; }

        public bool Equals(DeleteSuccess other) => 
            DeletedTo == other.DeletedTo && RemoteLogIds.SetEquals(other.RemoteLogIds);

        public override bool Equals(object obj) => obj is DeleteSuccess other && Equals(other);

        public override int GetHashCode()
        {
            unchecked
            {
                var hash = DeletedTo.GetHashCode();
                foreach (var logId in RemoteLogIds)
                {
                    hash = (hash * 397) ^ logId.GetHashCode();
                }

                return hash;
            }
        }
    }

    /// <summary>
    /// Failure reply after a <see cref="Delete"/>.
    /// </summary>
    public readonly struct DeleteFailure : IFailure<Exception>, IEquatable<DeleteFailure>
    {
        public DeleteFailure(Exception cause)
        {
            Cause = cause;
        }

        public Exception Cause { get; }

        public bool Equals(DeleteFailure other) => Equals(Cause, other.Cause);
        public override bool Equals(object obj) => obj is DeleteFailure other && Equals(other);
        public override int GetHashCode() => (Cause != null ? Cause.GetHashCode() : 0);
    }

    /// <summary>
    /// Instructs an event log to save the given <see cref="Snapshot"/>.
    /// </summary>
    public sealed class SaveSnapshot : IEquatable<SaveSnapshot>
    {
        public SaveSnapshot(Snapshot snapshot, IActorRef initiator, int instanceId)
        {
            Snapshot = snapshot;
            Initiator = initiator;
            InstanceId = instanceId;
        }

        public Snapshot Snapshot { get; }
        public IActorRef Initiator { get; }
        public int InstanceId { get; }

        public bool Equals(SaveSnapshot other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Equals(Snapshot, other.Snapshot) && Equals(Initiator, other.Initiator) && InstanceId == other.InstanceId;
        }

        public override bool Equals(object obj) => obj is SaveSnapshot ss && Equals(ss);

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (Snapshot != null ? Snapshot.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Initiator != null ? Initiator.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ InstanceId;
                return hashCode;
            }
        }
    }

    /// <summary>
    /// Success reply after a <see cref="SaveSnapshot"/>.
    /// </summary>
    public sealed class SaveSnapshotSuccess : IEquatable<SaveSnapshotSuccess>
    {
        public SaveSnapshotSuccess(SnapshotMetadata metadata, int instanceId)
        {
            Metadata = metadata;
            InstanceId = instanceId;
        }

        public SnapshotMetadata Metadata { get; }
        public int InstanceId { get; }

        public bool Equals(SaveSnapshotSuccess other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Metadata.Equals(other.Metadata) && InstanceId == other.InstanceId;
        }

        public override bool Equals(object obj) => obj is SaveSnapshotSuccess sss && Equals(sss);

        public override int GetHashCode()
        {
            unchecked
            {
                return (Metadata.GetHashCode() * 397) ^ InstanceId;
            }
        }
    }

    /// <summary>
    ///  Failure reply after a <see cref="SaveSnapshot"/>.
    /// </summary>
    public sealed class SaveSnapshotFailure : IFailure<Exception>, IEquatable<SaveSnapshotFailure>
    {
        public SaveSnapshotFailure(SnapshotMetadata metadata, Exception cause, int instanceId)
        {
            Metadata = metadata;
            Cause = cause;
            InstanceId = instanceId;
        }

        public SnapshotMetadata Metadata { get; }
        public Exception Cause { get; }
        public int InstanceId { get; }

        public bool Equals(SaveSnapshotFailure other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Metadata.Equals(other.Metadata) && Equals(Cause, other.Cause) && InstanceId == other.InstanceId;
        }

        public override bool Equals(object obj) => obj is SaveSnapshotFailure ssf && Equals(ssf);

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = Metadata.GetHashCode();
                hashCode = (hashCode * 397) ^ (Cause != null ? Cause.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ InstanceId;
                return hashCode;
            }
        }
    }

    /// <summary>
    /// Instructs an event log to load the most recent snapshot for <see cref="EmitterId"/>.
    /// </summary>
    public readonly struct LoadSnapshot : IEquatable<LoadSnapshot>
    {
        public LoadSnapshot(string emitterId, int instanceId)
        {
            EmitterId = emitterId;
            InstanceId = instanceId;
        }

        public string EmitterId { get; }
        public int InstanceId { get; }

        public bool Equals(LoadSnapshot other) => string.Equals(EmitterId, other.EmitterId) && InstanceId == other.InstanceId;
        public override bool Equals(object obj) => obj is LoadSnapshot other && Equals(other);

        public override int GetHashCode()
        {
            unchecked
            {
                return ((EmitterId != null ? EmitterId.GetHashCode() : 0) * 397) ^ InstanceId;
            }
        }
    }

    /// <summary>
    /// Success reply after a <see cref="LoadSnapshot"/>.
    /// </summary>
    public readonly struct LoadSnapshotSuccess : IEquatable<LoadSnapshotSuccess>
    {
        public LoadSnapshotSuccess(Snapshot snapshot, int instanceId)
        {
            Snapshot = snapshot;
            InstanceId = instanceId;
        }

        public Snapshot Snapshot { get; }
        public int InstanceId { get; }

        public bool Equals(LoadSnapshotSuccess other) => Equals(Snapshot, other.Snapshot) && InstanceId == other.InstanceId;
        public override bool Equals(object obj) => obj is LoadSnapshotSuccess other && Equals(other);

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Snapshot != null ? Snapshot.GetHashCode() : 0) * 397) ^ InstanceId;
            }
        }
    }

    /// <summary>
    /// Failure reply after a <see cref="LoadSnapshot"/>.
    /// </summary>
    public readonly struct LoadSnapshotFailure : IFailure<Exception>, IEquatable<LoadSnapshotFailure>
    {
        public LoadSnapshotFailure(Exception cause, int instanceId)
        {
            Cause = cause;
            InstanceId = instanceId;
        }

        public Exception Cause { get; }
        public int InstanceId { get; }

        public bool Equals(LoadSnapshotFailure other) => Equals(Cause, other.Cause) && InstanceId == other.InstanceId;
        public override bool Equals(object obj) => obj is LoadSnapshotFailure other && Equals(other);

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Cause != null ? Cause.GetHashCode() : 0) * 397) ^ InstanceId;
            }
        }
    }

    /// <summary>
    /// Instructs an event log to delete all snapshots with a sequence number greater than or equal to <see cref="LowerSequenceNr"/>.
    /// </summary>
    public readonly struct DeleteSnapshots : IEquatable<DeleteSnapshots>
    {
        public DeleteSnapshots(long lowerSequenceNr)
        {
            LowerSequenceNr = lowerSequenceNr;
        }

        public long LowerSequenceNr { get; }

        public bool Equals(DeleteSnapshots other) => LowerSequenceNr == other.LowerSequenceNr;
        public override bool Equals(object obj) => obj is DeleteSnapshots other && Equals(other);
        public override int GetHashCode() => LowerSequenceNr.GetHashCode();
    }

    /// <summary>
    /// Success reply after a <see cref="DeleteSnapshots"/>.
    /// </summary>
    public readonly struct DeleteSnapshotsSuccess : IEquatable<DeleteSnapshotsSuccess>
    {
        public bool Equals(DeleteSnapshotsSuccess other) => true;

        public override bool Equals(object obj) => obj is DeleteSnapshotsSuccess;

        public override int GetHashCode() => typeof(DeleteSnapshotsSuccess).GetHashCode();
    }

    /// <summary>
    /// Failure reply after a <see cref="DeleteSnapshots"/>.
    /// </summary>
    public readonly struct DeleteSnapshotsFailure : IFailure<Exception>, IEquatable<DeleteSnapshotsFailure>
    {
        public DeleteSnapshotsFailure(Exception cause)
        {
            Cause = cause;
        }

        public Exception Cause { get; }

        public bool Equals(DeleteSnapshotsFailure other) => Equals(Cause, other.Cause);
        public override bool Equals(object obj) => obj is DeleteSnapshotsFailure other && Equals(other);
        public override int GetHashCode() => (Cause != null ? Cause.GetHashCode() : 0);
    }
}
