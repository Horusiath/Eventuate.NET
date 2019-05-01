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
    public sealed class WriteMany
    {
        public WriteMany(IEnumerable<Write> writes)
        {
            Writes = writes;
        }

        public IEnumerable<Write> Writes { get; }
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
    public sealed class WriteSuccess
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
    }

    /// <summary>
    /// Failure reply after a <see cref="Write"/>.
    /// </summary>
    public sealed class WriteFailure : IFailure<Exception>
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
    }

    /// <summary>
    /// Sent by an event log to all registered participants, if `event` has been successfully written.
    /// This message is not sent to a participant if that participant has sent a corresponding <see cref="Write"/>.
    /// </summary>
    public readonly struct Written
    {
        public Written(DurableEvent @event)
        {
            Event = @event;
        }

        public DurableEvent Event { get; }
    }

    /// <summary>
    /// Instructs an event log to read up to <see cref="Max"/> events starting at <see cref="FromSequenceNr"/>. 
    /// If <see cref="AggregateId"/> is defined, only those events that have the aggregate id in their 
    /// `destinationAggregateIds` are returned.
    /// </summary>
    public sealed class Replay
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
    }

    /// <summary>
    /// Success reply after a <see cref="Replay"/>.
    /// </summary>
    public sealed class ReplaySuccess
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
    }

    /// <summary>
    /// Failure reply after a <see cref="Replay"/>.
    /// </summary>
    public sealed class ReplayFailure : IFailure<Exception>
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
    }

    /// <summary>
    /// Internal message to trigger a new <see cref="Replay"/> attempt.
    /// </summary>
    internal readonly struct ReplayRetry
    {
        public ReplayRetry(long replayProgress)
        {
            ReplayProgress = replayProgress;
        }

        public long ReplayProgress { get; }
    }

    /// <summary>
    /// Instructs an event log to delete events with a sequence nr less or equal a given one.
    /// Deleted events are not replayed any more, however depending on the log implementation
    /// and <see cref="RemoteLogIds"/> they might still be replicated.
    /// </summary>
    public readonly struct Delete
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
    }

    /// <summary>
    /// Success reply after a <see cref="Delete"/>.
    /// </summary>
    /// @param deletedTo 
    /// 
    public readonly struct DeleteSuccess
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
    }

    /// <summary>
    /// Failure reply after a <see cref="Delete"/>.
    /// </summary>
    public readonly struct DeleteFailure : IFailure<Exception>
    {
        public DeleteFailure(Exception cause)
        {
            Cause = cause;
        }

        public Exception Cause { get; }
    }

    /// <summary>
    /// Instructs an event log to save the given <see cref="Snapshot"/>.
    /// </summary>
    public sealed class SaveSnapshot
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
    }

    /// <summary>
    /// Success reply after a <see cref="SaveSnapshot"/>.
    /// </summary>
    public sealed class SaveSnapshotSuccess
    {
        public SaveSnapshotSuccess(SnapshotMetadata metadata, int instanceId)
        {
            Metadata = metadata;
            InstanceId = instanceId;
        }

        public SnapshotMetadata Metadata { get; }
        public int InstanceId { get; }
    }

    /// <summary>
    ///  Failure reply after a <see cref="SaveSnapshot"/>.
    /// </summary>
    public sealed class SaveSnapshotFailure : IFailure<Exception>
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
    }

    /// <summary>
    /// Instructs an event log to load the most recent snapshot for <see cref="EmitterId"/>.
    /// </summary>
    public readonly struct LoadSnapshot
    {
        public LoadSnapshot(string emitterId, int instanceId)
        {
            EmitterId = emitterId;
            InstanceId = instanceId;
        }

        public string EmitterId { get; }
        public int InstanceId { get; }
    }

    /// <summary>
    /// Success reply after a <see cref="LoadSnapshot"/>.
    /// </summary>
    public readonly struct LoadSnapshotSuccess
    {
        public LoadSnapshotSuccess(Snapshot snapshot, int instanceId)
        {
            Snapshot = snapshot;
            InstanceId = instanceId;
        }

        public Snapshot Snapshot { get; }
        public int InstanceId { get; }
    }

    /// <summary>
    /// Failure reply after a <see cref="LoadSnapshot"/>.
    /// </summary>
    public readonly struct LoadSnapshotFailure : IFailure<Exception>
    {
        public LoadSnapshotFailure(Exception cause, int instanceId)
        {
            Cause = cause;
            InstanceId = instanceId;
        }

        public Exception Cause { get; }
        public int InstanceId { get; }
    }

    /// <summary>
    /// Instructs an event log to delete all snapshots with a sequence number greater than or equal to <see cref="LowerSequenceNr"/>.
    /// </summary>
    public readonly struct DeleteSnapshots
    {
        public DeleteSnapshots(long lowerSequenceNr)
        {
            LowerSequenceNr = lowerSequenceNr;
        }

        public long LowerSequenceNr { get; }
    }

    /// <summary>
    /// Success reply after a <see cref="DeleteSnapshots"/>.
    /// </summary>
    public readonly struct DeleteSnapshotsSuccess
    {

    }

    /// <summary>
    /// Failure reply after a <see cref="DeleteSnapshots"/>.
    /// </summary>
    public readonly struct DeleteSnapshotsFailure : IFailure<Exception>
    {
        public DeleteSnapshotsFailure(Exception cause)
        {
            Cause = cause;
        }

        public Exception Cause { get; }
    }
}
