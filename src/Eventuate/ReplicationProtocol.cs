using Akka.Actor;
using Eventuate.EventLogs;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;

namespace Eventuate.ReplicationProtocol
{
    /// <summary>
    /// <see cref="ReplicationEndpoint"/> info object. Exchanged between replication endpoints to establish replication connections.
    /// </summary>
    public readonly struct ReplicationEndpointInfo : ISerializable
    {
        public ReplicationEndpointInfo(string endpointId, ImmutableDictionary<string, long> logSequenceNumbers)
        {
            EndpointId = endpointId;
            LogSequenceNumbers = logSequenceNumbers;
        }

        /// <summary>
        /// Replication endpoint id.
        /// </summary>
        public string EndpointId { get; }

        /// <summary>
        /// Sequence numbers of logs managed by the replication endpoint.
        /// </summary>
        public ImmutableDictionary<string, long> LogSequenceNumbers { get; }

        /// <summary>
        /// The names of logs managed by the <see cref="ReplicationEndpoint"/>.
        /// </summary>
        public IEnumerable<string> LogNames => LogSequenceNumbers.Keys;

        /// <summary>
        /// Creates a log identifier from this info object's <paramref name="endpointId"/> and <paramref name="logName"/>.
        /// </summary>
        /// <param name="logName"></param>
        /// <returns></returns>
        public string LogId(string logName) => LogId(this.EndpointId, logName);

        /// <summary>
        /// Creates a log identifier from <paramref name="endpointId"/> and <paramref name="logName"/>.
        /// </summary>
        public static string LogId(string endpointId, string logName) => $"{endpointId}_{logName}";
    }

    /// <summary>
    /// Instructs a remote <see cref="ReplicationEndpoint"/> to return a <see cref="ReplicationEndpointInfo"/> object.
    /// </summary>
    internal readonly struct GetReplicationEndpointInfo : ISerializable { }

    /// <summary>
    /// Success reply to <see cref="GetReplicationEndpointInfo"/>.
    /// </summary>
    internal readonly struct GetReplicationEndpointInfoSuccess : ISerializable
    {
        public GetReplicationEndpointInfoSuccess(ReplicationEndpointInfo info)
        {
            Info = info;
        }

        public ReplicationEndpointInfo Info { get; }
    }

    /// <summary>
    /// Used to synchronize replication progress between two <see cref="ReplicationEndpoint"/>s when disaster recovery
    /// is initiated (through <see cref="ReplicationEndpoint.Recover"/>. Sent by the <see cref="ReplicationEndpoint"/>
    /// that is recovered to instruct a remote <see cref="ReplicationEndpoint"/> to
    /// 
    /// - reset locally stored replication progress according to the sequence numbers given in <see cref="Info"/> and
    /// - respond with a <see cref="ReplicationEndpoint"/> containing the after disaster progress of its logs
    /// </summary>
    internal readonly struct SynchronizeReplicationProgress : ISerializable
    {
        public SynchronizeReplicationProgress(ReplicationEndpointInfo info)
        {
            Info = info;
        }

        public ReplicationEndpointInfo Info { get; }
    }

    /// <summary>
    /// Successful response to a <see cref="SynchronizeReplicationProgress"/> request.
    /// </summary>
    internal readonly struct SynchronizeReplicationProgressSuccess : ISerializable
    {
        public SynchronizeReplicationProgressSuccess(ReplicationEndpointInfo info)
        {
            Info = info;
        }

        public ReplicationEndpointInfo Info { get; }
    }

    /// <summary>
    /// Failure response to a <see cref="SynchronizeReplicationProgress"/> request.
    /// </summary>
    internal readonly struct SynchronizeReplicationProgressFailure : ISerializable
    {
        public SynchronizeReplicationProgressFailure(SynchronizeReplicationProgressException cause)
        {
            Cause = cause;
        }

        public SynchronizeReplicationProgressException Cause { get; }
    }

    /// <summary>
    /// Base class for all <see cref="SynchronizeReplicationProgressFailure"/> `cause`s.
    /// </summary>
    internal abstract class SynchronizeReplicationProgressException : Exception
    {
        public SynchronizeReplicationProgressException(string message) : base(message) { }
    }

    /// <summary>
    /// Indicates a problem synchronizing the replication progress of a remote <see cref="ReplicationEndpoint"/>
    /// </summary>
    internal class SynchronizeReplicationProgressSourceException : SynchronizeReplicationProgressException, ISerializable
    {
        public SynchronizeReplicationProgressSourceException(string message) : base($"Failure when updating local replication progress: {message}")
        {
        }
    }

    /// <summary>
    /// Update notification sent to a replicator indicating that new events are available for replication.
    /// </summary>
    public readonly struct ReplicationDue : ISerializable { }

    /// <summary>
    /// Requests the clock from an event log.
    /// </summary>
    public readonly struct GetEventLogClock { }

    /// <summary>
    /// Success reply after a <see cref="GetEventLogClock"/>.
    /// </summary>
    public readonly struct GetEventLogClockSuccess
    {
        public GetEventLogClockSuccess(EventLogClock clock)
        {
            Clock = clock;
        }

        public EventLogClock Clock { get; }
    }

    /// <summary>
    /// Requests all local replication progresses from a log. The local replication progress is the sequence number
    /// in the remote log up to which the local log has replicated all events from the remote log.
    /// </summary>
    public readonly struct GetReplicationProgresses { }

    /// <summary>
    /// Success reply after a <see cref="GetReplicationProgresses"/>.
    /// </summary>
    public readonly struct GetReplicationProgressesSuccess
    {
        public GetReplicationProgressesSuccess(ImmutableDictionary<string, long> progresses) {
            Progresses = progresses;
        }

        public ImmutableDictionary<string, long> Progresses { get; }
    }

    /// <summary>
    /// Failure reply after a <see cref="GetReplicationProgresses"/>.
    /// </summary>
    public readonly struct GetReplicationProgressesFailure
    {
        public GetReplicationProgressesFailure(Exception cause)
        {
            Cause = cause;
        }

        public Exception Cause { get; }
    }

    /// <summary>
    /// Requests the local replication progress for given <see cref="SourceLogId"/> from a target log.
    /// </summary>
    /// <seealso cref="GetReplicationProgresses"/>
    public readonly struct GetReplicationProgress
    {
        public GetReplicationProgress(string sourceLogId)
        {
            SourceLogId = sourceLogId;
        }

        public string SourceLogId { get; }
    }

    /// <summary>
    /// Success reply after a <see cref="GetReplicationProgress"/>.
    /// </summary>
    public sealed class GetReplicationProgressSuccess
    {
        public GetReplicationProgressSuccess(string sourceLogId, long storedReplicationProgress, VectorTime currentTargetVersionVector)
        {
            SourceLogId = sourceLogId;
            StoredReplicationProgress = storedReplicationProgress;
            CurrentTargetVersionVector = currentTargetVersionVector;
        }

        public string SourceLogId { get; }
        public long StoredReplicationProgress { get; }
        public VectorTime CurrentTargetVersionVector { get; }
    }

    /// <summary>
    /// Failure reply after a <see cref="GetReplicationProgress"/>.
    /// </summary>
    public readonly struct GetReplicationProgressFailure
    {
        public GetReplicationProgressFailure(Exception cause)
        {
            Cause = cause;
        }

        public Exception Cause { get; }
    }

    /// <summary>
    /// Requests a target log to set the given <see cref="ReplicationProgress"/> for <see cref="SourceLogId"/>.
    /// </summary>
    public readonly struct SetReplicationProgress
    {
        public SetReplicationProgress(string sourceLogId, long replicationProgress)
        {
            SourceLogId = sourceLogId;
            ReplicationProgress = replicationProgress;
        }

        public string SourceLogId { get; }
        public long ReplicationProgress { get; }
    }

    /// <summary>
    /// Success reply after a <see cref="SetReplicationProgress"/>.
    /// </summary>
    public readonly struct SetReplicationProgressSuccess
    {
        public SetReplicationProgressSuccess(string sourceLogId, long storedReplicationProgress)
        {
            SourceLogId = sourceLogId;
            StoredReplicationProgress = storedReplicationProgress;
        }

        public string SourceLogId { get; }
        public long StoredReplicationProgress { get; }
    }

    /// <summary>
    /// Failure reply after a <see cref="SetReplicationProgress"/>.
    /// </summary>
    public readonly struct SetReplicationProgressFailure
    {
        public SetReplicationProgressFailure(Exception cause)
        {
            Cause = cause;
        }

        public Exception Cause { get; }
    }

    /// <summary>
    /// <see cref="ReplicationRead"/> requests are sent within this envelope to allow a remote acceptor to
    /// dispatch the request to the appropriate log.
    /// </summary>
    public sealed class ReplicationReadEnvelope : ISerializable
    {
        public ReplicationReadEnvelope(ReplicationRead payload, string logName, string targetApplicationName, ApplicationVersion targetApplicationVersion)
        {
            Payload = payload;
            LogName = logName;
            TargetApplicationName = targetApplicationName;
            TargetApplicationVersion = targetApplicationVersion;
        }

        public ReplicationRead Payload { get; }
        public string LogName { get; }
        public string TargetApplicationName { get; }
        public ApplicationVersion TargetApplicationVersion { get; }

        public bool IncompatibleWith(string sourceApplicationName, ApplicationVersion sourceApplicationVersion) =>
            TargetApplicationName == sourceApplicationName && TargetApplicationVersion < sourceApplicationVersion;
    }

    /// <summary>
    /// Instructs a source log to read up to <see cref="Max"/> events starting at <see cref="FromSequenceNr"/> and applying
    /// the given replication <see cref="Filter"/>.
    /// </summary>
    public sealed class ReplicationRead : ISerializable
    {
        public ReplicationRead(long fromSequenceNr, int max, int scanLimit, ReplicationFilter filter, string targetLogId, IActorRef replicator, VectorTime currentTargetVersionVector)
        {
            FromSequenceNr = fromSequenceNr;
            Max = max;
            ScanLimit = scanLimit;
            Filter = filter;
            TargetLogId = targetLogId;
            Replicator = replicator;
            CurrentTargetVersionVector = currentTargetVersionVector;
        }

        public long FromSequenceNr { get; }
        public int Max { get; }
        public int ScanLimit { get; }
        public ReplicationFilter Filter { get; }
        public string TargetLogId { get; }
        public IActorRef Replicator { get; }
        public VectorTime CurrentTargetVersionVector { get; }
    }

    /// <summary>
    /// Success reply after a <see cref="ReplicationRead"/>.
    /// </summary>
    public sealed class ReplicationReadSuccess : ISerializable
    {
        public ReplicationReadSuccess(IReadOnlyCollection<DurableEvent> events, long fromSequenceNr, long replicationProgress, string targetLogId, VectorTime currentSourceVersionVector)
        {
            Events = events;
            FromSequenceNr = fromSequenceNr;
            ReplicationProgress = replicationProgress;
            TargetLogId = targetLogId;
            CurrentSourceVersionVector = currentSourceVersionVector;
        }

        public IReadOnlyCollection<DurableEvent> Events { get; }
        public long FromSequenceNr { get; }
        public long ReplicationProgress { get; }
        public string TargetLogId { get; }
        public VectorTime CurrentSourceVersionVector { get; }
    }

    /// <summary>
    /// Failure reply after a <see cref="ReplicationRead"/>.
    /// </summary>
    public sealed class ReplicationReadFailure : ISerializable
    {
        public ReplicationReadFailure(ReplicationReadException cause, string targetLogId)
        {
            Cause = cause;
            TargetLogId = targetLogId;
        }

        public ReplicationReadException Cause { get; }
        public string TargetLogId { get; }
    }

    /// <summary>
    /// Instructs an event log to batch-execute the given <see cref="Writes"/>.
    /// </summary>
    public readonly struct ReplicationWriteMany
    {
        public ReplicationWriteMany(IEnumerable<ReplicationWrite> writes)
        {
            Writes = writes;
        }

        public IEnumerable<ReplicationWrite> Writes { get; }
    }

    /// <summary>
    /// Completion reply after a <see cref="ReplicationWriteMany"/>.
    /// </summary>
    public readonly struct ReplicationWriteManyComplete { }

    /// <summary>
    /// Source-scoped replication metadata.
    /// </summary>
    public readonly struct ReplicationMetadata
    {
        public ReplicationMetadata(long replicationProgress, VectorTime currentVersionVector)
        {
            ReplicationProgress = replicationProgress;
            CurrentVersionVector = currentVersionVector;
        }

        /// <summary>
        /// Replication read progress at source log.
        /// </summary>
        public long ReplicationProgress { get; }

        /// <summary>
        /// When used with <see cref="ReplicationWrite"/> the current version vector at the source log. When
        /// used with <see cref="ReplicationWriteSuccess"/> the current version vector at the target log.
        /// </summary>
        public VectorTime CurrentVersionVector { get; }

        public ReplicationMetadata WithVersionVector(VectorTime versionVector) =>
            new ReplicationMetadata(ReplicationProgress, versionVector);
    }

    /// <summary>
    /// Instructs a target log to write replicated `events` from one or more source logs along with the latest read
    /// positions in the source logs.
    /// </summary>
    public sealed class ReplicationWrite : IUpdateableEventBatch<ReplicationWrite>
    {
        public ReplicationWrite(IReadOnlyCollection<DurableEvent> events, ImmutableDictionary<string, ReplicationMetadata> metadata, bool continueReplication = false, IActorRef replyTo = null)
        {
            Events = events;
            Metadata = metadata;
            ContinueReplication = continueReplication;
            ReplyTo = replyTo;
        }

        public IReadOnlyCollection<DurableEvent> Events { get; }
        public ImmutableDictionary<string, ReplicationMetadata> Metadata { get; }
        public bool ContinueReplication { get; }
        public IActorRef ReplyTo { get; }
        public int Count => Events.Count;
        IEnumerable<DurableEvent> IDurableEventBatch.Events => Events;
        public IEnumerable<string> SourceLogIds => Metadata.Keys;
        public IEnumerable<KeyValuePair<string, long>> ReplicationProgresses => Metadata.Select(kv => new KeyValuePair<string, long>(kv.Key, kv.Value.ReplicationProgress));

        public ReplicationWrite Update(params DurableEvent[] events) => new ReplicationWrite(events, Metadata, ContinueReplication, ReplyTo);
        public ReplicationWrite WithReplyToDefault(IActorRef replyTo) => this.ReplyTo is null ? new ReplicationWrite(Events, Metadata, ContinueReplication, replyTo) : this;

    }

    /// <summary>
    /// Success reply after a <see cref="ReplicationWrite"/>.
    /// </summary>
    public sealed class ReplicationWriteSuccess
    {
        public ReplicationWriteSuccess(IEnumerable<DurableEvent> events, ImmutableDictionary<string, ReplicationMetadata> metadata, bool continueReplication = false)
        {
            Events = events;
            Metadata = metadata;
            ContinueReplication = continueReplication;
        }

        public IEnumerable<DurableEvent> Events { get; }
        public ImmutableDictionary<string, ReplicationMetadata> Metadata { get; }
        public bool ContinueReplication { get; }
    }

    /// <summary>
    /// Failure reply after a <see cref="ReplicationWrite"/>.
    /// </summary>
    public readonly struct ReplicationWriteFailure
    {
        public ReplicationWriteFailure(Exception cause)
        {
            Cause = cause;
        }

        public Exception Cause { get; }
    }

    /// <summary>
    /// Base class for all <see cref="ReplicationReadFailure"/> `cause`s.
    /// </summary>
    public abstract class ReplicationReadException : Exception
    {
        public ReplicationReadException(string message) : base(message) { }
    }

    /// <summary>
    /// Indicates a problem reading events from the backend store at the source endpoint.
    /// </summary>
    public class ReplicationReadSourceException : ReplicationReadException
    {
        public ReplicationReadSourceException(string message) : base(message)
        {
        }
    }

    /// <summary>
    /// Indicates that a <see cref="ReplicationRead"/> request timed out.
    /// </summary>
    public class ReplicationReadTimeoutException : ReplicationReadException
    {
        public ReplicationReadTimeoutException(TimeSpan timeout) : base($"Replication read timed out after {timeout}")
        {
        }
    }

    /// <summary>
    /// Instruct a log to adjust the sequence nr of the internal <see cref="EventLogClock"/> to the version vector.
    /// This is ensures that the sequence nr is greater than or equal to the log's entry in the version vector.
    /// </summary>
    public readonly struct AdjustEventLogClock { }

    /// <summary>
    /// Success reply after a <see cref="AdjustEventLogClock"/>. Contains the adjusted clock.
    /// </summary>
    public readonly struct AdjustEventLogClockSuccess
    {
        public AdjustEventLogClockSuccess(EventLogClock clock)
        {
            Clock = clock;
        }

        public EventLogClock Clock { get; }
    }

    /// <summary>
    /// Failure reply after a <see cref="AdjustEventLogClock"/>.
    /// </summary>
    public readonly struct AdjustEventLogClockFailure
    {
        public AdjustEventLogClockFailure(Exception cause)
        {
            Cause = cause;
        }

        public Exception Cause { get; }
    }

    /// <summary>
    /// Indicates that events cannot be replication from a source <see cref="ReplicationEndpoint"/> to a target <see cref="ReplicationEndpoint"/>
    /// because their application versions are incompatible.
    /// </summary>
    public class IncompatibleApplicationVersionException : ReplicationReadException, ISerializable
    {
        public IncompatibleApplicationVersionException(string sourceEndpointId, ApplicationVersion sourceApplicationVersion, ApplicationVersion targetApplicationVersion) 
            : base($"Event replication rejected by remote endpoint {sourceEndpointId}. Target {targetApplicationVersion} not compatible with source {sourceApplicationVersion}")
        {
        }

    }
}
