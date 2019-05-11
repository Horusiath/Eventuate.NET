#region copyright
// -----------------------------------------------------------------------
//  <copyright file="ReplicationProtocol.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

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
    public readonly struct ReplicationEndpointInfo : IEquatable<ReplicationEndpointInfo>, ISerializable
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

        public bool Equals(ReplicationEndpointInfo other)
        {
            if (EndpointId != other.EndpointId) return false;
            if (ReferenceEquals(LogSequenceNumbers, other.LogSequenceNumbers)) return true;
            if (LogSequenceNumbers.Count != other.LogSequenceNumbers.Count) return false;

            foreach (var pid in LogSequenceNumbers.Keys.Union(other.LogSequenceNumbers.Keys))
            {
                if (!LogSequenceNumbers.TryGetValue(pid, out var a) ||
                    !other.LogSequenceNumbers.TryGetValue(pid, out var b) || a != b) return false;
            }

            return true;
        }

        public override bool Equals(object obj) => obj is ReplicationEndpointInfo other && Equals(other);

        public override int GetHashCode()
        {
            unchecked
            {
                var hash = EndpointId.GetHashCode();
                foreach (var (pid, seqNr) in LogSequenceNumbers)
                {
                    hash = (hash * 397) ^ pid.GetHashCode() ^ seqNr.GetHashCode();
                }

                return hash;
            }
        }
    }

    /// <summary>
    /// Instructs a remote <see cref="ReplicationEndpoint"/> to return a <see cref="ReplicationEndpointInfo"/> object.
    /// </summary>
    internal readonly struct GetReplicationEndpointInfo : ISerializable { }

    /// <summary>
    /// Success reply to <see cref="GetReplicationEndpointInfo"/>.
    /// </summary>
    internal readonly struct GetReplicationEndpointInfoSuccess : IEquatable<GetReplicationEndpointInfoSuccess>, ISerializable
    {
        public GetReplicationEndpointInfoSuccess(ReplicationEndpointInfo info)
        {
            Info = info;
        }

        public ReplicationEndpointInfo Info { get; }

        public bool Equals(GetReplicationEndpointInfoSuccess other) => Info.Equals(other.Info);
        public override bool Equals(object obj) => obj is GetReplicationEndpointInfoSuccess other && Equals(other);
        public override int GetHashCode() => Info.GetHashCode();
    }

    /// <summary>
    /// Used to synchronize replication progress between two <see cref="ReplicationEndpoint"/>s when disaster recovery
    /// is initiated (through <see cref="ReplicationEndpoint.Recover"/>. Sent by the <see cref="ReplicationEndpoint"/>
    /// that is recovered to instruct a remote <see cref="ReplicationEndpoint"/> to
    /// 
    /// - reset locally stored replication progress according to the sequence numbers given in <see cref="Info"/> and
    /// - respond with a <see cref="ReplicationEndpoint"/> containing the after disaster progress of its logs
    /// </summary>
    internal readonly struct SynchronizeReplicationProgress : IEquatable<SynchronizeReplicationProgress>, ISerializable
    {
        public SynchronizeReplicationProgress(ReplicationEndpointInfo info)
        {
            Info = info;
        }

        public ReplicationEndpointInfo Info { get; }

        public bool Equals(SynchronizeReplicationProgress other) => Info.Equals(other.Info);
        public override bool Equals(object obj) => obj is SynchronizeReplicationProgress other && Equals(other);
        public override int GetHashCode() => Info.GetHashCode();
    }

    /// <summary>
    /// Successful response to a <see cref="SynchronizeReplicationProgress"/> request.
    /// </summary>
    internal readonly struct SynchronizeReplicationProgressSuccess : IEquatable<SynchronizeReplicationProgressSuccess>, ISerializable
    {
        public SynchronizeReplicationProgressSuccess(ReplicationEndpointInfo info)
        {
            Info = info;
        }

        public ReplicationEndpointInfo Info { get; }

        public bool Equals(SynchronizeReplicationProgressSuccess other) => Info.Equals(other.Info);
        public override bool Equals(object obj) => obj is SynchronizeReplicationProgressSuccess other && Equals(other);
        public override int GetHashCode() => Info.GetHashCode();
    }

    /// <summary>
    /// Failure response to a <see cref="SynchronizeReplicationProgress"/> request.
    /// </summary>
    internal readonly struct SynchronizeReplicationProgressFailure : ISerializable, IEquatable<SynchronizeReplicationProgressFailure>, IFailure<SynchronizeReplicationProgressException>
    {
        public SynchronizeReplicationProgressFailure(SynchronizeReplicationProgressException cause)
        {
            Cause = cause;
        }

        public SynchronizeReplicationProgressException Cause { get; }

        public bool Equals(SynchronizeReplicationProgressFailure other) => Equals(Cause, other.Cause);
        public override bool Equals(object obj) => obj is SynchronizeReplicationProgressFailure other && Equals(other);
        public override int GetHashCode() => (Cause != null ? Cause.GetHashCode() : 0);
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
    public readonly struct GetEventLogClockSuccess : IEquatable<GetEventLogClockSuccess>
    {
        public GetEventLogClockSuccess(EventLogClock clock)
        {
            Clock = clock;
        }

        public EventLogClock Clock { get; }

        public bool Equals(GetEventLogClockSuccess other) => Equals(Clock, other.Clock);
        public override bool Equals(object obj) => obj is GetEventLogClockSuccess other && Equals(other);
        public override int GetHashCode() => Clock.GetHashCode();
    }

    /// <summary>
    /// Requests all local replication progresses from a log. The local replication progress is the sequence number
    /// in the remote log up to which the local log has replicated all events from the remote log.
    /// </summary>
    public readonly struct GetReplicationProgresses { }

    /// <summary>
    /// Success reply after a <see cref="GetReplicationProgresses"/>.
    /// </summary>
    public readonly struct GetReplicationProgressesSuccess : IEquatable<GetReplicationProgressesSuccess>
    {
        public GetReplicationProgressesSuccess(ImmutableDictionary<string, long> progresses) {
            Progresses = progresses;
        }

        public ImmutableDictionary<string, long> Progresses { get; }

        public bool Equals(GetReplicationProgressesSuccess other)
        {
            if (ReferenceEquals(Progresses, other.Progresses)) return true;
            if (Progresses.Count != other.Progresses.Count) return false;

            foreach (var pid in Progresses.Keys.Union(other.Progresses.Keys))
            {
                if (!Progresses.TryGetValue(pid, out var a) ||
                    !other.Progresses.TryGetValue(pid, out var b) || a != b) return false;
            }

            return true;
        }

        public override bool Equals(object obj) => obj is GetReplicationProgressesSuccess other && Equals(other);

        public override int GetHashCode()
        {
            unchecked
            {
                var hash = 0;
                foreach (var (pid, seqNr) in Progresses)
                {
                    hash = (hash * 397) ^ pid.GetHashCode() ^ seqNr.GetHashCode();
                }

                return hash;
            }
        }
    }

    /// <summary>
    /// Failure reply after a <see cref="GetReplicationProgresses"/>.
    /// </summary>
    public readonly struct GetReplicationProgressesFailure : IFailure<Exception>, IEquatable<GetReplicationProgressesFailure>
    {
        public GetReplicationProgressesFailure(Exception cause)
        {
            Cause = cause;
        }

        public Exception Cause { get; }

        public bool Equals(GetReplicationProgressesFailure other) => Equals(Cause, other.Cause);
        public override bool Equals(object obj) => obj is GetReplicationProgressesFailure other && Equals(other);
        public override int GetHashCode() => (Cause != null ? Cause.GetHashCode() : 0);
    }

    /// <summary>
    /// Requests the local replication progress for given <see cref="SourceLogId"/> from a target log.
    /// </summary>
    /// <seealso cref="GetReplicationProgresses"/>
    public readonly struct GetReplicationProgress : IEquatable<GetReplicationProgress>
    {
        public GetReplicationProgress(string sourceLogId)
        {
            SourceLogId = sourceLogId;
        }

        public string SourceLogId { get; }

        public bool Equals(GetReplicationProgress other) => string.Equals(SourceLogId, other.SourceLogId);
        public override bool Equals(object obj) => obj is GetReplicationProgress other && Equals(other);
        public override int GetHashCode() => (SourceLogId != null ? SourceLogId.GetHashCode() : 0);
    }

    /// <summary>
    /// Success reply after a <see cref="GetReplicationProgress"/>.
    /// </summary>
    public sealed class GetReplicationProgressSuccess : IEquatable<GetReplicationProgressSuccess>
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

        public bool Equals(GetReplicationProgressSuccess other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return string.Equals(SourceLogId, other.SourceLogId) 
                   && StoredReplicationProgress == other.StoredReplicationProgress 
                   && Equals(CurrentTargetVersionVector, other.CurrentTargetVersionVector);
        }

        public override bool Equals(object obj) => ReferenceEquals(this, obj) || obj is GetReplicationProgressSuccess other && Equals(other);

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (SourceLogId != null ? SourceLogId.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ StoredReplicationProgress.GetHashCode();
                hashCode = (hashCode * 397) ^ CurrentTargetVersionVector.GetHashCode();
                return hashCode;
            }
        }
    }

    /// <summary>
    /// Failure reply after a <see cref="GetReplicationProgress"/>.
    /// </summary>
    public readonly struct GetReplicationProgressFailure : IFailure<Exception>, IEquatable<GetReplicationProgressFailure>
    {
        public GetReplicationProgressFailure(Exception cause)
        {
            Cause = cause;
        }

        public Exception Cause { get; }

        public bool Equals(GetReplicationProgressFailure other) => Equals(Cause, other.Cause);
        public override bool Equals(object obj) => obj is GetReplicationProgressFailure other && Equals(other);
        public override int GetHashCode() => (Cause != null ? Cause.GetHashCode() : 0);
    }

    /// <summary>
    /// Requests a target log to set the given <see cref="ReplicationProgress"/> for <see cref="SourceLogId"/>.
    /// </summary>
    public readonly struct SetReplicationProgress : IEquatable<SetReplicationProgress>
    {
        public SetReplicationProgress(string sourceLogId, long replicationProgress)
        {
            SourceLogId = sourceLogId;
            ReplicationProgress = replicationProgress;
        }

        public string SourceLogId { get; }
        public long ReplicationProgress { get; }

        public bool Equals(SetReplicationProgress other) => string.Equals(SourceLogId, other.SourceLogId) && ReplicationProgress == other.ReplicationProgress;
        public override bool Equals(object obj) => obj is SetReplicationProgress other && Equals(other);

        public override int GetHashCode()
        {
            unchecked
            {
                return ((SourceLogId != null ? SourceLogId.GetHashCode() : 0) * 397) ^ ReplicationProgress.GetHashCode();
            }
        }
    }

    /// <summary>
    /// Success reply after a <see cref="SetReplicationProgress"/>.
    /// </summary>
    public readonly struct SetReplicationProgressSuccess : IEquatable<SetReplicationProgressSuccess>
    {
        public SetReplicationProgressSuccess(string sourceLogId, long storedReplicationProgress)
        {
            SourceLogId = sourceLogId;
            StoredReplicationProgress = storedReplicationProgress;
        }

        public string SourceLogId { get; }
        public long StoredReplicationProgress { get; }

        public bool Equals(SetReplicationProgressSuccess other) => string.Equals(SourceLogId, other.SourceLogId) && StoredReplicationProgress == other.StoredReplicationProgress;

        public override bool Equals(object obj) => obj is SetReplicationProgressSuccess other && Equals(other);

        public override int GetHashCode()
        {
            unchecked
            {
                return ((SourceLogId != null ? SourceLogId.GetHashCode() : 0) * 397) ^ StoredReplicationProgress.GetHashCode();
            }
        }
    }

    /// <summary>
    /// Failure reply after a <see cref="SetReplicationProgress"/>.
    /// </summary>
    public readonly struct SetReplicationProgressFailure : IFailure<Exception>, IEquatable<SetReplicationProgressFailure>
    {
        public SetReplicationProgressFailure(Exception cause)
        {
            Cause = cause;
        }

        public Exception Cause { get; }

        public bool Equals(SetReplicationProgressFailure other) => Equals(Cause, other.Cause);
        public override bool Equals(object obj) => obj is SetReplicationProgressFailure other && Equals(other);
        public override int GetHashCode() => (Cause != null ? Cause.GetHashCode() : 0);
    }

    /// <summary>
    /// <see cref="ReplicationRead"/> requests are sent within this envelope to allow a remote acceptor to
    /// dispatch the request to the appropriate log.
    /// </summary>
    public sealed class ReplicationReadEnvelope : ISerializable, IEquatable<ReplicationReadEnvelope>
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

        public bool Equals(ReplicationReadEnvelope other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Equals(Payload, other.Payload) 
                   && string.Equals(LogName, other.LogName) 
                   && string.Equals(TargetApplicationName, other.TargetApplicationName) 
                   && TargetApplicationVersion.Equals(other.TargetApplicationVersion);
        }

        public override bool Equals(object obj) => ReferenceEquals(this, obj) || obj is ReplicationReadEnvelope other && Equals(other);

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (Payload != null ? Payload.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (LogName != null ? LogName.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (TargetApplicationName != null ? TargetApplicationName.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ TargetApplicationVersion.GetHashCode();
                return hashCode;
            }
        }
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
    public sealed class ReplicationReadSuccess : ISerializable, IEquatable<ReplicationReadSuccess>
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

        public bool Equals(ReplicationReadSuccess other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return FromSequenceNr == other.FromSequenceNr
                   && ReplicationProgress == other.ReplicationProgress
                   && string.Equals(TargetLogId, other.TargetLogId)
                   && Equals(CurrentSourceVersionVector, other.CurrentSourceVersionVector)
                   && Events.CollectionEquals(other.Events);
        }
        
        public override bool Equals(object obj) => ReferenceEquals(this, obj) || obj is ReplicationReadSuccess other && Equals(other);

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = FromSequenceNr.GetHashCode();
                hashCode = (hashCode * 397) ^ ReplicationProgress.GetHashCode();
                hashCode = (hashCode * 397) ^ (TargetLogId != null ? TargetLogId.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (CurrentSourceVersionVector != null ? CurrentSourceVersionVector.GetHashCode() : 0);
                foreach (var e in Events)
                {
                    hashCode = (hashCode * 397) ^ e.GetHashCode();
                }
                return hashCode;
            }
        }
    }

    /// <summary>
    /// Failure reply after a <see cref="ReplicationRead"/>.
    /// </summary>
    public sealed class ReplicationReadFailure : ISerializable, IFailure<ReplicationReadException>, IEquatable<ReplicationReadFailure>
    {
        public ReplicationReadFailure(ReplicationReadException cause, string targetLogId)
        {
            Cause = cause;
            TargetLogId = targetLogId;
        }

        public ReplicationReadException Cause { get; }
        public string TargetLogId { get; }

        public bool Equals(ReplicationReadFailure other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Equals(Cause, other.Cause) && string.Equals(TargetLogId, other.TargetLogId);
        }

        public override bool Equals(object obj) => ReferenceEquals(this, obj) || obj is ReplicationReadFailure other && Equals(other);

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Cause != null ? Cause.GetHashCode() : 0) * 397) ^ (TargetLogId != null ? TargetLogId.GetHashCode() : 0);
            }
        }
    }

    /// <summary>
    /// Instructs an event log to batch-execute the given <see cref="Writes"/>.
    /// </summary>
    public readonly struct ReplicationWriteMany : IEquatable<ReplicationWriteMany>
    {
        public ReplicationWriteMany(IReadOnlyCollection<ReplicationWrite> writes)
        {
            Writes = writes;
        }

        public IReadOnlyCollection<ReplicationWrite> Writes { get; }

        public bool Equals(ReplicationWriteMany other) => Writes.CollectionEquals(other.Writes);
        public override bool Equals(object obj) => obj is ReplicationWriteMany other && Equals(other);
        public override int GetHashCode() => Writes.GetCollectionHashCode();
    }

    /// <summary>
    /// Completion reply after a <see cref="ReplicationWriteMany"/>.
    /// </summary>
    public readonly struct ReplicationWriteManyComplete { }

    /// <summary>
    /// Source-scoped replication metadata.
    /// </summary>
    public readonly struct ReplicationMetadata : IEquatable<ReplicationMetadata>
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

        public bool Equals(ReplicationMetadata other) => ReplicationProgress == other.ReplicationProgress && Equals(CurrentVersionVector, other.CurrentVersionVector);
        public override bool Equals(object obj) => obj is ReplicationMetadata other && Equals(other);
        public override int GetHashCode()
        {
            unchecked
            {
                return (ReplicationProgress.GetHashCode() * 397) ^ (CurrentVersionVector != null ? CurrentVersionVector.GetHashCode() : 0);
            }
        }
    }

    /// <summary>
    /// Instructs a target log to write replicated `events` from one or more source logs along with the latest read
    /// positions in the source logs.
    /// </summary>
    public sealed class ReplicationWrite : IUpdateableEventBatch<ReplicationWrite>, IEquatable<ReplicationWrite>
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

        public bool Equals(ReplicationWrite other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return ContinueReplication == other.ContinueReplication
                   && Equals(ReplyTo, other.ReplyTo)
                   && Events.CollectionEquals(other.Events)
                   && Metadata.DictionaryEquals(other.Metadata);
        }

        public override bool Equals(object obj)
        {
            return ReferenceEquals(this, obj) || obj is ReplicationWrite other && Equals(other);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = Events.GetCollectionHashCode();
                hashCode = (hashCode * 397) ^ Metadata.GetDictionaryHashCode();
                hashCode = (hashCode * 397) ^ ContinueReplication.GetHashCode();
                hashCode = (hashCode * 397) ^ (ReplyTo != null ? ReplyTo.GetHashCode() : 0);
                return hashCode;
            }
        }
    }

    /// <summary>
    /// Success reply after a <see cref="ReplicationWrite"/>.
    /// </summary>
    public sealed class ReplicationWriteSuccess : IEquatable<ReplicationWriteSuccess>
    {
        public ReplicationWriteSuccess(IReadOnlyCollection<DurableEvent> events, ImmutableDictionary<string, ReplicationMetadata> metadata, bool continueReplication = false)
        {
            Events = events;
            Metadata = metadata;
            ContinueReplication = continueReplication;
        }

        public IReadOnlyCollection<DurableEvent> Events { get; }
        public ImmutableDictionary<string, ReplicationMetadata> Metadata { get; }
        public bool ContinueReplication { get; }

        public bool Equals(ReplicationWriteSuccess other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Events.CollectionEquals(other.Events) 
                   && Metadata.DictionaryEquals(other.Metadata) 
                   && ContinueReplication == other.ContinueReplication;
        }

        public override bool Equals(object obj) => ReferenceEquals(this, obj) || obj is ReplicationWriteSuccess other && Equals(other);

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = Events.GetCollectionHashCode();
                hashCode = (hashCode * 397) ^ Metadata.GetDictionaryHashCode();
                hashCode = (hashCode * 397) ^ ContinueReplication.GetHashCode();
                return hashCode;
            }
        }
    }

    /// <summary>
    /// Failure reply after a <see cref="ReplicationWrite"/>.
    /// </summary>
    public readonly struct ReplicationWriteFailure : IFailure<Exception>, IEquatable<ReplicationWriteFailure>
    {
        public ReplicationWriteFailure(Exception cause)
        {
            Cause = cause;
        }

        public Exception Cause { get; }

        public bool Equals(ReplicationWriteFailure other) => Equals(Cause, other.Cause);
        public override bool Equals(object obj) => obj is ReplicationWriteFailure other && Equals(other);
        public override int GetHashCode() => (Cause != null ? Cause.GetHashCode() : 0);
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
    public readonly struct AdjustEventLogClockSuccess : IEquatable<AdjustEventLogClockSuccess>
    {
        public AdjustEventLogClockSuccess(EventLogClock clock)
        {
            Clock = clock;
        }

        public EventLogClock Clock { get; }

        public bool Equals(AdjustEventLogClockSuccess other) => Equals(Clock, other.Clock);
        public override bool Equals(object obj) => obj is AdjustEventLogClockSuccess other && Equals(other);
        public override int GetHashCode() => (Clock != null ? Clock.GetHashCode() : 0);
    }

    /// <summary>
    /// Failure reply after a <see cref="AdjustEventLogClock"/>.
    /// </summary>
    public readonly struct AdjustEventLogClockFailure : IFailure<Exception>, IEquatable<AdjustEventLogClockFailure>
    {
        public AdjustEventLogClockFailure(Exception cause)
        {
            Cause = cause;
        }

        public Exception Cause { get; }

        public bool Equals(AdjustEventLogClockFailure other) => Equals(Cause, other.Cause);
        public override bool Equals(object obj) => obj is AdjustEventLogClockFailure other && Equals(other);
        public override int GetHashCode() => (Cause != null ? Cause.GetHashCode() : 0);
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
