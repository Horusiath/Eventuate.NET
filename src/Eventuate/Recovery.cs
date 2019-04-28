using Akka.Actor;
using Akka.Configuration;
using Eventuate.EventLogs;
using Eventuate.ReplicationProtocol;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Eventuate
{
    /// <summary>
    /// <see cref="ReplicationEndpoint.Recover"/> completes with this exception if recovery fails.
    /// </summary>
    public class RecoveryException : Exception
    {
        public RecoveryException(Exception inner, bool partialUpdate)
            : base(partialUpdate ? "Recovery failed after doing some partial updates" : "Recovery failed without doing any updates", inner)
        {
            PartialUpdate = partialUpdate;
        }

        /// <summary>
        /// Set to `true` if recovery already made partial updates, `false` if recovery 
        /// failed without having made partial updates to replication partners.
        /// </summary>
        public bool PartialUpdate { get; }
    }

    internal sealed class RecoverySettings
    {
        public RecoverySettings(Config config)
        {
            this.LocalReadTimeout = config.GetTimeSpan("eventuate.log.read-timeout");
            this.LocalWriteTimeout = config.GetTimeSpan("eventuate.log.write-timeout");
            this.RemoteOperationRetryMax = config.GetInt("eventuate.log.recovery.remote-operation-retry-max");
            this.RemoteOperationRetryDelay = config.GetTimeSpan("eventuate.log.recovery.remote-operation-retry-delay");
            this.RemoteOperationTimeout = config.GetTimeSpan("eventuate.log.recovery.remote-operation-timeout");
            this.SnapshotDeletionTimeout = config.GetTimeSpan("eventuate.log.recovery.snapshot-deletion-timeout");
        }

        public TimeSpan LocalReadTimeout { get; }
        public TimeSpan LocalWriteTimeout { get; }
        public int RemoteOperationRetryMax { get; }
        public TimeSpan RemoteOperationRetryDelay { get; }
        public TimeSpan RemoteOperationTimeout { get; }
        public TimeSpan SnapshotDeletionTimeout { get; }
    }

    /// <summary>
    /// Represents a link between a local and remote event log that are subject to disaster recovery.
    /// </summary>
    internal sealed class RecoveryLink
    {
        public RecoveryLink(ReplicationLink replicationLink, long localSequenceNr, long remoteSequenceNr)
        {
            ReplicationLink = replicationLink;
            LocalSequenceNr = localSequenceNr;
            RemoteSequenceNr = remoteSequenceNr;
        }

        /// <summary>
        /// Used to recover events (through replication)
        /// </summary>
        public ReplicationLink ReplicationLink { get; }

        /// <summary>
        /// Sequence number of the local event log at the beginning of disaster recovery.
        /// </summary>
        public long LocalSequenceNr { get; }

        /// <summary>
        /// Current sequence nr of the remote log.
        /// </summary>
        public long RemoteSequenceNr { get; }
    }

    /// <summary>
    /// Provides disaster recovery primitives.
    /// </summary>
    /// <seealso cref="ReplicationEndpoint.Recover"/>
    internal sealed class Recovery
    {
        private readonly RecoverySettings settings;

        public Recovery(ReplicationEndpoint endpoint)
        {
            this.Endpoint = endpoint;
            this.settings = new RecoverySettings(endpoint.System.Settings.Config);
        }

        /// <summary>
        /// Endpoint to be recovered.
        /// </summary>
        public ReplicationEndpoint Endpoint { get; }

        /// <summary>
        /// Read <see cref="ReplicationEndpointInfo"/> from local <see cref="ReplicationEndpoint"/>
        /// </summary>
        /// <returns></returns>
        public async Task<ReplicationEndpointInfo> ReadEndpointInfo()
        {
            var seqNrs = await ReadLogSequenceNrs();
            return new ReplicationEndpointInfo(Endpoint.Id, seqNrs);
        }

        public async Task<ImmutableDictionary<string, long>> ReadLogSequenceNrs()
        {
            var clocks = await ReadEventLogClocks();
            var builder = ImmutableDictionary.CreateBuilder<string, long>();
            foreach (var (key, clock) in clocks)
            {
                builder[key] = clock.SequenceNr;
            }
            return builder.ToImmutable();
        }

        /// <summary>
        /// Reads the clocks from local event logs.
        /// </summary>
        public async Task<ImmutableDictionary<string, EventLogClock>> ReadEventLogClocks()
        {
            var requests = Endpoint.LogNames
                .Select(async name => new KeyValuePair<string, EventLogClock>(name, await ReadEventLogClock(Endpoint.Logs[name])))
                .ToArray();

            var entries = await Task.WhenAll(requests);
            return entries.ToImmutableDictionary();
        }

        /// <summary>
        /// Synchronize sequence numbers of local logs with replication progress stored in remote replicas.
        /// </summary>
        /// <returns>A set of <see cref="RecoveryLink"/>s indicating the events that need to be recovered</returns>
        public async Task<ImmutableHashSet<RecoveryLink>> SynchronizeReplicationProgressesWithRemote(ReplicationEndpointInfo info)
        {
            var results = Endpoint.connectors
                .Select(async connector => {
                    var remoteInfo = await SynchronizeReplicationProgressWithRemote(connector.RemoteAcceptor, info);
                    var links = connector.Links(remoteInfo);
                    return links.Select(link => ToRecoveryLink(link, info, remoteInfo));
                })
                .ToArray();

            return (await Task.WhenAll(results)).SelectMany(x => x).ToImmutableHashSet();
        }

        private RecoveryLink ToRecoveryLink(ReplicationLink link, ReplicationEndpointInfo localInfo, ReplicationEndpointInfo remoteInfo) =>
            new RecoveryLink(link, localInfo.LogSequenceNumbers[link.Target.LogName], remoteInfo.LogSequenceNumbers[link.Target.LogName]);

        private async Task<ReplicationEndpointInfo> SynchronizeReplicationProgressWithRemote(ActorSelection remoteAcceptor, ReplicationEndpointInfo info)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Update the locally stored replication progress of remote replicas with the sequence numbers given in <paramref name="info"/>.
        /// Replication progress that is greater than the corresponding sequence number in <paramref name="info"/> is reset to that
        /// </summary>
        public async Task SynchronizeReplicationProgress(ReplicationEndpointInfo info)
        {
            throw new NotImplementedException();
        }

        private Task<long> ReadReplicationProgress(IActorRef logActor, string logId)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Sets the replication progress for the remote replicate with id `logId` to `replicationProgress`
        /// and clears the cached version vector.
        /// </summary>
        private Task<long> UpdateReplicationMetadata(IActorRef logActor, string logId, long replicationProgress)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Returns `true`, if the source of the <see cref="RecoveryLink"/> did not receive all events before the disaster, i.e.
        /// the initial replication from the location to be recovered to the source of event recovery was filtered.
        /// </summary>
        public bool IsFilteredLink(RecoveryLink link) => Endpoint.EndpointFilters.FilterFor(link.ReplicationLink.Source.LogId, link.ReplicationLink.Target.LogName) != NoFilter.Instance;

        /// <summary>
        /// Initiates event recovery for the given <see cref="ReplicationLink"/>s. The returned task completes when
        /// all events are successfully recovered.
        /// </summary>
        public Task RecoverLinks(ImmutableHashSet<RecoveryLink> recoveryLinks)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Deletes all invalid snapshots from local event logs. A snapshot is invalid if it covers
        /// events that have been lost.
        /// </summary>
        public Task DeleteSnapshots(ImmutableHashSet<RecoveryLink> links)
        {
            throw new NotImplementedException();
        }

        public async Task<EventLogClock> ReadEventLogClock(IActorRef targetLog)
        {
            var response = await targetLog.Ask<GetEventLogClockSuccess>(new GetEventLogClock(), timeout: settings.LocalReadTimeout);
            return response.Clock;
        }

        private Task DeleteSnapshots(RecoveryLink link)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// In case disaster recovery was not able to recover all events (e.g. only through a single filtered connection)
        /// the local sequence no must be adjusted to the log's version vector to avoid events being
        /// written in the causal past.
        /// </summary>
        public async Task AdjustEventLogClocks(IActorRef log)
        {
            var response = await log.Ask(new AdjustEventLogClock(), timeout: settings.RemoteOperationTimeout);
            if (response is AdjustEventLogClockFailure f)
                throw f.Cause;
        }
    }

    /**
     * [[ReplicationEndpoint]]-scoped singleton that receives all requests from remote endpoints. These are
     *
     *  - [[GetReplicationEndpointInfo]] requests.
     *  - [[ReplicationRead]] requests (inside [[ReplicationReadEnvelope]]s).
     *
     * This actor is also involved in disaster recovery and implements a state machine with the following
     * possible transitions:
     *
     *  - `initializing` -> `recovering` -> `processing` (when calling `endpoint.recover()`)
     *  - `initializing` -> `processing`                 (when calling `endpoint.activate()`)
     */
    internal sealed class Acceptor : ActorBase
    {
        protected override bool Receive(object message)
        {
            throw new NotImplementedException();
        }
    }

    /**
     * If disaster recovery is initiated events are recovered until
     * a [[ReplicationWriteSuccess]] sent as notification from the local [[Replicator]] is received indicating that all
     * events, known to exist remotely at the beginning of recovery, are replicated.
     *
     * When all replication links have been processed this actor
     * notifies [[Acceptor]] (= parent) that recovery completed and ends itself.
     */
    internal class RecoveryManager : ActorBase
    {
        protected override bool Receive(object message)
        {
            throw new NotImplementedException();
        }
    }
}
