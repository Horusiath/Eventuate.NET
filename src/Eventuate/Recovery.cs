#region copyright
// -----------------------------------------------------------------------
//  <copyright file="Recovery.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using Akka.Actor;
using Akka.Configuration;
using Akka.Event;
using Eventuate.EventLogs;
using Eventuate.EventsourcingProtocol;
using Eventuate.ReplicationProtocol;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Runtime.CompilerServices;
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
                .Select(async connector =>
                {
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
            var i = 0;
            while (true)
            {
                try
                {
                    i++;
                    var result = await remoteAcceptor.Ask(new SynchronizeReplicationProgress(info), timeout: settings.RemoteOperationTimeout);
                    switch (result)
                    {
                        case SynchronizeReplicationProgressSuccess s: return s.Info;
                        case SynchronizeReplicationProgressFailure f: throw f.Cause;
                        default: throw new InvalidOperationException($"Expected either [{nameof(SynchronizeReplicationProgressSuccess)}] or [{nameof(SynchronizeReplicationProgressFailure)}] but got [{result.GetType().FullName}]");
                    }
                }
                catch (AskTimeoutException) when (i <= settings.RemoteOperationRetryMax)
                {
                    await Task.Delay(settings.RemoteOperationRetryDelay);
                }
            }
        }

        /// <summary>
        /// Update the locally stored replication progress of remote replicas with the sequence numbers given in <paramref name="info"/>.
        /// Replication progress that is greater than the corresponding sequence number in <paramref name="info"/> is reset to that
        /// </summary>
        public Task SynchronizeReplicationProgress(ReplicationEndpointInfo info)
        {
            ImmutableHashSet<string> logNames = Endpoint.CommonLogNames(info);
            var tasks = logNames.Select(name => SynchronizeReplicationProgress(info, name));
            return Task.WhenAll(tasks);
        }

        private async Task<long> SynchronizeReplicationProgress(ReplicationEndpointInfo info, string name)
        {
            var logActor = Endpoint.Logs[name];
            var logId = info.LogId(name);
            var remoteSequenceNr = info.LogSequenceNumbers[name];
            var currentProgress = await ReadReplicationProgress(logActor, logId);
            if (currentProgress > remoteSequenceNr)
                return await UpdateReplicationMetadata(logActor, logId, remoteSequenceNr);
            else return currentProgress;
        }

        private async Task<long> ReadReplicationProgress(IActorRef logActor, string logId)
        {
            var result = await logActor.Ask(new GetReplicationProgress(logId), timeout: settings.LocalReadTimeout)
                .Unwrap<GetReplicationProgressSuccess, Exception>();
            return result.StoredReplicationProgress;
        }

        /// <summary>
        /// Sets the replication progress for the remote replicate with id `logId` to `replicationProgress`
        /// and clears the cached version vector.
        /// </summary>
        private async Task<long> UpdateReplicationMetadata(IActorRef logActor, string logId, long replicationProgress)
        {
            var metadata = new ReplicationMetadata(replicationProgress, VectorTime.Zero);
            var result = await logActor.Ask(new ReplicationWrite(Array.Empty<DurableEvent>(), ImmutableDictionary<string, ReplicationMetadata>.Empty.Add(logId, metadata)), timeout: settings.LocalWriteTimeout)
                .Unwrap<ReplicationWriteSuccess, Exception>();
            return replicationProgress;
        }

        /// <summary>
        /// Returns `true`, if the source of the <see cref="RecoveryLink"/> did not receive all events before the disaster, i.e.
        /// the initial replication from the location to be recovered to the source of event recovery was filtered.
        /// </summary>
        public bool IsFilteredLink(RecoveryLink link) =>
            Endpoint.EndpointFilters.FilterFor(link.ReplicationLink.Source.LogId, link.ReplicationLink.Target.LogName) != NoFilter.Instance;

        /// <summary>
        /// Initiates event recovery for the given <see cref="ReplicationLink"/>s. The returned task completes when
        /// all events are successfully recovered.
        /// </summary>
        public async Task RecoverLinks(ImmutableHashSet<RecoveryLink> recoveryLinks)
        {
            if (!recoveryLinks.IsEmpty)
            {
                await DeleteSnapshots(recoveryLinks);
                var promise = new TaskCompletionSource<Void>();
                Endpoint.Acceptor.Tell(new Acceptor.Recover(recoveryLinks, promise));
                await promise.Task;
            }
        }

        /// <summary>
        /// Deletes all invalid snapshots from local event logs. A snapshot is invalid if it covers
        /// events that have been lost.
        /// </summary>
        public Task DeleteSnapshots(ImmutableHashSet<RecoveryLink> links)
        {
            var tasks = links.Select(DeleteSnapshots);
            return Task.WhenAll(tasks);
        }

        public async Task<EventLogClock> ReadEventLogClock(IActorRef targetLog)
        {
            var response = await targetLog.Ask<GetEventLogClockSuccess>(new GetEventLogClock(), timeout: settings.LocalReadTimeout);
            return response.Clock;
        }

        private Task DeleteSnapshots(RecoveryLink link)
        {
            return Endpoint.Logs[link.ReplicationLink.Target.LogName]
                .Ask(new DeleteSnapshots(link.LocalSequenceNr + 1), timeout: settings.SnapshotDeletionTimeout)
                .Unwrap<DeleteSnapshotsSuccess, Exception>();
        }

        /// <summary>
        /// In case disaster recovery was not able to recover all events (e.g. only through a single filtered connection)
        /// the local sequence no must be adjusted to the log's version vector to avoid events being
        /// written in the causal past.
        /// </summary>
        public Task AdjustEventLogClocks() =>
            Task.WhenAll(Endpoint.Logs.Values.Select(this.AdjustEventLogClock));

        private Task AdjustEventLogClock(IActorRef log) =>
            log.Ask(new AdjustEventLogClock(), timeout: settings.RemoteOperationTimeout)
               .Unwrap<AdjustEventLogClockSuccess, Exception>();
    }

    /// <summary>
    /// <see cref="ReplicationEndpoint"/>-scoped singleton that receives all requests from remote endpoints. These are
    /// 
    ///  1. <see cref="GetReplicationEndpointInfo"/> requests.
    ///  2. <see cref="ReplicationRead"/> requests (inside <see cref="ReplicationReadEnvelope"/>s).
    /// 
    /// This actor is also involved in disaster recovery and implements a state machine with the following
    /// possible transitions:
    /// 
    ///  - `initializing` -> `recovering` -> `processing` (when calling `endpoint.recover()`)
    ///  - `initializing` -> `processing`                 (when calling `endpoint.activate()`)
    /// </summary>
    internal sealed class Acceptor : ActorBase
    {
        #region internal messages

        public readonly struct Process { }
        public readonly struct Recover
        {
            public Recover(ImmutableHashSet<RecoveryLink> links, TaskCompletionSource<Void> promise)
            {
                Links = links;
                Promise = promise;
            }

            public ImmutableHashSet<RecoveryLink> Links { get; }
            public TaskCompletionSource<Void> Promise { get; }
        }
        public readonly struct RecoveryCompleted { }
        public readonly struct RecoveryStepCompleted
        {
            public RecoveryStepCompleted(RecoveryLink link)
            {
                Link = link;
            }

            public RecoveryLink Link { get; }
        }
        public readonly struct MetadataRecoveryCompleted { }
        public readonly struct EventRecoveryCompleted { }

        #endregion

        public const string Name = "acceptor";

        private readonly ReplicationEndpoint endpoint;
        private readonly Recovery recovery;

        public Acceptor(ReplicationEndpoint endpoint)
        {
            this.endpoint = endpoint;
            this.recovery = new Recovery(endpoint);
        }

        protected override void Unhandled(object message)
        {
            switch (message)
            {
                case GetReplicationEndpointInfo _:
                    this.recovery.ReadEndpointInfo().PipeTo(Sender, success: info => new GetReplicationEndpointInfoSuccess(info));
                    break;
                case SynchronizeReplicationProgress sync:
                    var remoteInfo = sync.Info;
                    Task.Run<object>(async () =>
                    {
                        try
                        {
                            await this.recovery.SynchronizeReplicationProgress(remoteInfo);
                            var localInfo = await this.recovery.ReadEndpointInfo();
                            return new SynchronizeReplicationProgressSuccess(localInfo);
                        }
                        catch (Exception cause)
                        {
                            return new SynchronizeReplicationProgressFailure(new SynchronizeReplicationProgressSourceException(cause.Message));
                        }
                    }).PipeTo(Sender);
                    break;
                default: base.Unhandled(message); break;
            }            
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected override bool Receive(object message) => Initializing(message);

        private bool Initializing(object message)
        {
            if (message is Process)
            {
                Context.Become(Processing);
                return true;
            }
            else return false;
        }

        private bool Recovering(object message)
        {
            switch (message)
            {
                case Recover recover:
                    foreach (var connector in this.endpoint.connectors)
                    {
                        connector.Activate(recover.Links.Select(l => l.ReplicationLink).ToImmutableHashSet());
                    }
                    var recoveryManager = Context.ActorOf(Props.Create(() => new RecoveryManager(this.endpoint.Id, recover.Links)));
                    Context.Become(RecoveringEvents(recoveryManager, recover.Promise));
                    return true;

                case RecoveryCompleted _:
                    Context.Become(Processing);
                    return true;

                default: return false;
            }
        }

        private Receive RecoveringEvents(IActorRef recoveryManager, TaskCompletionSource<Void> promise) => (object message) =>
        {
            switch (message)
            {
                case ReplicationWriteSuccess _:
                    recoveryManager.Forward(message);
                    return true;

                case EventRecoveryCompleted _:
                    promise.TrySetResult(default);
                    Context.Become(msg => Recovering(msg) || Processing(msg));
                    return true;

                default: return Processing(message);
            }
        };

        private bool Processing(object message)
        {
            switch (message)
            {
                case ReplicationReadEnvelope envelope:
                    if (envelope.IncompatibleWith(endpoint.ApplicationName, endpoint.ApplicationVersion))
                    {
                        Sender.Tell(new ReplicationReadFailure(new IncompatibleApplicationVersionException(endpoint.Id, endpoint.ApplicationVersion, envelope.TargetApplicationVersion), envelope.Payload.TargetLogId));
                    }
                    else
                    {
                        var r = envelope.Payload;
                        var r2 = new ReplicationRead(r.FromSequenceNr, r.Max, r.ScanLimit, endpoint.EndpointFilters.FilterFor(r.TargetLogId, envelope.LogName).And(r.Filter), r.TargetLogId, r.Replicator, r.CurrentTargetVersionVector);
                        endpoint.Logs[envelope.LogName].Forward(r2);
                    }
                    return true;

                case ReplicationWriteSuccess _: return true;
                default: return false;
            }
        }
    }

    /// <summary>
    /// If disaster recovery is initiated events are recovered until
    /// a <see cref="ReplicationWriteSuccess"/> sent as notification from the local <see cref="Replicator"/> is received indicating that all
    /// events, known to exist remotely at the beginning of recovery, are replicated.
    /// 
    /// When all replication links have been processed this actor
    /// notifies <see cref="Acceptor"/> (= parent) that recovery completed and ends itself.
    /// </summary>
    internal sealed class RecoveryManager : ActorBase
    {
        private readonly ILoggingAdapter log = Context.GetLogger();
        private readonly string endpointId;
        private readonly ImmutableHashSet<RecoveryLink> links;

        public RecoveryManager(string endpointId, ImmutableHashSet<RecoveryLink> links)
        {
            this.endpointId = endpointId;
            this.links = links;
            Context.Become(RecoveringEvents(links));
        }

        protected override bool Receive(object message) => throw new NotImplementedException();

        private Receive RecoveringEvents(ImmutableHashSet<RecoveryLink> active) => message =>
        {
            if (message is ReplicationWriteSuccess writeSuccess && active.Any(link => writeSuccess.Metadata.ContainsKey(link.ReplicationLink.Source.LogId)))
            {
                foreach (var link in active)
                {
                    if (RecoveryForLinkFinished(link, writeSuccess))
                    {
                        var updatedActive = RemoveLink(active, link);
                        if (updatedActive.IsEmpty)
                        {
                            Context.Parent.Tell(new Acceptor.EventRecoveryCompleted());
                            Self.Tell(PoisonPill.Instance);
                        }
                        else
                        {
                            Context.Become(RecoveringEvents(updatedActive));
                        }

                        return true;
                    }
                }
                return true;
            }
            else return false;
        };

        private bool RecoveryForLinkFinished(RecoveryLink link, ReplicationWriteSuccess writeSuccess)
        {
            if (writeSuccess.Metadata.TryGetValue(link.ReplicationLink.Source.LogId, out var metadata))
            {
                return link.RemoteSequenceNr <= metadata.ReplicationProgress;
            }
            else return false;
        }

        private ImmutableHashSet<RecoveryLink> RemoveLink(ImmutableHashSet<RecoveryLink> active, RecoveryLink link)
        {
            var updated = active.Remove(link);
            var all = links.Count;
            var finished = all - updated.Count;
            log.Info("[recovery of {0}] Event recovery finished for remote log {1} ({2} of {3})", endpointId, link.ReplicationLink.Source.LogId, finished, all);
            return updated;
        }
    }
}
