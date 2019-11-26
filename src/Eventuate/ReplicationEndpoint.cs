#region copyright
// -----------------------------------------------------------------------
//  <copyright file="ReplicationEndpoint.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using Akka.Actor;
using Akka.Configuration;
using Akka.Event;
using Akka.Util;
using Eventuate.EventsourcingProtocol;
using Eventuate.ReplicationProtocol;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using static Eventuate.ReplicationEndpoint;

namespace Eventuate
{
    public sealed class ReplicationSettings
    {
        public ReplicationSettings(Config config)
        {
            this.WriteBatchSize = config.GetInt("eventuate.log.write-batch-size");
            this.WriteTimeout = config.GetTimeSpan("eventuate.log.write-timeout");
            this.ReadTimeout = config.GetTimeSpan("eventuate.log.read-timeout");
            this.RemoteReadTimeout = config.GetTimeSpan("eventuate.log.replication.remote-read-timeout");
            this.RemoteScanLimit = config.GetInt("eventuate.log.replication.remote-scan-limit");
            this.RetryDelay = config.GetTimeSpan("eventuate.log.replication.retry-delay");
            this.FailureDetectionLimit = config.GetTimeSpan("eventuate.log.replication.failure-detection-limit");

            if (FailureDetectionLimit < RemoteReadTimeout + RetryDelay)
                throw new ConfigurationException($"eventuate.log.replication.failure-detection-limit ({FailureDetectionLimit}) must be at least the sum of eventuate.log.replication.retry - delay({RetryDelay}) and eventuate.log.replication.remote - read - timeout({RemoteReadTimeout})");
        }

        public int WriteBatchSize { get; }
        public TimeSpan WriteTimeout { get; }
        public TimeSpan ReadTimeout { get; }
        public TimeSpan RemoteReadTimeout { get; }
        public int RemoteScanLimit { get; }
        public TimeSpan RetryDelay { get; }
        public TimeSpan FailureDetectionLimit { get; }
    }

    /// <summary>
    /// A replication endpoint connects to other replication endpoints for replicating events. Events are
    /// replicated from the connected endpoints to this endpoint. The connected endpoints are ''replication
    /// sources'', this endpoint is a ''replication target''. To setup bi-directional replication, the other
    /// replication endpoints must additionally setup replication connections to this endpoint.
    /// 
    /// A replication endpoint manages one or more event logs. Event logs are indexed by name. Events are
    /// replicated only between event logs with matching names.
    /// 
    /// If `applicationName` equals that of a replication source, events are only replicated if `applicationVersion`
    /// is greater than or equal to that of the replication source. This is a simple mechanism to support
    /// incremental version upgrades of replicated applications where each replica can be upgraded individually
    /// without shutting down other replicas. This avoids permanent state divergence during upgrade which may
    /// occur if events are replicated from replicas with higher version to those with lower version. If
    /// `applicationName` does not equal that of a replication source, events are always replicated, regardless
    /// of the `applicationVersion` value.
    /// </summary>
    public sealed class ReplicationEndpoint
    {
        #region internal types

        /// <summary>
        /// Published to the actor system's event stream if a remote log is available.
        /// </summary>
        public readonly struct Available : System.IEquatable<Available>
        {
            public Available(string endpointId, string logName)
            {
                EndpointId = endpointId;
                LogName = logName;
            }

            public string EndpointId { get; }
            public string LogName { get; }

            public bool Equals(Available other)
            {
                return Equals(EndpointId, other.EndpointId) && Equals(LogName, other.LogName);
            }

            public override bool Equals(object other)
            {
                return other is Available ? Equals((Available)other) : false;
            }

            public override int GetHashCode()
            {
                var hashCode = 17;
                hashCode = hashCode * 23 + (EndpointId?.GetHashCode() ?? 0);
                hashCode = hashCode * 23 + (LogName?.GetHashCode() ?? 0);
                return hashCode;
            }
            
            public override string ToString() =>
                $"Available(endpointId: '{EndpointId}', logName: '{LogName}')";
        }

        /// <summary>
        /// Published to the actor system's event stream if a remote log is unavailable.
        /// </summary>
        public readonly struct Unavailable : System.IEquatable<Unavailable>
        {
            public Unavailable(string endpointId, string logName, IEnumerable<Exception> causes)
            {
                EndpointId = endpointId;
                LogName = logName;
                Causes = causes;
            }

            public string EndpointId { get; }
            public string LogName { get; }
            public IEnumerable<Exception> Causes { get; }

            public bool Equals(Unavailable other) => Equals(EndpointId, other.EndpointId) && Equals(LogName, other.LogName) && Causes.SequenceEqual(other.Causes);

            public override bool Equals(object other) => other is Unavailable ? Equals((Unavailable)other) : false;

            public override int GetHashCode()
            {
                var hashCode = 17;
                hashCode = hashCode * 23 + (EndpointId?.GetHashCode() ?? 0);
                hashCode = hashCode * 23 + (LogName?.GetHashCode() ?? 0);
                foreach (var cause in Causes)
                {
                    hashCode = (hashCode * 23) ^ cause.GetHashCode();
                }
                return hashCode;
            }

            public override string ToString() =>
                $"Unavailable(endpointId: '{EndpointId}', logName: '{LogName}', causes: [{string.Join(", ", Causes)}])";
        }

        #endregion

        /// <summary>
        /// Default log name.
        /// </summary>
        public const string DefaultLogName = "default";

        /// <summary>
        /// Default application name.
        /// </summary>
        public const string DefaultApplicationName = "default";

        /// <summary>
        /// Default application version.
        /// </summary>
        public static readonly ApplicationVersion DefaultApplicationVersion = new ApplicationVersion(1, 0);

        private readonly AtomicBoolean isActive = new AtomicBoolean(false);
        internal readonly ImmutableHashSet<SourceConnector> connectors;

        // lazy to make sure concurrently running (created actors) do not access null-reference
        // https://github.com/RBMHTechnology/eventuate/issues/183
        private readonly Lazy<IActorRef> acceptor;

        public IActorRef Acceptor => acceptor.Value;

        /// <summary>
        /// The actor system's replication settings.
        /// </summary>
        public ReplicationSettings Settings { get; }

        /// <summary>
        /// The log actors managed by this endpoint, indexed by their name.
        /// </summary>
        public ImmutableDictionary<string, IActorRef> Logs { get; }

        public ReplicationEndpoint(
            ActorSystem system,
            string id,
            ImmutableHashSet<string> logNames,
            Func<string, Props> logFactory,
            ImmutableHashSet<ReplicationConnection> connections,
            IEndpointFilter endpointFilters = null,
            string applicationName = null,
            ApplicationVersion? applicationVersion = null)
        {
            System = system;
            Id = id;
            LogNames = logNames;
            LogFactory = logFactory;
            Connections = connections;
            EndpointFilters = endpointFilters ?? NoFilters.Instance;
            ApplicationName = applicationName ?? DefaultApplicationName;
            ApplicationVersion = applicationVersion ?? DefaultApplicationVersion;
            Settings = new ReplicationSettings(system.Settings.Config);

            var logs = ImmutableDictionary.CreateBuilder<string, IActorRef>();
            foreach (var logName in logNames)
            {
                var logId = LogId(logName);
                logs[logName] = system.ActorOf(LogFactory(logId), logId);
            }
            Logs = logs.ToImmutable();

            var connectors = ImmutableHashSet.CreateBuilder<SourceConnector>();
            foreach (var connection in connections)
            {
                connectors.Add(new SourceConnector(this, connection));
            }
            this.connectors = connectors.ToImmutable();
            this.acceptor = new Lazy<IActorRef>(() => system.ActorOf(Props.Create(() => new Acceptor(this)), Eventuate.Acceptor.Name));
            var started = this.acceptor.Value;
        }

        public ActorSystem System { get; }

        /// <summary>
        /// Unique replication endpoint id.
        /// </summary>
        public string Id { get; }

        /// <summary>
        /// Names of the event logs managed by this replication endpoint.
        /// </summary>
        public ImmutableHashSet<string> LogNames { get; }

        /// <summary>
        /// Factory of log actor <see cref="Props"/>. The `String` parameter of the factory is a unique
        /// log id generated by this endpoint. The log actor must be assigned this log id.
        /// </summary>
        public Func<string, Props> LogFactory { get; }

        /// <summary>
        /// Replication connections to other replication endpoints.
        /// </summary>
        public ImmutableHashSet<ReplicationConnection> Connections { get; }

        /// <summary>
        /// Replication filters applied to incoming replication read requests.
        /// </summary>
        public IEndpointFilter EndpointFilters { get; }

        /// <summary>
        /// Name of the application that creates this replication endpoint.
        /// </summary>
        public string ApplicationName { get; }

        /// <summary>
        /// Version of the application that creates this replication endpoint.
        /// </summary>
        public ApplicationVersion ApplicationVersion { get; }

        /// <summary>
        /// Returns the unique log id for given <paramref name="logName"/>.
        /// </summary>
        public string LogId(string logName) => ReplicationEndpointInfo.LogId(Id, logName);

        /// <summary>
        /// Runs an asynchronous disaster recovery procedure. This procedure recovers this endpoint in case of total or
        /// partial event loss. Partial event loss means event loss from a given sequence number upwards (for example,
        /// after having installed a storage backup). Recovery copies events from directly connected remote endpoints back
        /// to this endpoint and automatically removes invalid snapshots. A snapshot is invalid if it covers events that
        /// have been lost.
        /// 
        /// This procedure requires that event replication between this and directly connected endpoints is bi-directional
        /// and that these endpoints are available during recovery. After successful recovery the endpoint is automatically
        /// activated. A failed recovery completes with a <see cref="RecoveryException"/> and must be retried. Activating this endpoint
        /// without having successfully recovered from partial or total event loss may result in inconsistent replica states.
        /// 
        /// Running a recovery on an endpoint that didn't loose events has no effect but may still fail due to unavailable
        /// replication partners, for example. In this case, a recovery retry can be omitted if the `partialUpdate` field
        /// of <see cref="RecoveryException"/> is set to `false`.
        /// </summary>
        public async Task Recover()
        {
            if (Connections.IsEmpty)
                throw new InvalidOperationException("Recover an endpoint without connections");

            if (!isActive.CompareAndSet(false, true))
                throw new InvalidOperationException("Recovery running or endpoint already activated");

            var recovery = new Recovery(this);
            var partialUpdate = false;
            try
            {
                // Disaster recovery is executed in 3 steps:
                // 1. synchronize metadata to
                //    - reset replication progress of remote sites and
                //    - determine after disaster progress of remote sites
                // 2. Recover events from unfiltered links
                // 3. Recover events from filtered links
                // 4. Adjust the sequence numbers of local logs to their version vectors
                // unfiltered links are recovered first to ensure that no events are recovered from a filtered connection
                // where the causal predecessor is not yet recovered (from an unfiltered connection)
                // as causal predecessors cannot be written after their successors to the event log.
                // The sequence number of an event log needs to be adjusted if not all events could be
                // recovered as otherwise it could be less then the corresponding entriy in the
                // log's version vector

                var localEndpointInfo = await recovery.ReadEndpointInfo();
                LogLocalState(localEndpointInfo);
                var recoveryLinks = await recovery.SynchronizeReplicationProgressesWithRemote(localEndpointInfo);

                partialUpdate = true;
                var filteredBuilder = ImmutableHashSet.CreateBuilder<RecoveryLink>();
                var unfilteredBuilder = ImmutableHashSet.CreateBuilder<RecoveryLink>();
                foreach (var link in recoveryLinks)
                {
                    if (recovery.IsFilteredLink(link))
                        filteredBuilder.Add(link);
                    else
                        unfilteredBuilder.Add(link);
                }
                var unfilteredLinks = unfilteredBuilder.ToImmutable();
                var filteredLinks = filteredBuilder.ToImmutable();


                LogLinksToBeRecovered(unfilteredLinks, "unfiltered");
                await recovery.RecoverLinks(unfilteredLinks);
                LogLinksToBeRecovered(filteredLinks, "filtered");
                await recovery.RecoverLinks(filteredLinks);
                await recovery.AdjustEventLogClocks();

                Acceptor.Tell(new Acceptor.RecoveryCompleted());
            }
            catch (Exception cause)
            {
                throw new RecoveryException(cause, partialUpdate);
            }
        }

        private void LogLocalState(ReplicationEndpointInfo info)
        {
            if (System.Log.IsInfoEnabled)
            {
                System.Log.Info("Disaster recovery initiated for endpoint {0}. Sequence numbers of local logs are: {1}", info.EndpointId, SequenceNumbersLogString(info));
                System.Log.Info("Need to reset replication progress stored at remote replicas {0}", info.EndpointId, string.Join(", ", connectors.Select(c => c.RemoteAcceptor.ToString())));
            }
        }

        private void LogLinksToBeRecovered(ImmutableHashSet<RecoveryLink> links, string linkType)
        {
            if (System.Log.IsInfoEnabled)
            {
                var sb = new StringBuilder();
                foreach (var l in links)
                {
                    sb.AppendFormat("({0} ({1} -> {2} ({3}))", l.ReplicationLink.Source.LogId, l.RemoteSequenceNr.ToString(), l.ReplicationLink.Target.LogName, l.LocalSequenceNr.ToString()).Append(", ");
                }

                System.Log.Info("Start recovery for {0} links: (from remote source log (target seq no) -> local target log (initial seq no))\n{1}", linkType, sb.ToString());
            }
        }

        private string SequenceNumbersLogString(ReplicationEndpointInfo info)
        {
            var sb = new StringBuilder();
            foreach (var (logName, sequenceNr) in info.LogSequenceNumbers)
            {
                sb.Append(logName).Append(':').Append(sequenceNr).Append(", ");
            }
            return sb.ToString();
        }

        /// <summary>
        /// Delete events from a local log identified by <paramref name="logName"/> with a sequence number less than or equal to
        /// <paramref name="toSequenceNr"/>. Deletion is split into logical deletion and physical deletion. Logical deletion is
        /// supported by any storage backend and ensures that deleted events are not replayed any more. It has
        /// immediate effect. Logically deleted events can still be replicated to remote <see cref="ReplicationEndpoint"/>s.
        /// They are only physically deleted if the storage backend supports that (currently LevelDB only). Furthermore,
        /// physical deletion only starts after all remote replication endpoints identified by <paramref name="remoteEndpointIds"/>
        /// have successfully replicated these events. Physical deletion is implemented as reliable background
        /// process that survives event log restarts.
        /// 
        /// Use with care! When events are physically deleted they cannot be replicated any more to new replication
        /// endpoints (i.e. those that were unknown at the time of deletion). Also, a location with deleted events
        /// may not be suitable any more for disaster recovery of other locations.
        /// </summary>
        /// <param name="logName">Events are deleted from the local log with this name.</param>
        /// <param name="toSequenceNr">Sequence number up to which events shall be deleted (inclusive).</param>
        /// <param name="remoteEndpointIds">
        /// A set of remote <see cref="ReplicationEndpoint"/> ids that must have replicated events
        /// to their logs before they are allowed to be physically deleted at this endpoint.
        /// </param>
        /// <returns>
        /// The sequence number up to which events have been logically deleted. When the returned task
        /// completes logical deletion is effective. The returned sequence number can differ from the requested
        /// one, if:
        /// 
        /// - the log's current sequence number is smaller than the requested number. In this case the current
        ///  sequence number is returned.
        /// - there was a previous successful deletion request with a higher sequence number. In this case that
        ///  number is returned.
        /// </returns>
        public async Task<long> Delete(string logName, long toSequenceNr, ImmutableHashSet<string> remoteEndpointIds)
        {
            var remoteLogIds = remoteEndpointIds
                .Select(id => ReplicationEndpointInfo.LogId(id, logName))
                .ToImmutableHashSet();

            var response = await Logs[logName].Ask(new Delete(toSequenceNr, remoteEndpointIds), timeout: Settings.WriteTimeout);
            switch (response)
            {
                case DeleteSuccess s: return s.DeletedTo;
                case DeleteFailure f: throw f.Cause;
                default: throw new InvalidOperationException($"Expected either [{nameof(DeleteSuccess)}] or [{nameof(DeleteFailure)}] but got [{response.GetType().FullName}]");
            }
        }

        /// <summary>
        /// Activates this endpoint by starting event replication from remote endpoints to this endpoint.
        /// </summary>
        public void Activate()
        {
            if (isActive.CompareAndSet(false, true))
            {
                Acceptor.Tell(new Acceptor.Process());
                foreach (var connector in connectors)
                {
                    connector.Activate(replicationLinks: null);
                }
            }
            else throw new InvalidOperationException("Recovery running or endpoint already activated");
        }

        /// <summary>
        /// Creates <see cref="ReplicationTarget"/> for given <paramref name="logName"/>.
        /// </summary>
        public ReplicationTarget Target(string logName) => new ReplicationTarget(this, logName, LogId(logName), Logs[logName]);

        /// <summary>
        /// Returns all log names this endpoint and <paramref name="endpointInfo"/> have in common.
        /// </summary>
        internal ImmutableHashSet<string> CommonLogNames(ReplicationEndpointInfo endpointInfo) =>
            this.LogNames.Intersect(endpointInfo.LogNames);
    }

    /// <summary>
    /// <see cref="IEndpointFilter"/> computes a <see cref="ReplicationFilter"/> that shall be applied to a
    /// replication read request that replicates from a source log (defined by ``sourceLogName``)
    /// to a target log (defined by ``targetLogId``).
    /// </summary>
    public interface IEndpointFilter
    {
        ReplicationFilter FilterFor(string targetLogId, string sourceLogName);
    }

    public sealed class NoFilters : IEndpointFilter
    {
        public static IEndpointFilter Instance { get; } = new NoFilters();
        private NoFilters() { }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReplicationFilter FilterFor(string targetLogId, string sourceLogName) => NoFilter.Instance;
    }

    public static class EndpointFilters
    {
        private sealed class CombiningEndpointFilters : IEndpointFilter
        {
            private readonly ImmutableDictionary<string, ReplicationFilter> targetFilters;
            private readonly ImmutableDictionary<string, ReplicationFilter> sourceFilters;
            private readonly Func<ReplicationFilter, ReplicationFilter, ReplicationFilter> targetSourceCombinator;

            public CombiningEndpointFilters(
                ImmutableDictionary<string, ReplicationFilter> targetFilters,
                ImmutableDictionary<string, ReplicationFilter> sourceFilters,
                Func<ReplicationFilter, ReplicationFilter, ReplicationFilter> targetSourceCombinator)
            {
                this.targetFilters = targetFilters;
                this.sourceFilters = sourceFilters;
                this.targetSourceCombinator = targetSourceCombinator;
            }

            public ReplicationFilter FilterFor(string targetLogId, string sourceLogName)
            {
                if (targetFilters.TryGetValue(targetLogId, out var targetFilter))
                {
                    if (sourceFilters.TryGetValue(sourceLogName, out var sourceFilter))
                    {
                        return this.targetSourceCombinator(targetFilter, sourceFilter);
                    }
                    else return targetFilter;
                }
                else if (sourceFilters.TryGetValue(sourceLogName, out var sourceFilter)) return sourceFilter;
                else return NoFilter.Instance;
            }
        }

        private sealed class SimpleFilters : IEndpointFilter
        {
            private readonly ImmutableDictionary<string, ReplicationFilter> filters;
            private readonly bool pickTarget;

            public SimpleFilters(ImmutableDictionary<string, ReplicationFilter> filters, bool pickTarget)
            {
                this.filters = filters;
                this.pickTarget = pickTarget;
            }

            public ReplicationFilter FilterFor(string targetLogId, string sourceLogName) => filters.GetValueOrDefault(pickTarget ? targetLogId : sourceLogName, NoFilter.Instance);
        }

        /// <summary>
        /// An <see cref="IEndpointFilters"/> instance that always returns [[NoFilter]]
        /// independent from source/target logs of the replication read request.
        /// </summary>
        public static IEndpointFilter NoFilters = Eventuate.NoFilters.Instance;

        /// <summary>
        /// Creates an <see cref="IEndpointFilter"/> instance that computes a <see cref="ReplicationFilter"/> for a replication read request
        /// from a source log to a target log by and-combining target and source filters when given in the provided [[Map]]s.
        /// If only source or target filter is given that is returned. If no filter is given <see cref="NoFilters"/> is returned.
        /// A typical use case is that target specific filters are and-combined with a (default) source filter.
        /// </summary>
        /// <param name="targetFilters">Maps target log ids to the <see cref="ReplicationFilter"/> that shall be applied when replicating from a source log to this target log.</param>
        /// <param name="sourceFilters">Maps source log names to the <see cref="ReplicationFilter"/> that shall be applied when replicating from this source log to any target log.</param>
        public static IEndpointFilter TargetAndSourceFilters(ImmutableDictionary<string, ReplicationFilter> targetFilters, ImmutableDictionary<string, ReplicationFilter> sourceFilters) =>
            new CombiningEndpointFilters(targetFilters, sourceFilters, (target, source) => target.And(source));

        /// <summary>
        /// Creates an <see cref="IEndpointFilter"/> instance that computes a <see cref="ReplicationFilter"/> for a replication read request
        /// from a source log to a target log by returning a target filter when given in <paramref name="targetFilters"/>. If only
        /// a source filter is given in <paramref name="sourceFilters"/> that is returned otherwise <see cref="NoFilters"/> is returned.
        /// A typical use case is that (more privileged) remote targets may replace a (default) source filter with a target-specific filter.
        /// </summary>
        /// <param name="targetFilters">Maps target log ids to the <see cref="ReplicationFilter"/> that shall be applied when replicating from a source log to this target log.</param>
        /// <param name="sourceFilters">Maps source log names to the <see cref="ReplicationFilter"/> that shall be applied when replicating from this source log to any target log.</param>
        public static IEndpointFilter TargetOverridesSourceFilters(ImmutableDictionary<string, ReplicationFilter> targetFilters, ImmutableDictionary<string, ReplicationFilter> sourceFilters) =>
            new CombiningEndpointFilters(targetFilters, sourceFilters, (target, source) => target);

        /// <summary>
        /// Creates an <see cref="IEndpointFilter"/> instance that computes a <see cref="ReplicationFilter"/> for a replication read request
        /// from a source log to any target log by returning the source filter when given in <paramref name="sourceFilters"/> or
        /// <see cref="NoFilters"/> otherwise.
        /// </summary>
        /// <param name="sourceFilters">Maps source log names to the <see cref="ReplicationFilter"/> that shall be applied when replicating from this source log to any target log.</param>
        public static IEndpointFilter SourceFilters(ImmutableDictionary<string, ReplicationFilter> sourceFilters) =>
            new SimpleFilters(sourceFilters, pickTarget: false);

        /// <summary>
        /// Creates an <see cref="IEndpointFilter"/> instance that computes a <see cref="ReplicationFilter"/> for a replication read request
        /// to a target log by returning the target filter when given in <paramref name="targetFilters"/> or
        /// <see cref="NoFilters"/> otherwise.
        /// </summary>
        /// <param name="targetFilters">Maps target log ids to the <see cref="ReplicationFilter"/> that shall be applied when replicating from a source log to this target log.</param>
        public static IEndpointFilter TargetFilters(ImmutableDictionary<string, ReplicationFilter> targetFilters) =>
            new SimpleFilters(targetFilters, pickTarget: true);
    }

    /// <summary>
    /// References a remote event log at a source <see cref="ReplicationEndpoint"/>.
    /// </summary>
    public sealed class ReplicationSource
    {
        public ReplicationSource(string endpointId, string logName, string logId, ActorSelection acceptor)
        {
            EndpointId = endpointId;
            LogName = logName;
            LogId = logId;
            Acceptor = acceptor;
        }

        public string EndpointId { get; }
        public string LogName { get; }
        public string LogId { get; }
        public ActorSelection Acceptor { get; }
    }

    /// <summary>
    /// References a local event log at a target <see cref="ReplicationEndpoint"/>.
    /// </summary>
    public sealed class ReplicationTarget
    {
        public ReplicationTarget(ReplicationEndpoint endpoint, string logName, string logId, IActorRef log)
        {
            Endpoint = endpoint;
            LogName = logName;
            LogId = logId;
            Log = log;
        }

        public ReplicationEndpoint Endpoint { get; }
        public string LogName { get; }
        public string LogId { get; }
        public IActorRef Log { get; }
    }

    /// <summary>
    /// Represents an unidirectional replication link between a <see cref="Source"/> and a <see cref="Target"/>.
    /// </summary>
    internal readonly struct ReplicationLink
    {
        public ReplicationLink(ReplicationSource source, ReplicationTarget target)
        {
            Source = source;
            Target = target;
        }

        public ReplicationSource Source { get; }
        public ReplicationTarget Target { get; }
    }

    internal sealed class SourceConnector
    {
        public SourceConnector(ReplicationEndpoint targetEndpoint, ReplicationConnection connection)
        {
            TargetEndpoint = targetEndpoint;
            Connection = connection;
            RemoteAcceptor = RemoteActorSelection(Acceptor.Name);
        }

        public ReplicationEndpoint TargetEndpoint { get; }
        public ReplicationConnection Connection { get; }
        public ActorSelection RemoteAcceptor { get; }

        public void Activate(ImmutableHashSet<ReplicationLink> replicationLinks = null)
        {
            if (replicationLinks is null)
            {
                var builder = replicationLinks.ToBuilder();
                foreach (var replicationLink in replicationLinks)
                {
                    if (replicationLink.Source.Acceptor != RemoteAcceptor)
                        builder.Remove(replicationLink);
                }
                TargetEndpoint.System.ActorOf(Props.Create(() => new Connector(this, builder.ToImmutable())));
            }
            else TargetEndpoint.System.ActorOf(Props.Create(() => new Connector(this, null)));
        }

        public IEnumerable<ReplicationLink> Links(ReplicationEndpointInfo sourceInfo)
        {
            foreach (var logName in this.TargetEndpoint.CommonLogNames(sourceInfo))
            {
                var sourceLogId = sourceInfo.LogId(logName);
                var source = new ReplicationSource(sourceInfo.EndpointId, logName, sourceLogId, RemoteAcceptor);
                yield return new ReplicationLink(source, TargetEndpoint.Target(logName));
            }
        }

        private ActorSelection RemoteActorSelection(string actor)
        {
            var name = Connection.Name;
            var host = Connection.Host;
            var port = Connection.Port;
            if (TargetEndpoint.System is ExtendedActorSystem sys)
            {
                var protocol = sys.Provider.DefaultAddress.Protocol;
                return TargetEndpoint.System.ActorSelection($"{protocol}://{name}@{host}:{port.ToString()}/user/{actor}");
            }
            else return TargetEndpoint.System.ActorSelection($"akka.tcp://{name}@{host}:{port.ToString()}/user/{actor}");
        }
    }

    /// <summary>
    /// If `replicationLinks` is <c>null</c> reliably sends <see cref="GetReplicationEndpointInfo"/> requests to the <see cref="Acceptor"/> at a source <see cref="ReplicationEndpoint"/>.
    /// On receiving a <see cref="GetReplicationEndpointInfoSuccess"/> reply, this connector sets up log <see cref="Replicator"/>s, one per
    /// common log name between source and target endpoints.
    /// 
    /// If `replicationLinks` is notReplicator <see cref="Replicator"/>s will be setup for the given <see cref="ReplicationLink"/>s.
    /// </summary>
    internal sealed class Connector : ActorBase
    {
        private readonly SourceConnector sourceConnector;
        private readonly ImmutableHashSet<ReplicationLink> replicationLinks;
        private readonly ActorSelection acceptor;
        private ICancelable acceptorRequestSchedule = null;
        private bool connected = false;

        public Connector(SourceConnector sourceConnector, ImmutableHashSet<ReplicationLink> replicationLinks = null)
        {
            this.sourceConnector = sourceConnector;
            this.replicationLinks = replicationLinks;
            this.acceptor = sourceConnector.RemoteAcceptor;
        }

        protected override bool Receive(object message)
        {
            if (message is GetReplicationEndpointInfoSuccess success && !connected)
            {
                foreach (var link in this.sourceConnector.Links(success.Info))
                {
                    CreateReplicator(link);
                }

                this.connected = true;
                this.acceptorRequestSchedule?.Cancel();
                return true;
            }
            return false;
        }

        protected override void PreStart()
        {
            if (replicationLinks is null)
                acceptorRequestSchedule = ScheduleAcceptorRequest(acceptor);
            else
            {
                foreach (var replicationLink in this.replicationLinks)
                {
                    CreateReplicator(replicationLink);
                }
            }
        }

        protected override void PostStop()
        {
            base.PostStop();
            this.acceptorRequestSchedule?.Cancel();
        }

        private void CreateReplicator(ReplicationLink replicationLink)
        {
            Context.ActorOf(Props.Create(() => new Replicator(replicationLink.Target, replicationLink.Source)));
        }

        private ICancelable ScheduleAcceptorRequest(ActorSelection acceptor)
        {
            var interval = sourceConnector.TargetEndpoint.Settings.RetryDelay;
            var request = new GetReplicationEndpointInfo();
            acceptor.Tell(request);
            return Context.System.Scheduler.ScheduleTellRepeatedlyCancelable(interval, interval, acceptor, request, Self);
        }
    }

    /// <summary>
    /// Replicates events from a remote source log to a local target log. This replicator guarantees that
    /// the ordering of replicated events is preserved. Potential duplicates are either detected at source
    /// (which is an optimization) or at target (for correctness). Duplicate detection is based on tracked
    /// event vector times.
    /// </summary>
    internal sealed class Replicator : ActorBase
    {
        private readonly ILoggingAdapter log = Context.GetLogger();
        private readonly ReplicationTarget target;
        private readonly ReplicationSource source;
        private readonly IActorRef detector;
        private readonly IScheduler scheduler = Context.System.Scheduler;
        private readonly ReplicationSettings settings;
        private ICancelable readSchedule = null;

        public Replicator(ReplicationTarget target, ReplicationSource source)
        {
            this.target = target;
            this.source = source;
            this.settings = target.Endpoint.Settings;
            this.detector = Context.ActorOf(Props.Create(() => new FailureDetector(source.EndpointId, source.LogName, target.Endpoint.Settings.FailureDetectionLimit)));
        }

        private bool Fetching(object message)
        {
            switch (message)
            {
                case GetReplicationProgressSuccess s:
                    Context.Become(Reading);
                    Read(s.StoredReplicationProgress, s.CurrentTargetVersionVector);
                    return true;

                case GetReplicationProgressFailure f:
                    log.Warning("Replication progress read failed: {0}", f.Cause);
                    ScheduleFetch();
                    return true;

                default: return false;
            }
        }

        private bool Idle(object message)
        {
            if (message is ReplicationDue)
            {
                this.readSchedule?.Cancel(); // if it's notification from source concurrent to a scheduled read
                Context.Become(Fetching);
                return true;
            }
            else return false;
        }

        private bool Reading(object message)
        {
            switch (message)
            {
                case ReplicationReadSuccess s:
                    this.detector.Tell(FailureDetectorChange.AvailabilityDetected);
                    Context.Become(Writing);
                    Write(s.Events, s.ReplicationProgress, s.CurrentSourceVersionVector, s.ReplicationProgress >= s.FromSequenceNr);
                    return true;

                case ReplicationReadFailure f:
                    this.detector.Tell(FailureDetectorChange.FailureDetected(f.Cause));
                    log.Warning("Replication read failed: {0}", f.Cause);
                    Context.Become(Idle);
                    ScheduleRead();
                    return true;

                default: return false;
            }
        }

        private bool Writing(object message)
        {
            switch (message)
            {
                case ReplicationWriteSuccess s:
                    NotifyLocalAcceptor(s);
                    if (s.ContinueReplication)
                    {
                        var sourceMetadata = s.Metadata[source.LogId];
                        Context.Become(Reading);
                        Read(sourceMetadata.ReplicationProgress, sourceMetadata.CurrentVersionVector);
                    }
                    else
                    {
                        Context.Become(Idle);
                        ScheduleRead();
                    }
                    return true;

                case ReplicationWriteFailure f:
                    log.Warning("Replication write failed: {0}", f.Cause);
                    Context.Become(Idle);
                    ScheduleRead();
                    return true;

                default: return false;
            }
        }

        private void Fetch() =>
            target.Log.Ask(new GetReplicationProgress(source.LogId), timeout: settings.ReadTimeout)
                .PipeTo(Self, failure: error => new GetReplicationProgressFailure(error));

        private void NotifyLocalAcceptor(ReplicationWriteSuccess writeSuccess) => target.Endpoint.Acceptor.Tell(writeSuccess);

        private void ScheduleFetch() => this.scheduler.Advanced.ScheduleOnce(settings.RetryDelay, Fetch);

        private void ScheduleRead() => this.readSchedule = this.scheduler.ScheduleTellOnceCancelable(settings.RetryDelay, Self, new ReplicationDue(), Self);

        private void Read(long storedReplicationProgress, VectorTime currentTargetVersionVector)
        {
            var replicationRead = new ReplicationRead(storedReplicationProgress + 1, settings.WriteBatchSize, settings.RemoteScanLimit, NoFilter.Instance, target.LogId, Self, currentTargetVersionVector);
            source.Acceptor.Ask(new ReplicationReadEnvelope(replicationRead, source.LogName, target.Endpoint.ApplicationName, target.Endpoint.ApplicationVersion), timeout: settings.RemoteReadTimeout)
                .PipeTo(Self, failure: error => new ReplicationReadFailure(new ReplicationReadTimeoutException(settings.RemoteReadTimeout), target.LogId));
        }

        private void Write(IReadOnlyCollection<DurableEvent> events, long replicationProgress, VectorTime currentSourceVersionVector, bool continueReplication)
        {
            var metadata = ImmutableDictionary<string, ReplicationMetadata>.Empty.SetItem(source.LogId, new ReplicationMetadata(replicationProgress, currentSourceVersionVector));
            target.Log.Ask(new ReplicationWrite(events, metadata, continueReplication), timeout: settings.WriteTimeout)
                .PipeTo(Self, failure: error => new ReplicationWriteFailure(error));
        }

        protected override void PreStart() => Fetch();

        protected override void PostStop()
        {
            base.PostStop();
            readSchedule?.Cancel();
        }

        protected override void Unhandled(object message)
        {
            if (!(message is ReplicationDue))
                base.Unhandled(message);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected override bool Receive(object message) => Fetching(message);
    }

    internal readonly struct FailureDetectorChange
    {
        public enum FailureDetectorType
        {
            AvailabilityDetected,
            FailureDetected,
            FailureDetectionLimitReached
        }

        public static readonly FailureDetectorChange AvailabilityDetected = new FailureDetectorChange(FailureDetectorType.AvailabilityDetected, 0, null);
        public static FailureDetectorChange FailureDetected(Exception cause) => new FailureDetectorChange(FailureDetectorType.FailureDetected, 0, cause);
        public static FailureDetectorChange FailureDetectionLimitReached(int counter) => new FailureDetectorChange(FailureDetectorType.FailureDetectionLimitReached, counter, null);

        public FailureDetectorChange(FailureDetectorType type, int counter, Exception cause)
        {
            Type = type;
            Counter = counter;
            Cause = cause;
        }
        public readonly FailureDetectorType Type;
        public readonly int Counter;
        public readonly Exception Cause;
    }

    internal sealed class FailureDetector : ActorBase
    {
        private readonly ILoggingAdapter log = Context.GetLogger();
        private readonly string sourceEndpointId;
        private readonly string logName;
        private readonly TimeSpan failureDetectionLimit;

        private int counter = 0;
        private ImmutableArray<Exception> causes = ImmutableArray<Exception>.Empty;
        private ICancelable schedule;
        private long lastReportedAvailability = 0;
        public FailureDetector(string sourceEndpointId, string logName, TimeSpan failureDetectionLimit)
        {
            this.schedule = ScheduleFailureDetectionLimitReached();
            this.sourceEndpointId = sourceEndpointId;
            this.logName = logName;
            this.failureDetectionLimit = failureDetectionLimit;
        }

        private ICancelable ScheduleFailureDetectionLimitReached()
        {
            this.counter++;
            return Context.System.Scheduler.ScheduleTellOnceCancelable(this.failureDetectionLimit, Self, FailureDetectorChange.FailureDetectionLimitReached(counter), Self);
        }

        protected override bool Receive(object message)
        {
            if (message is FailureDetectorChange change)
            {
                switch (change.Type)
                {
                    case FailureDetectorChange.FailureDetectorType.AvailabilityDetected:
                        var currentTime = DateTime.UtcNow.Ticks; //TODO: rework to use monotonic time clock
                        var lastInterval = currentTime - lastReportedAvailability;
                        if (lastInterval >= failureDetectionLimit.Ticks)
                        {
                            Context.System.EventStream.Publish(new Available(sourceEndpointId, logName));
                            lastReportedAvailability = currentTime;
                        }
                        schedule.Cancel();
                        schedule = ScheduleFailureDetectionLimitReached();
                        causes = ImmutableArray<Exception>.Empty;
                        break;
                    case FailureDetectorChange.FailureDetectorType.FailureDetected:
                        this.causes = this.causes.Add(change.Cause);
                        break;
                    case FailureDetectorChange.FailureDetectorType.FailureDetectionLimitReached:
                        var aggregateError = new AggregateException(causes);
                        log.Error(aggregateError, "replication failure detection limit reached ({0}), publishing Unavailable for {1}/{2} (last exception being reported)", failureDetectionLimit, sourceEndpointId, logName);
                        Context.System.EventStream.Publish(new Unavailable(sourceEndpointId, logName, causes));
                        this.schedule = ScheduleFailureDetectionLimitReached();
                        this.causes = ImmutableArray<Exception>.Empty;
                        break;
                }

                return true;
            }
            else return false;
        }
    }

}
