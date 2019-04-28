using Akka.Actor;
using Akka.Configuration;
using Akka.Event;
using Akka.Util;
using Eventuate.ReplicationProtocol;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
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
        public readonly struct Available
        {
            public Available(string endpointId, string logName)
            {
                EndpointId = endpointId;
                LogName = logName;
            }

            public string EndpointId { get; }
            public string LogName { get; }
        }

        /// <summary>
        /// Published to the actor system's event stream if a remote log is unavailable.
        /// </summary>
        public readonly struct Unavailable
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
            this.acceptor = new Lazy<IActorRef>(() => system.ActorOf(Props.Create(() => new Acceptor(this)), Acceptor.Name));
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
            var unfilteredLinks = recoveryLinks.Where(recovery.IsFilteredLink)
        }
    }

    public interface IEndpointFilter
    {

    }

    public sealed class NoFilters : IEndpointFilter
    {
        public static IEndpointFilter Instance { get; } = new NoFilters();
        private NoFilters() { }
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
                TargetEndpoint.System.ActorOf(Props.Create(() => new Connector(this, builder.ToImmutable())))
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
        private readonly Stopwatch stopwatch = new Stopwatch();

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
            this.stopwatch.Start();
        }

        private ICancelable ScheduleFailureDetectionLimitReached()
        {
            this.counter++;
            return Context.System.Scheduler.ScheduleTellOnceCancelable(this.failureDetectionLimit, Self, FailureDetectorChange.FailureDetectionLimitReached(counter), ActorRefs.NoSender);
        }

        protected override bool Receive(object message)
        {
            if (message is FailureDetectorChange change)
            {
                switch (change.Type)
                {
                    case FailureDetectorChange.FailureDetectorType.AvailabilityDetected:
                        var currentTime = stopwatch.ElapsedTicks;
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
