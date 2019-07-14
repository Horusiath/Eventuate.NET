using System;
using System.Collections.Immutable;
using System.Linq.Expressions;
using Akka.Actor;
using Akka.Configuration;
using Akka.Event;
using Akka.TestKit;
using Akka.TestKit.Xunit2;
using Akka.TestKit.Xunit2.Internals;
using Xunit.Abstractions;

namespace Eventuate.Tests
{
    public static class MultiLocationConfig
    {
        public static Config Create(int port = 0, Config customConfig = null)
        {
            var defaultConfig = ConfigurationFactory.ParseString($@"
                akka.actor.provider = ""remote""
                akka.remote.enabled-transports = [""akka.remote.dot-netty.tcp""]
                akka.remote.netty.tcp.hostname = ""127.0.0.1""
                akka.remote.netty.tcp.port = {port}
                akka.remote.retry-gate-closed-for = 300ms
                akka.test.single-expect-default = 20s
                akka.loglevel = ""ERROR""
                
                eventuate.log.write-batch-size = 3
                eventuate.log.replication.retry-delay = 1s
                eventuate.log.replication.remote-read-timeout = 1s
                eventuate.log.replication.failure-detection-limit = 3s
                eventuate.snapshot.filesystem.dir = target/test-snapshot
            ");

            return customConfig is null ? defaultConfig : customConfig.WithFallback(defaultConfig);
        }
    }
    
    public sealed class Location : IDisposable
    {
        public sealed class EventListener : TestProbe
        {
            private sealed class EventListenerView : EventsourcedView
            {
                private readonly EventListener listener;

                public EventListenerView(EventListener listener)
                {
                    this.listener = listener;
                    Id = "EventListener-" + listener.LocationId;
                    EventLog = listener.EventLog;
                }

                public override string Id { get; }
                public override IActorRef EventLog { get; }
                protected override bool OnCommand(object message) => true;

                protected override bool OnEvent(object message)
                {
                    listener.Ref.Tell(message);
                    return true;
                }
            }
            
            public string LocationId { get; }
            public IActorRef EventLog { get; }
            public string AggregateId { get; }

            public IActorRef View { get; }

            public EventListener(string locationId, IActorRef eventLog, ActorSystem system, ITestKitAssertions assertions, string aggregateId = null) 
                : base(system, assertions, "EventListener-" + locationId)
            {
                LocationId = locationId;
                EventLog = eventLog;
                AggregateId = aggregateId;
                View = system.ActorOf(Props.Create(() => new EventListenerView(this)));
            }
        }
        
        public string Id { get; }
        public Func<string, Props> LogFactory { get; }
        public ActorSystem System { get; }
        public int Port { get; }
        public TestProbe Probe { get; }

        private static readonly XunitAssertions Assertions = new XunitAssertions();

        public Location(string id, Func<string, Props> logFactory, int customPort = 0, Config customConfig = null)
        {
            Id = id;
            LogFactory = logFactory;
            this.System = ActorSystem.Create(ReplicationConnection.DefaultRemoteSystemName,
                MultiLocationConfig.Create(customPort, customConfig));
            this.Port = customPort != 0 ? customPort : ((ExtendedActorSystem) this.System).Provider.DefaultAddress.Port.Value;
            this.Probe = new TestProbe(System, Assertions);
        }
        
        public EventListener Listener(IActorRef eventLog, string aggregateId = null) =>
            new EventListener(Id, eventLog, System, Assertions, aggregateId);

        public ReplicationEndpoint Endpoint(
            ImmutableHashSet<string> logNames,
            ImmutableHashSet<ReplicationConnection> connections,
            IEndpointFilter endpointFilters = null,
            string applicationName = null,
            ApplicationVersion? applicationVersion = null,
            bool activate = true)
        {
            endpointFilters = endpointFilters ?? NoFilters.Instance;
            applicationName = applicationName ?? ReplicationEndpoint.DefaultApplicationName;
            var appVer = applicationVersion ?? ReplicationEndpoint.DefaultApplicationVersion;
            
            var endpoint = new ReplicationEndpoint(System, Id, logNames, LogFactory, connections, endpointFilters, applicationName, appVer);
            if (activate)
                endpoint.Activate();
            return endpoint;
        }

        public void Dispose()
        {
            System.Dispose();
        }
    }
    
    public abstract class MultiLocationSpec : IDisposable
    {
        protected static readonly ITestKitAssertions Assert = new XunitAssertions();
        
        private readonly Config providerConfig;
        private ImmutableList<Location> locations = ImmutableList<Location>.Empty;
        private static int counter = 0;

        protected void InitializeLogger(ActorSystem system, ITestOutputHelper output)
        {
            ((ExtendedActorSystem) system).SystemActorOf(Props.Create<TestOutputLogger>((Expression<Func<TestOutputLogger>>) (() => new TestOutputLogger(output)), (SupervisorStrategy) null), "log-test").Tell((object) new InitializeLogger((LoggingBus) system.EventStream));
        }
        
        protected MultiLocationSpec(Config providerConfig = null)
        {
            this.providerConfig = providerConfig;
            this.Config = MultiLocationConfig.Create(0, providerConfig);
            counter++;
        }

        public abstract Props LogFactory(string logName);
        
        public Config Config { get; }

        public Location Location(string name, int customPort = 0, Config customConfig = null,
            Func<string, Props> logFactory = null)
        {
            logFactory = logFactory ?? LogFactory;
            var location = new Location(LocationId(name), logFactory, customPort, customConfig is null ? providerConfig : customConfig.WithFallback(providerConfig));
            locations = locations.Add(location);
            return location;
        }

        public string LocationId(string name) => $"{name}_{counter}";
        
        public void Dispose()
        {
            foreach (var location in locations)
            {
                location.Dispose();
            }
        }
    }
}