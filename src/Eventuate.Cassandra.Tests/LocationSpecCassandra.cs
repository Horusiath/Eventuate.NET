#region copyright
// -----------------------------------------------------------------------
//  <copyright file="LocationSpecCassandra.cs">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Collections.Immutable;
using System.Diagnostics.Tracing;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.TestKit;
using Akka.TestKit.Xunit2;
using Eventuate.Snapshots;
using Eventuate.Tests.EventLogs;

namespace Eventuate.Cassandra.Tests
{
    sealed class TestIndexStore : CassandraIndexStore
    {
        public TestIndexStore(Cassandra cassandra, string logId) : base(cassandra, logId)
        {
        }
    }
    
    sealed class TestEventLog : CassandraEventLog
    {
        public TestEventLog(string id, bool aggregateIndexing, ISnapshotStore snapshotStore) 
            : base(id, aggregateIndexing, snapshotStore)
        {
        }

        protected override void Unhandled(object message)
        {
            if (message is "boom") 
                throw IntegrationTestException.Instance;
            
            base.Unhandled(message);
        }
    }
    
    public abstract class SingleLocationSpecCassandra
    {
        
    }

    public static class MultiLocationConfig
    {
        public static Config Create(int port, Config config) =>
            ConfigurationFactory.ParseString($@"
                akka.actor.provider = remote
                akka.remote.netty.tcp.hostname = ""127.0.0.1""
                akka.remote.netty.tcp.port = {port}
                akka.remote.retry-gate-closed-for = 300ms
                akka.test.single-expect-default = 20s
                akka.loglevel = ERROR
                
                eventuate.log.write-batch-size = 3
                eventuate.log.replication.retry-delay = 1s
                eventuate.log.replication.remote-read-timeout = 1s
                eventuate.log.replication.failure-detection-limit = 3s
                eventuate.snapshot.filesystem.dir = target/test-snapshot
            ").WithFallback(config);
    }

    public abstract class MultiLocationSpec
    {
        
    }

    public sealed class EventListener : TestProbe
    {
        sealed class EventListenerView : EventsourcedView
        {
            private readonly TestProbe probe;

            public EventListenerView(string id, string aggregateId, IActorRef eventLog, TestProbe probe)
            {
                this.probe = probe;
                Id = id;
                AggregateId = aggregateId;
                EventLog = eventLog;
            }

            public override string Id { get; }
            public override string AggregateId { get; }
            public override IActorRef EventLog { get; }
            protected override bool OnCommand(object message) => true;

            protected override bool OnEvent(object message)
            {
                probe.Ref.Tell(message);
                return true;
            }
        }
        
        public EventListener(string locationId, IActorRef eventLog, string aggregateId, ActorSystem system)
            : base(system, new XunitAssertions())
        {
            system.ActorOf(Props.Create(() => new EventListenerView(this.TestActor.Path.Name, aggregateId, eventLog, this)));
        }
    }

    public sealed class Location : IDisposable
    {
        public string Id { get; }
        public Func<string, Props> LogFactory { get; }
        public int Port { get; }
        public ActorSystem System { get; }
        public TestProbe Probe { get; }

        public Location(string id, Func<string, Props> logFactory, int customPort, Config customConfig)
        {
            this.System = ActorSystem.Create("location", MultiLocationConfig.Create(customPort, customConfig));
            this.Id = id;
            this.LogFactory = logFactory;
            this.Port = customPort != 0 ? customPort : ((ExtendedActorSystem)System).Provider.DefaultAddress.Port.Value;
            this.Probe = new TestProbe(System, new XunitAssertions());
        }

        public EventListener Listener(IActorRef eventLog, string? aggregateId = null) => 
            new EventListener(Id, eventLog, aggregateId, System);

        public ReplicationEndpoint Endpoint(
            ImmutableHashSet<string> logNames,
            ImmutableHashSet<ReplicationConnection> connections,
            IEndpointFilter endpointFilters = null,
            string applicationName = null,
            ApplicationVersion applicationVersion = default,
            bool activate = true)
        {
            endpointFilters ??= NoFilters.Instance;
            applicationName ??= "default";
            applicationVersion = applicationVersion == default ? new ApplicationVersion(1, 0) : applicationVersion;
            
            var endpoint = new ReplicationEndpoint(System, Id, logNames, LogFactory, connections, endpointFilters, applicationName, applicationVersion);
            if (activate) endpoint.Activate();
            return endpoint;
        }
        
        public Task Terminate() => System.Terminate();

        public void Dispose()
        {
            System?.Dispose();
        }
    }
}