using System;
using System.Collections.Immutable;
using System.Linq;
using Akka.Actor;
using Akka.Configuration;
using Akka.Remote.TestKit;
using Akka.Remote.Transport;
using Eventuate.ReplicationProtocol;
using FluentAssertions;

namespace Eventuate.Tests.MultiNode
{
    public class FailureDetectionConfig : MultiNodeReplicationConfig
    {
        public RoleName NodeA { get; }
        public RoleName NodeB { get; }
        
        public FailureDetectionConfig(Config providerConfig) : base()
        {
            NodeA = Role("nodeA");
            NodeB = Role("nodeB");

            TestTransport = true;
            CommonConfig = ConfigurationFactory.ParseString("eventuate.log.replication.remote-read-timeout = 2s")
                .WithFallback(providerConfig)
                .WithFallback(DefaultConfig);
        }
    }
    
    public abstract class FailureDetectionSpec : MultiNodeSpec, IMultiNodeReplicationEndpoint
    {
        protected readonly RoleName nodeA;
        protected readonly RoleName nodeB;
        protected readonly string logIdA;
        protected readonly string logIdB;
        
        protected FailureDetectionSpec(FailureDetectionConfig config, Type type) : base(config, type)
        {
            this.nodeA = config.NodeA;
            this.nodeB = config.NodeB;
            this.logIdA = ReplicationEndpointInfo.LogId(nodeA.Name, this.LogName());
            this.logIdB = ReplicationEndpointInfo.LogId(nodeB.Name, this.LogName());
            
            MuteDeadLetters(Sys, typeof(object));
        }

        protected VectorTime VectorTime(long a, long b)
        {
            if (a == 0) return b == 0 ? Eventuate.VectorTime.Zero : new VectorTime((logIdB, b));
            else if (b == 0) return new VectorTime((logIdA, a));
            else return new VectorTime((logIdA, a), (logIdB, b));
        }

        protected override int InitialParticipantsValueFactory => Roles.Count;
        public abstract Props LogProps(string logId);

        [MultiNodeFact]
        public void EventLog_replication_must_detect_replication_server_availability()
        {
            var probeAvailable1 = CreateTestProbe();
            var probeAvailable2 = CreateTestProbe();
            var probeUnavailable = CreateTestProbe();

            Sys.EventStream.Subscribe(probeAvailable1.Ref, typeof(ReplicationEndpoint.Available));
            Sys.EventStream.Subscribe(probeUnavailable.Ref, typeof(ReplicationEndpoint.Unavailable));
            
            EnterBarrier("subscribe");

            var logName = this.LogName();
            RunOn(() =>
            {
                this.CreateEndpoint(nodeA.Name, ImmutableHashSet<ReplicationConnection>.Empty.Add(Node(nodeB).Address.ToReplicationConnection()));
                probeAvailable1.ExpectMsg(new ReplicationEndpoint.Available(nodeB.Name, logName));
                
                EnterBarrier("connected");
                TestConductor.Blackhole(nodeA, nodeB, ThrottleTransportAdapter.Direction.Both).Wait();

                var u = probeUnavailable.ExpectMsg<ReplicationEndpoint.Unavailable>();
                u.EndpointId.Should().Be(nodeB.Name);
                u.Causes.First().Should().BeOfType<ReplicationReadTimeoutException>();
                Sys.EventStream.Subscribe(probeAvailable2.Ref, typeof(ReplicationEndpoint.Available));
                
                EnterBarrier("repair");
                TestConductor.PassThrough(nodeA, nodeB, ThrottleTransportAdapter.Direction.Both).Wait();
                probeAvailable2.ExpectMsg(new ReplicationEndpoint.Available(nodeB.Name, logName));
            }, this.nodeA);
            
            RunOn(() =>
            {
                this.CreateEndpoint(nodeB.Name, ImmutableHashSet<ReplicationConnection>.Empty.Add(Node(nodeA).Address.ToReplicationConnection()));
                probeAvailable1.ExpectMsg(new ReplicationEndpoint.Available(nodeA.Name, logName));
                
                
                EnterBarrier("connected");
                var u = probeUnavailable.ExpectMsg<ReplicationEndpoint.Unavailable>();
                u.EndpointId.Should().Be(nodeB.Name);
                u.Causes.First().Should().BeOfType<ReplicationReadTimeoutException>();
                Sys.EventStream.Subscribe(probeAvailable2.Ref, typeof(ReplicationEndpoint.Available));
                
                EnterBarrier("repair");
                probeAvailable2.ExpectMsg(new ReplicationEndpoint.Available(nodeB.Name, logName));
            }, this.nodeB);
            
            EnterBarrier("finish");
        }
    }
}