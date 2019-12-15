#region copyright
// -----------------------------------------------------------------------
//  <copyright file="ReplicatedORSetSpec.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Collections.Immutable;
using Akka.Actor;
using Akka.Configuration;
using Akka.Remote.TestKit;
using Akka.Remote.Transport;
using Eventuate.Crdt;
using FluentAssertions;

namespace Eventuate.Tests.MultiNode.Crdt
{
    public class ReplicatedORSetConfig : MultiNodeReplicationConfig
    {
        public RoleName NodeA { get; }
        public RoleName NodeB { get; }
        
        protected readonly Config ReplicationConfig;
        
        public ReplicatedORSetConfig()
        {
            NodeA = Role("nodeA");
            NodeB = Role("nodeB");
            
            CommonConfig = ConfigurationFactory.ParseString(@"
                eventuate.log.rocksdb.dir = ""target/test-log""
                eventuate.log.write-batch-size = 200
                eventuate.log.replication.remote-read-timeout = 2s")
                .WithFallback(DefaultConfig);
            
            TestTransport = true;
        }
    }
    
    public class ReplicatedORSetSpec : MultiNodeSpec, IMultiNodeReplicationEndpoint
    {
        private sealed class TestORSetService : ORSetService<int>
        {
            private readonly IActorRef probe;

            public TestORSetService(ActorSystem system, string serviceId, IActorRef eventLog, IActorRef probe) : base(system, serviceId, eventLog)
            {
                this.probe = probe;
            }

            protected override void OnChange(ORSet<int> crdt, object operation) => this.probe.Tell(crdt.Value);
        }
        
        private readonly RoleName nodeA;
        private readonly RoleName nodeB;
        
        public ReplicatedORSetSpec() : this(new ReplicatedORSetConfig()){}
        
        private ReplicatedORSetSpec(ReplicatedORSetConfig config) : base(config, typeof(ReplicatedORSetSpec))
        {
            this.nodeA = config.NodeA;
            this.nodeB = config.NodeB;
            MuteDeadLetters(Sys, typeof(object));
        }
        
        protected override int InitialParticipantsValueFactory => Roles.Count;

        [MultiNodeFact]
        public void Replicated_ORSet_must_converge()
        {
            var probe = CreateTestProbe();
            RunOn(() =>
            {
                var endpoint = this.CreateEndpoint(nodeA.Name,ImmutableHashSet<ReplicationConnection>.Empty.Add(Node(nodeB).Address.ToReplicationConnection()));
                var service = new TestORSetService(Sys, "A", endpoint.Logs[this.LogName()], probe.Ref);

                service.Add("x", 1);
                probe.ExpectMsg<ImmutableHashSet<int>>(x => x.Should().BeEquivalentTo(1));
                probe.ExpectMsg<ImmutableHashSet<int>>(x => x.Should().BeEquivalentTo(1, 2));
                
                // network partition
                TestConductor.Blackhole(nodeA, nodeB, ThrottleTransportAdapter.Direction.Both).Wait();
                EnterBarrier("broken");
                
                // this is concurrent to service.remove("x", 1) on node B
                service.Add("x", 1);
                probe.ExpectMsg<ImmutableHashSet<int>>(x => x.Should().BeEquivalentTo(1, 2));
                
                EnterBarrier("repair");
                TestConductor.PassThrough(nodeA, nodeB, ThrottleTransportAdapter.Direction.Both).Wait();
                
                probe.ExpectMsg<ImmutableHashSet<int>>(x => x.Should().BeEquivalentTo(1, 2));
                service.Remove("x", 2);
                probe.ExpectMsg<ImmutableHashSet<int>>(x => x.Should().BeEquivalentTo(1));
            }, nodeA);
            
            RunOn(() =>
            {
                var endpoint = this.CreateEndpoint(nodeA.Name,ImmutableHashSet<ReplicationConnection>.Empty.Add(Node(nodeA).Address.ToReplicationConnection()));
                var service = new TestORSetService(Sys, "B", endpoint.Logs[this.LogName()], probe.Ref);

                service.GetValue("x").Result.Should().BeEquivalentTo(1);
                probe.ExpectMsg<ImmutableHashSet<int>>(x => x.Should().BeEquivalentTo(1));
                service.Add("x", 2);
                probe.ExpectMsg<ImmutableHashSet<int>>(x => x.Should().BeEquivalentTo(1, 2));
                
                EnterBarrier("broken");
                
                // this is concurrent to service.add("x", 1) on node A
                service.Remove("x", 1);
                probe.ExpectMsg<ImmutableHashSet<int>>(x => x.Should().BeEquivalentTo(2));
                
                EnterBarrier("repair");
                
                // add has precedence over (concurrent) remove
                
                probe.ExpectMsg<ImmutableHashSet<int>>(x => x.Should().BeEquivalentTo(1, 2));
                probe.ExpectMsg<ImmutableHashSet<int>>(x => x.Should().BeEquivalentTo(1));
            }, nodeB);
        }

        public Props LogProps(string logId)
        {
            throw new NotImplementedException();
            //return LeveldbEventLog.props(logId)
        }
    }
}