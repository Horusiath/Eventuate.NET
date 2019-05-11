using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using Akka.Actor;
using Akka.Configuration;
using Akka.Remote.TestKit;
using FluentAssertions;

namespace Eventuate.Tests.MultiNode
{
    public class BasicReplicationConfig : MultiNodeReplicationConfig
    {
        public RoleName NodeA { get; }
        public RoleName NodeB { get; }
        public RoleName NodeC { get; }
        
        public BasicReplicationConfig(Config providerConfig) : base()
        {
            NodeA = Role("nodeA");
            NodeB = Role("nodeB");
            NodeC = Role("nodeC");

            CommonConfig = providerConfig.WithFallback(DefaultConfig);
        }
    }
    public abstract class BasicReplicationSpec : MultiNodeSpec, IMultiNodeReplicationEndpoint
    {
        private readonly RoleName nodeA;
        private readonly RoleName nodeB;
        private readonly RoleName nodeC;

        internal sealed class ReplicatedActor : EventsourcedActor
        {
            private readonly IActorRef probe;

            public ReplicatedActor(string id, IActorRef eventLog, IActorRef probe)
            {
                this.probe = probe;
                Id = id;
                EventLog = eventLog;
            }

            public override string Id { get; }
            public override IActorRef EventLog { get; }
            protected override bool OnCommand(object message)
            {
                if (message is string s)
                {
                    Persist(s, r =>
                    {
                        if (r.IsFailure) throw r.Exception;
                    });
                    return true;
                }
                else return false;
            }

            protected override bool OnEvent(object message)
            {
                if (message is string s)
                {
                    probe.Tell((s, LastVectorTimestamp, CurrentVersion));
                    return true;
                }
                else return false;
            }
        }

        
        protected BasicReplicationSpec(BasicReplicationConfig config, Type type) : base(config, type)
        {
            this.nodeA = config.NodeA;
            this.nodeB = config.NodeB;
            this.nodeC = config.NodeC;

            MuteDeadLetters(Sys, typeof(object));
        }
        
        protected override int InitialParticipantsValueFactory => Roles.Count;
        public abstract Props LogProps(string logId);

        private void AssertPartialOrder<T>(IList<T> events, params T[] sample)
        {
            var indices = sample.Select(events.IndexOf).ToArray();
            indices.OrderBy(x=>x).Should().AllBeEquivalentTo(indices);
        }

        [MultiNodeFact]
        public void EventLog_replication_must_replicate_all_events_by_default()
        {
            var probe = CreateTestProbe();
            var logName = this.LogName();
            
            RunOn(() =>
            {
                var endpoint = this.CreateEndpoint(nodeA.Name, ImmutableHashSet<ReplicationConnection>.Empty.Add(Node(nodeB).Address.ToReplicationConnection()));
                var actor = Sys.ActorOf(Props.Create(() => new ReplicatedActor("pa", endpoint.Logs[logName], probe.Ref)));
                
                actor.Tell("A1");
                actor.Tell("A2");
                
            }, nodeA);
            
            RunOn(() =>
            {
                var endpoint = this.CreateEndpoint(nodeB.Name, ImmutableHashSet<ReplicationConnection>.Empty
                    .Add(Node(nodeA).Address.ToReplicationConnection())
                    .Add(Node(nodeC).Address.ToReplicationConnection()));
                var actor = Sys.ActorOf(Props.Create(() => new ReplicatedActor("pb", endpoint.Logs[logName], probe.Ref)));
                
                actor.Tell("B1");
                actor.Tell("B2");
                
            }, nodeB);
            
            RunOn(() =>
            {
                var endpoint = this.CreateEndpoint(nodeC.Name, ImmutableHashSet<ReplicationConnection>.Empty.Add(Node(nodeB).Address.ToReplicationConnection()));
                var actor = Sys.ActorOf(Props.Create(() => new ReplicatedActor("pc", endpoint.Logs[logName], probe.Ref)));
                
                actor.Tell("C1");
                actor.Tell("C2");
                
            }, nodeC);

            var actual = probe.ExpectMsgAllOf("A1", "A2", "B1", "B2", "C1", "C2").ToList();
            
            AssertPartialOrder(actual, "A1", "A2");
            AssertPartialOrder(actual, "B1", "B2");
            AssertPartialOrder(actual, "C1", "C2");
            
            EnterBarrier("finish");
        }
    }
}