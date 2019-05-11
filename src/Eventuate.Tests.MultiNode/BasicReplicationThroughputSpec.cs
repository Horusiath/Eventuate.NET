
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using Akka.Actor;
using Akka.Configuration;
using Akka.Remote.TestKit;
using FluentAssertions;
using FluentAssertions.Extensions;

namespace Eventuate.Tests.MultiNode
{
    public class BasicReplicationThroughputConfig : MultiNodeReplicationConfig
    {
        public RoleName NodeA { get; }
        public RoleName NodeB { get; }
        public RoleName NodeC { get; }
        public RoleName NodeD { get; }
        public RoleName NodeE { get; }
        public RoleName NodeF { get; }
        
        public BasicReplicationThroughputConfig(Config providerConfig) : base()
        {
            NodeA = Role("nodeA");
            NodeB = Role("nodeB");
            NodeC = Role("nodeC");
            NodeD = Role("nodeD");
            NodeE = Role("nodeE");
            NodeF = Role("nodeF");

            var customConfig = ConfigurationFactory.ParseString(@"
              akka.remote.netty.tcp.maximum-frame-size = 1048576
              eventuate.log.write-batch-size = 2000
              eventuate.log.replication.retry-delay = 10s
            ");
            CommonConfig = customConfig 
                .WithFallback(providerConfig)
                .WithFallback(DefaultConfig);
        }
    }
    public abstract class BasicReplicationThroughputSpec : MultiNodeSpec, IMultiNodeReplicationEndpoint
    {
        private readonly RoleName nodeA;
        private readonly RoleName nodeB;
        private readonly RoleName nodeC;
        private readonly RoleName nodeD;
        private readonly RoleName nodeE;
        private readonly RoleName nodeF;

        internal sealed class ReplicatedActor : EventsourcedActor
        {
            private readonly IActorRef probe;
            private readonly Stopwatch stopwatch = new Stopwatch();
            private ImmutableArray<string> events = ImmutableArray<string>.Empty;

            public ReplicatedActor(string id, IActorRef eventLog, IActorRef probe)
            {
                this.probe = probe;
                Id = id;
                EventLog = eventLog;
            }

            public override bool StateSync => false;
            public override string Id { get; }
            public override IActorRef EventLog { get; }
            protected override bool OnCommand(object message)
            {
                if (message is string s)
                {
                    if (s == "stats")
                        probe.Tell($"{1000.0 * events.Length / stopwatch.ElapsedMilliseconds} events/sec");
                    
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
                switch (message)
                {
                    case "start": stopwatch.Start(); return true;
                    case "stop": 
                        stopwatch.Stop();
                        probe.Tell(events);
                        return true;
                    case string s:
                        events = events.Add(s);
                        return true;
                    default: return false;
                }
            }
        }

        
        protected BasicReplicationThroughputSpec(BasicReplicationThroughputConfig config, Type type) : base(config, type)
        {
            this.nodeA = config.NodeA;
            this.nodeB = config.NodeB;
            this.nodeC = config.NodeC;
            this.nodeD = config.NodeD;
            this.nodeE = config.NodeE;
            this.nodeF = config.NodeF;

            MuteDeadLetters(Sys, typeof(object));
        }
        
        protected override int InitialParticipantsValueFactory => Roles.Count;
        public abstract Props LogProps(string logId);

        [MultiNodeFact]
        public void EventLog_replication_must_replicate_all_events_by_default()
        {
            var probe = CreateTestProbe();
            var num = 5000;
            var timeout = 60.Seconds();
            var expectedBuilder = ImmutableArray.CreateBuilder<string>(num);
            for (int i = 0; i < num; i++)
            {
                expectedBuilder.Add($"e-{i}");
            }

            var expected = expectedBuilder.ToArray();
            var actor = Sys.DeadLetters;
            var logName = this.LogName();
            
            // ---------------------------------------
            //
            //  Topology:
            //
            //  A        E
            //   \      /
            //    C -- D
            //   /      \
            //  B        F
            //
            // ---------------------------------------

            RunOn(() =>
            {
                var endpoint = this.CreateEndpoint(nodeA.Name, ImmutableHashSet<ReplicationConnection>.Empty.Add(Node(nodeC).Address.ToReplicationConnection()));
                actor = Sys.ActorOf(Props.Create(() => new ReplicatedActor("pa", endpoint.Logs[logName], probe.Ref)));
                
                actor.Tell("start");
                foreach (var s in expected)
                {
                    actor.Tell(s);
                }
                actor.Tell("stop");
                
            }, nodeA);
            
            RunOn(() =>
            {
                var endpoint = this.CreateEndpoint(nodeB.Name, ImmutableHashSet<ReplicationConnection>.Empty.Add(Node(nodeC).Address.ToReplicationConnection()));
                actor = Sys.ActorOf(Props.Create(() => new ReplicatedActor("pb", endpoint.Logs[logName], probe.Ref)));
            }, nodeB);
            
            RunOn(() =>
            {
                var endpoint = this.CreateEndpoint(nodeC.Name, ImmutableHashSet<ReplicationConnection>.Empty
                    .Add(Node(nodeA).Address.ToReplicationConnection())
                    .Add(Node(nodeB).Address.ToReplicationConnection())
                    .Add(Node(nodeD).Address.ToReplicationConnection()));
                actor = Sys.ActorOf(Props.Create(() => new ReplicatedActor("pc", endpoint.Logs[logName], probe.Ref)));
            }, nodeC);
            
            RunOn(() =>
            {
                var endpoint = this.CreateEndpoint(nodeD.Name, ImmutableHashSet<ReplicationConnection>.Empty
                    .Add(Node(nodeC).Address.ToReplicationConnection())
                    .Add(Node(nodeE).Address.ToReplicationConnection())
                    .Add(Node(nodeF).Address.ToReplicationConnection()));
                actor = Sys.ActorOf(Props.Create(() => new ReplicatedActor("pd", endpoint.Logs[logName], probe.Ref)));
            }, nodeD);

            RunOn(() =>
            {
                var endpoint = this.CreateEndpoint(nodeE.Name, ImmutableHashSet<ReplicationConnection>.Empty.Add(Node(nodeD).Address.ToReplicationConnection()));
                actor = Sys.ActorOf(Props.Create(() => new ReplicatedActor("pe", endpoint.Logs[logName], probe.Ref)));
            }, nodeE);
            
            RunOn(() =>
            {
                var endpoint = this.CreateEndpoint(nodeF.Name, ImmutableHashSet<ReplicationConnection>.Empty.Add(Node(nodeD).Address.ToReplicationConnection()));
                actor = Sys.ActorOf(Props.Create(() => new ReplicatedActor("pf", endpoint.Logs[logName], probe.Ref)));
            }, nodeF);
            
            probe.ExpectMsg(expected, timeout);
            actor.Tell("stats");
            Sys.Log.Info(probe.ReceiveOne(timeout).ToString());
            
            EnterBarrier("finish");
        }
    }
}