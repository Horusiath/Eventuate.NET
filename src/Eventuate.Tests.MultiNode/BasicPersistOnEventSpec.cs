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
    public sealed class BasicPersistOnEventConfig : MultiNodeReplicationConfig
    {
        public RoleName NodeA { get; }
        public RoleName NodeB { get; }
        
        public BasicPersistOnEventConfig(Config providerConfig) : base()
        {
            NodeA = Role("nodeA");
            NodeB = Role("nodeB");

            TestTransport = true;
            CommonConfig = ConfigurationFactory.ParseString("eventuate.log.replication.remote-read-timeout = 2s")
                .WithFallback(providerConfig)
                .WithFallback(DefaultConfig);
        }
    }
    
    public abstract class BasicPersistOnEventSpec : MultiNodeSpec, IMultiNodeReplicationEndpoint
    {
        internal readonly struct Ping : IEquatable<Ping>
        {
            public int Value { get; }

            public Ping(int value)
            {
                Value = value;
            }

            public bool Equals(Ping other) => Value == other.Value;
            public override bool Equals(object obj) => obj is Ping other && Equals(other);
            public override int GetHashCode() => Value;
        }
        
        internal readonly struct Pong : IEquatable<Pong>
        {
            public int Value { get; }

            public Pong(int value)
            {
                Value = value;
            }

            public bool Equals(Pong other) => Value == other.Value;
            public override bool Equals(object obj) => obj is Pong other && Equals(other);
            public override int GetHashCode() => Value;
        }
        
        internal sealed class PingActor : PersistOnEventActor
        {
            private readonly IActorRef probe;

            public PingActor(string id, IActorRef eventLog, IActorRef probe)
            {
                this.probe = probe;
                Id = id;
                EventLog = eventLog;
            }

            public override string Id { get; }
            public override IActorRef EventLog { get; }
            protected override bool OnCommand(object message)
            {
                switch (message)
                {
                    case Ping p: Persist(p, r => {}); return true;
                    default: return false;
                }
            }

            protected override bool OnEvent(object message)
            {
                switch (message)
                {
                    case Pong pong when pong.Value == 10 || pong.Value == 5: probe.Tell(message); return true;
                    case Ping ping when ping.Value == 6: probe.Tell(message); return true;
                    case Pong pong: PersistOnEvent(new Ping(pong.Value + 1)); return true;
                    default: return false;
                }
            }
        }
        
        internal sealed class PongActor : PersistOnEventActor
        {
            public PongActor(string id, IActorRef eventLog)
            {
                Id = id;
                EventLog = eventLog;
            }

            public override string Id { get; }
            public override IActorRef EventLog { get; }
            protected override bool OnCommand(object message) => true;

            protected override bool OnEvent(object message)
            {
                switch (message)
                {
                    case Ping ping: PersistOnEvent(new Pong(ping.Value)); return true;
                    default: return false;
                }
            }
        }
        
        protected readonly RoleName nodeA;
        protected readonly RoleName nodeB;
        protected readonly string logIdA;
        protected readonly string logIdB;
        
        protected BasicPersistOnEventSpec(BasicPersistOnEventConfig config, Type type) : base(config, type)
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
        public void
            EventsourcedActors_when_located_at_different_locations_must_play_partition_tolerant_event_driven_ping_pong()
        {
            var probe = CreateTestProbe();
            var logName = this.LogName();
            
            RunOn(() =>
            {
                var endpoint = this.CreateEndpoint(nodeA.Name, ImmutableHashSet<ReplicationConnection>.Empty.Add(Node(nodeB).Address.ToReplicationConnection()));
                var pingActor = Sys.ActorOf(Props.Create(() => new PingActor("ping", endpoint.Logs[logName], probe.Ref)));
                
                pingActor.Tell(new Ping(1));
                probe.ExpectMsg(new Pong(5));

                TestConductor.Blackhole(nodeA, nodeB, ThrottleTransportAdapter.Direction.Both).Wait();
                
                // partitioned from PongActor
                pingActor.Tell(new Ping(6));
                probe.ExpectMsg(new Ping(6));

                TestConductor.PassThrough(nodeA, nodeB, ThrottleTransportAdapter.Direction.Both).Wait();

                probe.ExpectMsg(new Pong(10));
            }, nodeA);
            
            RunOn(() =>
            {
                var endpoint = this.CreateEndpoint(nodeB.Name, ImmutableHashSet<ReplicationConnection>.Empty.Add(Node(nodeA).Address.ToReplicationConnection()));
                this.Sys.ActorOf(Props.Create(() => new PongActor("pong", endpoint.Logs[logName])));
            }, nodeB);
            
            EnterBarrier("finish");
        }
    }
}