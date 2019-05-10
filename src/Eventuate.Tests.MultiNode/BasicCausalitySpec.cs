#region copyright
// -----------------------------------------------------------------------
//  <copyright file="BasicCausalitySpec.cs" company="Akka.NET Project">
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
using Akka.Streams.Dsl;
using Eventuate.ReplicationProtocol;

namespace Eventuate.Tests.MultiNode
{
    public sealed class BasicCausalityConfig : MultiNodeReplicationConfig
    {
        public RoleName NodeA { get; }
        public RoleName NodeB { get; }
        
        public BasicCausalityConfig(Config providerConfig) : base()
        {
            NodeA = Role("nodeA");
            NodeB = Role("nodeB");

            TestTransport = true;
            CommonConfig = ConfigurationFactory.ParseString("eventuate.log.replication.remote-read-timeout = 2s")
                .WithFallback(providerConfig)
                .WithFallback(DefaultConfig);
        }
    }
    
    public abstract class BasicCausalitySpec : MultiNodeSpec, IMultiNodeReplicationEndpoint
    {
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

        protected readonly RoleName nodeA;
        protected readonly RoleName nodeB;
        protected readonly string logIdA;
        protected readonly string logIdB;
        
        protected BasicCausalitySpec(BasicCausalityConfig config, Type type) : base(config, type)
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
        public void EventsourcedActors_when_located_at_different_locations_must_track_causality()
        {
            var probe = CreateTestProbe();
            
            RunOn(() =>
            {
                var logName = this.LogName();
                var endpoint = this.CreateEndpoint(nodeA.Name,ImmutableHashSet<ReplicationConnection>.Empty.Add(Node(nodeB).Address.ToReplicationConnection()));
                var actor = Sys.ActorOf(Props.Create(() => new ReplicatedActor("pa", endpoint.Logs[logName], probe.Ref)));

                probe.ExpectMsg(("x", VectorTime(0, 1), VectorTime(0, 1)));
                
                actor.Tell("y");
                probe.ExpectMsg(("y", VectorTime(2, 1), VectorTime(2, 1)));
                
                EnterBarrier("reply");
                TestConductor.Blackhole(nodeA, nodeB, ThrottleTransportAdapter.Direction.Both).Wait();
                EnterBarrier("broken");
                
                actor.Tell("z1");
                probe.ExpectMsg(("z1", VectorTime(3, 1), VectorTime(3, 1)));
                
                EnterBarrier("repair");
                TestConductor.PassThrough(nodeA, nodeB, ThrottleTransportAdapter.Direction.Both).Wait();

                probe.ExpectMsg(("z2", VectorTime(2, 3), VectorTime(3, 3)));
            }, this.nodeA);
            
            RunOn(() =>
            {
                var logName = this.LogName();
                var endpoint = this.CreateEndpoint(nodeB.Name,ImmutableHashSet<ReplicationConnection>.Empty.Add(Node(nodeA).Address.ToReplicationConnection()));
                var actor = Sys.ActorOf(Props.Create(() => new ReplicatedActor("pb", endpoint.Logs[logName], probe.Ref)));
                
                actor.Tell("x");
                probe.ExpectMsg(("x", VectorTime(0, 1), VectorTime(0, 1)));
                probe.ExpectMsg(("y", VectorTime(2, 1), VectorTime(2, 1)));
                
                EnterBarrier("reply");
                EnterBarrier("broken");
                
                actor.Tell("z2");
                probe.ExpectMsg(("z2", VectorTime(2, 3), VectorTime(2, 3)));
                
                EnterBarrier("repair");

                probe.ExpectMsg(("z1", VectorTime(3, 1), VectorTime(3, 3)));
            }, this.nodeB);
            
            EnterBarrier("finish");
        }
    }
}