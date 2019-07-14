#region copyright
// -----------------------------------------------------------------------
//  <copyright file="EventsourcedViewSpec.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Xunit;
using Xunit.Abstractions;

namespace Eventuate.Tests.Abstractions
{
    public sealed class ReplicatedActor : EventsourcedActor
    {
        private readonly IActorRef probe;

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
            Persist(message, result => result.ThrowIfFailure());
            return true;
        }

        protected override bool OnEvent(object message)
        {
            probe.Tell(message);
            return true;
        }
    }

    public abstract class ReplicationCycleSpec : MultiLocationSpec
    {
        #region internal classes

        #endregion
        
        private readonly Lazy<Location> locationA;
        private readonly Lazy<Location> locationB;
        private readonly Lazy<Location> locationC;

        protected ReplicationCycleSpec()
        {
            this.locationA = new Lazy<Location>(() => this.Location("A"));
            this.locationA = new Lazy<Location>(() => this.Location("B"));
            this.locationA = new Lazy<Location>(() => this.Location("C"));
        }

        public Location LocationA => locationA.Value;
        public Location LocationB => locationB.Value;
        public Location LocationC => locationC.Value;

        private void TestReplication(ReplicationEndpoint endpointA, ReplicationEndpoint endpointB, ReplicationEndpoint endpointC)
        {
            var actorA = LocationA.System.ActorOf(Props.Create(() => new ReplicatedActor("pa", endpointA.Logs["L1"], LocationA.Probe.Ref)));
            var actorB = LocationB.System.ActorOf(Props.Create(() => new ReplicatedActor("pb", endpointB.Logs["L1"], LocationB.Probe.Ref)));
            var actorC = LocationC.System.ActorOf(Props.Create(() => new ReplicatedActor("pc", endpointC.Logs["L1"], LocationC.Probe.Ref)));

            actorA.Tell("a1");
            actorB.Tell("b1");
            actorC.Tell("c1");

            var expected = new[] {"a1", "b1", "c1"};

            LocationA.Probe.ExpectMsgAllOf(expected);
            LocationB.Probe.ExpectMsgAllOf(expected);
            LocationC.Probe.ExpectMsgAllOf(expected);

            var num = 100;
            var expectedA = Enumerable.Range(2, 100).Select(i => $"a{i}").ToImmutableArray();
            var expectedB = Enumerable.Range(2, 100).Select(i => $"b{i}").ToImmutableArray();
            var expectedC = Enumerable.Range(2, 100).Select(i => $"c{i}").ToImmutableArray();
            var all = expectedA.Union(expectedB).Union(expectedC).ToImmutableArray();

            Task.Run(() => { foreach (var x in expectedA) actorA.Tell(x); });
            Task.Run(() => { foreach (var x in expectedB) actorB.Tell(x); });
            Task.Run(() => { foreach (var x in expectedC) actorC.Tell(x); });

            LocationA.Probe.ExpectMsgAllOf(all);
            LocationB.Probe.ExpectMsgAllOf(all);
            LocationC.Probe.ExpectMsgAllOf(all);
        }

        [Fact]
        public void EventLog_replication_must_support_bidirectional_cyclic_replication_networks()
        {
            TestReplication(
                LocationA.Endpoint(ImmutableHashSet.Create("L1"), ImmutableHashSet.Create(new ReplicationConnection("127.0.0.1", LocationB.Port), new ReplicationConnection("127.0.0.1", LocationC.Port))),
                LocationB.Endpoint(ImmutableHashSet.Create("L1"), ImmutableHashSet.Create(new ReplicationConnection("127.0.0.1", LocationA.Port), new ReplicationConnection("127.0.0.1", LocationC.Port))),
                LocationC.Endpoint(ImmutableHashSet.Create("L1"), ImmutableHashSet.Create(new ReplicationConnection("127.0.0.1", LocationA.Port), new ReplicationConnection("127.0.0.1", LocationB.Port))));
        }
        
        [Fact]
        public void EventLog_replication_must_support_unidirectional_cyclic_replication_networks()
        {
            TestReplication(
                LocationA.Endpoint(ImmutableHashSet.Create("L1"), ImmutableHashSet.Create(new ReplicationConnection("127.0.0.1", LocationB.Port))),
                LocationB.Endpoint(ImmutableHashSet.Create("L1"), ImmutableHashSet.Create(new ReplicationConnection("127.0.0.1", LocationC.Port))),
                LocationC.Endpoint(ImmutableHashSet.Create("L1"), ImmutableHashSet.Create(new ReplicationConnection("127.0.0.1", LocationA.Port))));
        }
    }
}