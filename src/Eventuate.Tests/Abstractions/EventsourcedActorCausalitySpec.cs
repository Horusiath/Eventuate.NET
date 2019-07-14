using System.Collections.Immutable;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.TestKit;
using Xunit;
using Xunit.Abstractions;

namespace Eventuate.Tests.Abstractions
{
    public abstract class EventsourcedActorCausalitySpec : MultiLocationSpec
    {
        sealed class Collaborator : EventsourcedActor
        {
            private readonly ImmutableHashSet<string> handles;
            private readonly IActorRef probe;

            public Collaborator(string id, IActorRef eventLog, ImmutableHashSet<string> handles, IActorRef probe)
            {
                this.handles = handles;
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
                    Persist(s, attempt =>
                    {
                        if (attempt.IsFailure) throw attempt.Exception;
                    });
                    return true;
                }
                else return false;
            }

            protected override bool OnEvent(object message)
            {
                if (message is string s && handles.Contains(s))
                {
                    probe.Tell((s, LastVectorTimestamp, CurrentVersion));
                    return true;
                }

                return false;
            }
        }

        private const string LogName = "L1";
        private readonly ITestOutputHelper output;
        
        protected EventsourcedActorCausalitySpec(ITestOutputHelper output)
        {
            this.output = output;
        }
        
        [Fact]
        public async Task Eventsourced_actors_when_located_at_different_locations_can_track_causality()
        {
            var locationA = Location("A");
            var locationB = Location("B");
            
            InitializeLogger(locationA.System, this.output);
            InitializeLogger(locationB.System, this.output);

            var endpointA = locationA.Endpoint(ImmutableHashSet.Create(LogName),
                ImmutableHashSet.Create(new ReplicationConnection("127.0.0.1", locationB.Port)));
            var endpointB = locationB.Endpoint(ImmutableHashSet.Create(LogName),
                ImmutableHashSet.Create(new ReplicationConnection("127.0.0.1", locationA.Port)));

            var logA = endpointA.Logs[LogName];
            var logB = endpointB.Logs[LogName];
            
            var logIdA = endpointA.LogId(LogName);
            var logIdB = endpointB.LogId(LogName);

            var probeA1 = new TestProbe(locationA.System, Assert);
            var probeA2 = new TestProbe(locationA.System, Assert);
            var probeA3 = new TestProbe(locationA.System, Assert);
            var probeB = new TestProbe(locationB.System, Assert);

            var actorA1 = locationA.System.ActorOf(Props.Create(() =>
                new Collaborator("pa1", logA, ImmutableHashSet.Create("e1", "e2", "e5"), probeA1.Ref)));
            var actorA2 = locationA.System.ActorOf(Props.Create(() =>
                new Collaborator("pa2", logA, ImmutableHashSet.Create("e3", "e5", "e6"), probeA2.Ref)));
            var actorA3 = locationA.System.ActorOf(Props.Create(() =>
                new Collaborator("pa3", logA, ImmutableHashSet.Create("e4"), probeA3.Ref)));
            var actorB = locationB.System.ActorOf(Props.Create(() =>
                new Collaborator("pb", logB, ImmutableHashSet.Create("e1", "e6"), probeB.Ref)));
            
            VectorTime Timestamp(long a, long b)
            {
                if (a == 0 && b == 0) return VectorTime.Zero;
                if (a == 0) return new VectorTime((logIdB, b));
                if (b == 0) return new VectorTime((logIdA, a));
                return new VectorTime((logIdA, a), (logIdB, b));
            }
            
            actorB.Tell("e1");
            probeA1.ExpectMsg(("e1", Timestamp(0, 1), Timestamp(0, 1)));
            probeB.ExpectMsg(("e1", Timestamp(0, 1), Timestamp(0, 1)));
            
            actorA1.Tell("e2");
            probeA1.ExpectMsg(("e2", Timestamp(2, 1), Timestamp(2, 1)));
            
            actorA2.Tell("e3");
            probeA2.ExpectMsg(("e1", Timestamp(3, 0), Timestamp(3, 0)));
            
            actorA3.Tell("e4");
            probeA3.ExpectMsg(("e4", Timestamp(4, 0), Timestamp(4, 0)));
            
            actorA1.Tell("e5");
            probeA1.ExpectMsg(("e5", Timestamp(5, 1), Timestamp(5, 1)));
            probeA2.ExpectMsg(("e5", Timestamp(5, 1), Timestamp(5, 1)));
            
            actorA2.Tell("e6");
            probeA2.ExpectMsg(("e6", Timestamp(6, 1), Timestamp(6, 1)));
            probeB.ExpectMsg(("e6", Timestamp(6, 1), Timestamp(6, 1)));

            // -----------------------------------------------------------
            //  Please note:
            //  - e2 <-> e3 (because e1 -> e2 and e1 <-> e3)
            //  - e3 <-> e4 (but plausible clocks reports e3 -> e4)
            // -----------------------------------------------------------
        }
    }
}