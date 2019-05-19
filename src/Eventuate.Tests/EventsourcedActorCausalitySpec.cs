using System.Collections.Immutable;
using Akka.Actor;
using Xunit.Abstractions;

namespace Eventuate.Tests
{
    public class EventsourcedActorCausalitySpec
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

        public EventsourcedActorCausalitySpec(ITestOutputHelper output)
        {
        }
    }
}