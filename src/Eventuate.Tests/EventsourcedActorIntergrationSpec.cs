#region copyright
// -----------------------------------------------------------------------
//  <copyright file="FailureDetectorSpec.cs" company="Bartosz Sypytkowski">
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
using Akka.TestKit;
using Akka.TestKit.Xunit2;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Eventuate.Tests
{
    public abstract class EventsourcedActorIntergrationSpec : TestKit
    {
        #region internal classes

        public readonly struct Cmd : IEquatable<Cmd>
        {
            public ImmutableArray<string> Payloads { get; }

            public Cmd(ImmutableArray<string> payloads)
            {
                Payloads = payloads;
            }
            
            public Cmd(params string[] payloads)
            {
                Payloads = payloads.ToImmutableArray();
            }

            public bool Equals(Cmd other) => Payloads.SequenceEqual(other.Payloads);
            public override bool Equals(object obj) => obj is Cmd other && Equals(other);
            public override int GetHashCode() => Payloads.GetHashCode();
            public override string ToString() => $"Cmd({string.Join(", ", Payloads)})";
        }
        
        public readonly struct State : IEquatable<State>
        {
            public ImmutableArray<string> Value { get; }

            public State(ImmutableArray<string> value)
            {
                Value = value;
            }

            public bool Equals(State other) => Value.SequenceEqual(other.Value);
            public override bool Equals(object obj) => obj is State other && Equals(other);
            public override int GetHashCode() => Value.GetHashCode();
            public override string ToString() => $"State({string.Join(", ", Value)})";
        }
        
        public readonly struct UnhandledEvent {}

        class ReplyActor : EventsourcedActor
        {
            public ReplyActor(string id, IActorRef eventLog)
            {
                Id = id;
                EventLog = eventLog;
            }

            public override string Id { get; }
            public override IActorRef EventLog { get; }
            
            protected override bool OnCommand(object message)
            {
                switch (message)
                {
                    case "reply-success":
                        Persist("okay", result =>
                        {
                            if (result.TryGetValue(out var value)) Sender.Tell(value);
                            else Sender.Tell("unexpected failure");
                        });
                        return true;
                    case "reply-failure":
                        Persist("boom", result =>
                        {
                            if (result.TryGetValue(out _)) Sender.Tell("unexpected success");
                            else Sender.Tell(result.Exception.Message);
                        });
                        return true;
                    default: return false;
                }
            }

            protected override bool OnEvent(object message) => true;
        }

        class BatchActor : EventsourcedActor
        {
            private readonly IActorRef probe;

            public BatchActor(string id, IActorRef eventLog, IActorRef probe)
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
                    case "boom": throw new IntergrationTestException();
                    case Cmd cmd:
                        foreach (var payload in cmd.Payloads)
                        {
                            Persist(payload, result =>
                            {
                                if (result.TryGetValue(out var value)) Sender.Tell(value);
                                else Sender.Tell(result.Exception.Message);
                            });
                        }
                        return true;
                    default:
                        return false;
                }
            }

            protected override bool OnEvent(object message)
            {
                if (message is string && IsRecovering)
                {
                    probe.Tell(message);
                    return true;
                }
                else return false;
            }
        }

        class AccActor : EventsourcedActor
        {
            private readonly IActorRef probe;
            private ImmutableArray<string> acc = ImmutableArray<string>.Empty;

            public AccActor(string id, IActorRef eventLog, IActorRef probe)
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
                    case "get-acc": Sender.Tell(acc); return true;
                    case string s: 
                        Persist(s, res =>
                        {
                            if (res.IsFailure) throw res.Exception;
                        });
                        return true;
                    default: return false;
                }
            }

            protected override bool OnEvent(object message)
            {
                if (message is string s)
                {
                    acc = acc.Add(s);
                    if (acc.Length == 4) probe.Tell(acc);
                    return true;
                }
                else return false;
            }
        }

        class ConfirmedDeliveryActor : ConfirmedDelivery
        {
            private readonly IActorRef probe;

            public ConfirmedDeliveryActor(string id, IActorRef eventLog, IActorRef probe)
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
                    case "boom": throw new IntergrationTestException();
                    case "end": probe.Tell("end"); return true;
                    case "cmd-1": Persist("evt-1", _ => probe.Tell("out-1")); return true;
                    case "cmd-2": Persist("evt-2", _ => {}); return true;
                    case "cmd-2-confirm": PersistConfirmation("evt-2-confirm", "2", _ => {}); return true;
                    default: return false;
                }
            }

            protected override bool OnEvent(object message)
            {
                if (message is string s && s == "evt-2")
                {
                    Deliver("2", "out-2", probe.Path);
                    return true;
                }
                else return false;
            }
        }

        class ConditionalActor : ConditionalRequestActor
        {
            private readonly IActorRef probe;

            public ConditionalActor(string id, IActorRef eventLog, IActorRef probe)
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
                    case "persist": Persist("a", r => probe.Tell(r.Value)); return true;
                    case "persist-mute": Persist("a", _ => {}); return true;
                    default: probe.Tell(message); return true;
                }
            }

            protected override bool OnEvent(object message) => message is "a";
        }

        class ConditionalView : ConditionalRequestActor
        {
            private readonly IActorRef probe;

            public ConditionalView(string id, IActorRef eventLog, IActorRef probe)
            {
                this.probe = probe;
                Id = id;
                EventLog = eventLog;
            }

            public override string Id { get; }
            public override IActorRef EventLog { get; }
            protected override bool OnCommand(object message)
            {
                probe.Tell(message);
                return true;
            }

            protected override bool OnEvent(object message) => message is "a";
        }
        
        public readonly struct CollabCmd : IEquatable<CollabCmd>
        {
            public string To { get; }

            public CollabCmd(string to)
            {
                To = to;
            }

            public bool Equals(CollabCmd other) => string.Equals(To, other.To);
            public override bool Equals(object obj) => obj is CollabCmd other && Equals(other);
            public override int GetHashCode() => (To != null ? To.GetHashCode() : 0);
            public override string ToString() => $"CollabCmd('{To}')";
        }
        
        public readonly struct CollabEvent : IEquatable<CollabEvent>
        {
            public string To { get; }
            public string From { get; }

            public CollabEvent(string to, string from)
            {
                To = to;
                From = @from;
            }

            public override string ToString() => $"CollabEvent(to: '{To}', from: '{From}')";
            public bool Equals(CollabEvent other) => string.Equals(To, other.To) && string.Equals(From, other.From);
            public override bool Equals(object obj) => obj is CollabEvent other && Equals(other);
            public override int GetHashCode()
            {
                unchecked
                {
                    return ((To != null ? To.GetHashCode() : 0) * 397) ^ (From != null ? From.GetHashCode() : 0);
                }
            }
        }

        class CollabActor : EventsourcedActor
        {
            private readonly IActorRef probe;
            private bool initialized = false;

            public CollabActor(string id, IActorRef eventLog, IActorRef probe)
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
                    case CollabCmd cmd:
                        Persist(new CollabEvent(cmd.To, Id), _ => {});
                        return true;
                    default: return false;
                }
            }

            protected override bool OnEvent(object message)
            {
                switch (message)
                {
                    case CollabEvent e when e.To == Id:
                        if (initialized) 
                            probe.Tell(LastVectorTimestamp);
                        else
                            Self.Tell(new CollabCmd(e.From));
                        return true;
                    default: return false;
                }
            }
        }
        
        public readonly struct Route : IEquatable<Route>
        {
            public string Source { get; }
            public ImmutableHashSet<string> Destinations { get; }

            public Route(string source, ImmutableHashSet<string> destinations)
            {
                Source = source;
                Destinations = destinations;
            }

            public override string ToString() => $"Route(source: '{Source}', dest: [{string.Join(", ", Destinations)}])";
            public bool Equals(Route other) => string.Equals(Source, other.Source) && Destinations.SetEquals(other.Destinations);
            public override bool Equals(object obj) => obj is Route other && Equals(other);
            public override int GetHashCode()
            {
                unchecked
                {
                    return ((Source != null ? Source.GetHashCode() : 0) * 397) ^ (Destinations != null ? Destinations.GetHashCode() : 0);
                }
            }
        }

        class RouteActor : EventsourcedActor
        {
            private readonly IActorRef probe;

            public RouteActor(string id, string aggregateId, IActorRef eventLog, IActorRef probe)
            {
                this.probe = probe;
                Id = id;
                AggregateId = aggregateId;
                EventLog = eventLog;
            }

            public override string Id { get; }
            public override IActorRef EventLog { get; }
            public override string AggregateId { get; }

            protected override bool OnCommand(object message)
            {
                switch (message)
                {
                    case Route r:
                        Persist(r.Source, customDestinationAggregateIds: r.Destinations, handler: res => res.ThrowIfFailure());
                        return true;
                    default: return false;
                }
            }

            protected override bool OnEvent(object message)
            {
                if (message is string)
                {
                    probe.Tell(message);
                    return true;
                }
                else return false;
            }
        }

        class SnapshotActor : EventsourcedActor
        {
            private readonly IActorRef probe;
            private ImmutableArray<string> state = ImmutableArray<string>.Empty;

            public SnapshotActor(string id, IActorRef eventLog, IActorRef probe)
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
                    case "boom": throw new IntergrationTestException();
                    case "snap":
                        Save(new State(state), result =>
                        {
                            if (result.TryGetValue(out var snapshot))
                                Sender.Tell(snapshot.SequenceNr);
                            else
                                throw result.Exception;
                        });
                        return true;
                    case string s: Persist(s, r => r.ThrowIfFailure()); return true;
                    case UnhandledEvent _: Persist(default(UnhandledEvent), r => r.ThrowIfFailure()); return true;
                    default: return false;
                }
            }

            protected override bool OnEvent(object message)
            {
                if (message is string s)
                {
                    state = state.Add(s);
                    probe.Tell(state);
                    return true;
                }
                else return false;
            }

            protected override bool OnSnapshot(object message)
            {
                if (message is State s)
                {
                    this.state = s.Value;
                    probe.Tell(this.state);
                    return true;
                }
                else return false;
            }
        }

        class SnapshotView : EventsourcedView
        {
            private readonly IActorRef probe;
            private ImmutableArray<string> state = ImmutableArray<string>.Empty;

            public SnapshotView(string id, IActorRef eventLog, IActorRef probe)
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
                    case "boom": throw new IntergrationTestException();
                    case "snap":
                        Save(new State(state), res =>
                        {
                            if (res.TryGetValue(out var m)) Sender.Tell(m.SequenceNr);
                            else throw res.Exception;
                        });
                        return true;
                    default: return false;
                }
            }

            protected override bool OnEvent(object message)
            {
                switch (message)
                {
                    case string s:
                        state = state.Add($"v-{s}");
                        probe.Tell(state);
                        return true;
                    case UnhandledEvent _:
                        // in constrast to the `SnapshotActor` the `SnapshotView` actor should react on the `UnhandledEvent`
                        // so we can wait for that event during the test run
                        probe.Tell(new UnhandledEvent());
                        return true;
                    default: return false;
                }
            }

            protected override bool OnSnapshot(object message)
            {
                if (message is State s)
                {
                    state = s.Value;
                    probe.Tell(state);
                    return true;
                }
                else return false;
            }
        }

        class ChunkedReplayActor : EventsourcedActor
        {
            private readonly IActorRef probe;
            private ImmutableArray<string> state = ImmutableArray<string>.Empty;

            public ChunkedReplayActor(string id, IActorRef eventLog, IActorRef probe)
            {
                this.probe = probe;
                Id = id;
                EventLog = eventLog;
            }

            public override string Id { get; }
            public override IActorRef EventLog { get; }
            public override int ReplayBatchSize => 2;

            protected override bool OnCommand(object message)
            {
                switch (message)
                {
                    case "boom": throw new IntergrationTestException();
                    case "state": probe.Tell(state); return true;
                    case string s: Persist(s, res => res.ThrowIfFailure()); return true;
                    default: return false;
                }
            }

            protected override bool OnEvent(object message)
            {
                if (message is string s)
                {
                    state = state.Add(s);
                    return true;
                }
                else return false;
            }
        }

        #endregion
        
        private readonly TestProbe probe;
        protected abstract IActorRef Log { get; }
        
        protected EventsourcedActorIntergrationSpec(ITestOutputHelper output) : base(output: output)
        {
            probe = CreateTestProbe();
        }

        [Fact]
        public void EventsourcedActor_can_preserve_command_sender_when_invoking_persist_handler_on_success()
        {
            var actor = Sys.ActorOf(Props.Create(() => new ReplyActor("1", Log)));
            actor.Tell("reply-success", probe.Ref);
            probe.ExpectMsg("okay");
        }
        
        [Fact]
        public void EventsourcedActor_can_preserve_command_sender_when_invoking_persist_handler_on_failure()
        {
            var actor = Sys.ActorOf(Props.Create(() => new ReplyActor("1", Log)));
            actor.Tell("reply-failure", probe.Ref);
            probe.ExpectMsg("boom");
        }
        
        [Fact]
        public void EventsourcedActor_can_persist_multiple_events_per_command_as_atomic_batch()
        {
            var actor = Sys.ActorOf(Props.Create(() => new BatchActor("1", Log, probe.Ref)));
            actor.Tell(new Cmd("a", "boom", "c"), probe.Ref);

            probe.ExpectMsg("boom");
            probe.ExpectMsg("boom");
            probe.ExpectMsg("boom");
            
            actor.Tell(new Cmd("x", "y"), probe.Ref);

            probe.ExpectMsg("x");
            probe.ExpectMsg("y");
            
            actor.Tell("boom");
            
            probe.ExpectMsg("x");
            probe.ExpectMsg("y");
        }
        
        [Fact]
        public void EventsourcedActor_can_consume_events_from_other_actors_via_event_log()
        {
            var probe1 = CreateTestProbe();
            var probe2 = CreateTestProbe();
            var probe3 = CreateTestProbe();

            var actor1 = Sys.ActorOf(Props.Create(() => new AccActor("1", Log, probe1.Ref)));
            var actor2 = Sys.ActorOf(Props.Create(() => new AccActor("2", Log, probe2.Ref)));
            var actor3 = Sys.ActorOf(Props.Create(() => new AccActor("3", Log, probe3.Ref)));
            
            actor1.Tell("a");
            actor2.Tell("b");
            actor1.Tell("boom");
            actor1.Tell("c");
            actor3.Tell("d");

            var r1 = probe1.ExpectMsg<ImmutableArray<string>>();
            var r2 = probe1.ExpectMsg<ImmutableArray<string>>();
            var r3 = probe1.ExpectMsg<ImmutableArray<string>>();

            var expected = new[] {"a", "b", "c", "d"};
            
            //check content
            r1.Sort().Should().BeEquivalentTo(expected);
            r2.Sort().Should().BeEquivalentTo(expected);
            r3.Sort().Should().BeEquivalentTo(expected);
            
            // check ordering
            r1.Should().BeEquivalentTo(r2);
            r1.Should().BeEquivalentTo(r3);
        }
        
        [Fact]
        public void EventsourcedActor_can_produce_commands_to_other_actors_at_most_once()
        {
            var actor = Sys.ActorOf(Props.Create(() => new ConfirmedDeliveryActor("1", Log, probe.Ref)));
            actor.Tell("cmd-1");
            probe.ExpectMsg("out-1");
            actor.Tell("boom");
            actor.Tell("end");
            probe.ExpectMsg("end");
        }
        
        [Fact]
        public void EventsourcedActor_can_produce_commands_to_other_actors_at_least_once()
        {
            var actor = Sys.ActorOf(Props.Create(() => new ConfirmedDeliveryActor("1", Log, probe.Ref)));
            actor.Tell("cmd-2");
            probe.ExpectMsg("out-2    ");
            actor.Tell("boom");
            actor.Tell("end");
            probe.ExpectMsg("out-2");
            probe.ExpectMsg("end");
            actor.Tell("cmd-2-confirm");
            actor.Tell("boom");
            actor.Tell("end");
            probe.ExpectMsg("end");
        }
        
        [Fact]
        public void EventsourcedActor_can_route_events_to_custom_destinations()
        {
            var probe1 = CreateTestProbe();
            var probe2 = CreateTestProbe();
            var probe3 = CreateTestProbe();

            var actor1 = Sys.ActorOf(Props.Create(() => new RouteActor("1", "a1", Log, probe1.Ref)));
            var actor2 = Sys.ActorOf(Props.Create(() => new RouteActor("2", "a1", Log, probe2.Ref)));
            var actor3 = Sys.ActorOf(Props.Create(() => new RouteActor("3", "a2", Log, probe3.Ref)));
            
            actor1.Tell(new Route("x", ImmutableHashSet<string>.Empty));

            probe1.ExpectMsg("x");
            probe2.ExpectMsg("x");
            
            actor1.Tell(new Route("y", ImmutableHashSet.Create("a2")));

            probe1.ExpectMsg("y");
            probe2.ExpectMsg("y");
            probe3.ExpectMsg("y");
        }
        
        [Fact]
        public void EventsourcedActor_and_View_must_support_conditional_request_processing()
        {
            var props1 = Props.Create(() => new ConditionalActor("1", Log, probe.Ref));
            var props2 = Props.Create(() => new ConditionalActor("2", Log, probe.Ref));
            var viewProps = Props.Create(() => new ConditionalView("3", Log, probe.Ref));

            var actor1 = Sys.ActorOf(props1, "act1");
            var actor2 = Sys.ActorOf(props2, "act2");
            var view = Sys.ActorOf(viewProps, "view");
            
            var condition = new VectorTime(("Log1", 3L));
            
            view.Tell(new ConditionalRequest(condition, "delayed"));
            actor1.Tell(new ConditionalRequest(condition, "delayed-1"));
            actor2.Tell(new ConditionalRequest(condition, "delayed-2"));
            
            actor1.Tell("persist");
            actor1.Tell("persist");
            actor1.Tell("persist-mute");

            probe.ExpectMsg("a");
            probe.ExpectMsg("a");
            probe.ExpectMsgAllOf("delayed-1", "delayed-2", "delayed");
            
            // make sure that conditions are also met after recovery
            Sys.ActorOf(props1, "act1").Tell(new ConditionalRequest(condition, "delayed"));
            Sys.ActorOf(props2, "act2").Tell(new ConditionalRequest(condition, "delayed-1"));
            Sys.ActorOf(viewProps, "view").Tell(new ConditionalRequest(condition, "delayed-2"));
            
            probe.ExpectMsgAllOf("delayed-1", "delayed-2", "delayed");
        }
        
        [Fact]
        public void EventsourcedActor_and_View_must_support_snapshots()
        {
            var actorProbe = CreateTestProbe();
            var viewProbe = CreateTestProbe();

            var actor = Sys.ActorOf(Props.Create(() => new SnapshotActor("1", Log, actorProbe.Ref)));
            var view = Sys.ActorOf(Props.Create(() => new SnapshotView("2", Log, viewProbe.Ref)));
            
            actor.Tell("a");
            actor.Tell("b");
            actor.Tell(new UnhandledEvent());
            actor.Tell(new UnhandledEvent());

            actorProbe.ExpectMsg<ImmutableArray<string>>().Should().BeEquivalentTo("a");
            actorProbe.ExpectMsg<ImmutableArray<string>>().Should().BeEquivalentTo("a", "b");

            viewProbe.ExpectMsg<ImmutableArray<string>>().Should().BeEquivalentTo("v-a");
            viewProbe.ExpectMsg<ImmutableArray<string>>().Should().BeEquivalentTo("v-a", "v-b");
            viewProbe.ExpectMsg<UnhandledEvent>();
            viewProbe.ExpectMsg<UnhandledEvent>();

            actor.Tell("snap", actorProbe.Ref);
            actor.Tell("snap", viewProbe.Ref);
            
            // although the `SnapshotActor` handled only events up to sequence number 2
            // we expect the snapshot to be saved until all 4 'processed' events
            actorProbe.ExpectMsg(4L);
            viewProbe.ExpectMsg(4L);
            
            actor.Tell("c");
            
            actorProbe.ExpectMsg<ImmutableArray<string>>().Should().BeEquivalentTo("a", "b", "c");
            viewProbe.ExpectMsg<ImmutableArray<string>>().Should().BeEquivalentTo("v-a", "v-b", "v-c");
            
            actor.Tell("boom");
            view.Tell("boom");
            
            actorProbe.ExpectMsg<ImmutableArray<string>>().Should().BeEquivalentTo("a", "b");
            actorProbe.ExpectMsg<ImmutableArray<string>>().Should().BeEquivalentTo("a", "b", "c");
            
            viewProbe.ExpectMsg<ImmutableArray<string>>().Should().BeEquivalentTo("v-a", "v-b");
            viewProbe.ExpectMsg<ImmutableArray<string>>().Should().BeEquivalentTo("v-a", "v-b", "v-c");
        }
        
        [Fact]
        public void EventsourcedActor_and_View_must_support_batch_event_replay()
        {
            var actor = Sys.ActorOf(Props.Create(() => new ChunkedReplayActor("1", Log, probe.Ref)));
            var messages = Enumerable.Range(1, 10).Select(i => $"m-{i}").ToImmutableArray();

            foreach (var message in messages)
                actor.Tell(message);
            
            actor.Tell("state");
            probe.ExpectMsg<ImmutableArray<string>>().Should().BeEquivalentTo(messages);
            
            actor.Tell("boom");
            actor.Tell("state");
            probe.ExpectMsg<ImmutableArray<string>>().Should().BeEquivalentTo(messages);
        }
    }

    internal class IntergrationTestException : Exception
    {
    }
}