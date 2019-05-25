#region copyright
// -----------------------------------------------------------------------
//  <copyright file="EventsourcedActorSpec.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Collections.Immutable;
using System.Linq;
using Akka.Actor;
using Akka.Configuration;
using Akka.TestKit;
using Akka.TestKit.Xunit2;
using Eventuate.EventsourcingProtocol;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Eventuate.Tests
{
    using static EventsourcedViewSpec;
    public class EventsourcedActorSpec : TestKit
    {
        #region internal classes

        internal readonly struct Cmd
        {
            public object Payload { get; }
            public int Num { get; }

            public Cmd(object payload, int num = 1)
            {
                Payload = payload;
                Num = num;
            }
        }

        internal readonly struct Deliver
        {
            public object Payload { get; }

            public Deliver(object payload)
            {
                Payload = payload;
            }
        }

        internal readonly struct DeliverRequested : IEquatable<DeliverRequested>
        {
            public object Payload { get; }

            public DeliverRequested(object payload)
            {
                Payload = payload;
            }

            public bool Equals(DeliverRequested other) => Equals(Payload, other.Payload);

            public override bool Equals(object obj) => obj is DeliverRequested other && Equals(other);

            public override int GetHashCode() => (Payload != null ? Payload.GetHashCode() : 0);
        }

        internal readonly struct State : IEquatable<State>
        {
            public ImmutableArray<string> Value { get; }

            public State(params string[] values)
            {
                Value = values.ToImmutableArray();
            }
            
            public State(ImmutableArray<string> value)
            {
                Value = value;
            }

            public bool Equals(State other)
            {
                if (Value.Length != other.Value.Length) return false;
                for (int i = 0; i < Value.Length; i++)
                {
                    if (Value[i] != other.Value[i]) return false;
                }

                return true;
            }

            public override bool Equals(object obj) => obj is State other && Equals(other);

            public override int GetHashCode()
            {
                unchecked
                {
                    var hash = 0;
                    foreach (var item in Value)
                        hash = (397 * hash) ^ item.GetHashCode();
                    return hash;
                }
            }

            public override string ToString() => $"State({string.Join(", ", this.Value)})";
        }

        internal sealed class TestEventsourcedActor : EventsourcedActor
        {
            private readonly IActorRef cmdProbe;
            private readonly IActorRef evtProbe;

            public TestEventsourcedActor(IActorRef logProbe, IActorRef cmdProbe, IActorRef evtProbe, bool stateSync)
            {
                this.EventLog = logProbe;
                this.cmdProbe = cmdProbe;
                this.evtProbe = evtProbe;
                this.StateSync = stateSync;
            }

            public override bool StateSync { get; }
            public override string Id => "A";
            public override IActorRef EventLog { get; }

            protected override bool OnCommand(object message)
            {
                switch (message)
                {
                    case "boom": throw TestException.Instance;
                    case "status":
                        cmdProbe.Tell(("status", LastVectorTimestamp, CurrentVersion, LastSequenceNr));
                        return true;
                    case Ping ping:
                        cmdProbe.Tell(new Pong(ping.I));
                        return true;
                    case "test-handler-order":
                        Persist("a",
                            r => cmdProbe.Tell(($"{r.Value}-1", LastVectorTimestamp, CurrentVersion, LastSequenceNr)));
                        Persist("b",
                            r => cmdProbe.Tell(($"{r.Value}-2", LastVectorTimestamp, CurrentVersion, LastSequenceNr)));
                        return true;
                    case "test-multi-persist":
                        Action<Try<string>> handler = attempt =>
                            cmdProbe.Tell((attempt.Value, CurrentVersion, LastVectorTimestamp, LastSequenceNr));
                        PersistMany(new[] {"a", "b", "c"}, handler, handler);
                        return true;
                    case Cmd cmd:
                        for (int i = 1; i <= cmd.Num; i++)
                        {
                            Persist($"{cmd.Payload}-{i}", r =>
                            {
                                if (r.IsFailure)
                                    cmdProbe.Tell((r.Exception, LastVectorTimestamp, CurrentVersion, LastSequenceNr));
                            });
                        }

                        return true;
                    default: return false;
                }
            }

            protected override bool OnEvent(object message)
            {
                switch (message)
                {
                    case "boom": throw TestException.Instance;
                    case "x": return false;
                    case string s:
                        evtProbe.Tell((s, LastVectorTimestamp, CurrentVersion, LastSequenceNr));
                        return true;
                    default: return false;
                }
            }

            protected override void Unhandled(object message)
            {
                if (message is string) cmdProbe.Tell(message);
                else base.Unhandled(message);
            }
        }

        internal sealed class TestStashingActor : EventsourcedActor
        {
            private readonly IActorRef msgProbe;
            private bool stashing = false;

            public TestStashingActor(IActorRef logProbe, IActorRef msgProbe, bool stateSync)
            {
                this.msgProbe = msgProbe;
                this.EventLog = logProbe;
                this.StateSync = stateSync;
            }

            public override bool StateSync { get; }
            public override string Id => "A";
            public override IActorRef EventLog { get; }

            protected override bool OnCommand(object message)
            {
                switch (message)
                {
                    case "boom": throw TestException.Instance;
                    case "stash-on":
                        stashing = true;
                        return true;
                    case "stash-off":
                        stashing = false;
                        return true;
                    case "unstash":
                        UnstashAll();
                        return true;
                    case Ping _ when stashing:
                        StashCommand();
                        return true;
                    case Ping ping:
                        msgProbe.Tell(new Pong(ping.I));
                        return true;
                    case Cmd cmd:
                        Persist(cmd.Payload, r =>
                        {
                            if (r.IsFailure) msgProbe.Tell(r.Exception);
                        });
                        return true;
                    default: return false;
                }
            }

            protected override bool OnEvent(object message)
            {
                switch (message)
                {
                    case "unstash":
                        UnstashAll();
                        return true;
                    default:
                        msgProbe.Tell(message);
                        return true;
                }
            }
        }

        internal sealed class TestSnapshotActor : ConfirmedDelivery
        {
            private readonly IActorRef cmdProbe;
            private readonly IActorRef evtProbe;
            private ImmutableArray<string> state = ImmutableArray<string>.Empty;

            public TestSnapshotActor(IActorRef eventLog, IActorRef cmdProbe, IActorRef evtProbe)
            {
                this.cmdProbe = cmdProbe;
                this.evtProbe = evtProbe;
                this.EventLog = eventLog;
            }

            public override string Id => "A";
            public override IActorRef EventLog { get; }

            protected override bool OnCommand(object message)
            {
                switch (message)
                {
                    case "boom": throw TestException.Instance;
                    case "snap":
                        Save(new State(state), r =>
                        {
                            Context.System.Log.Warning("WTF");
                            if (r.IsSuccess) cmdProbe.Tell(r.Value);
                            else cmdProbe.Tell(r.Exception);
                        });
                        return true;
                    case Cmd cmd:
                        Persist(cmd.Payload, r =>
                        {
                            if (r.IsFailure) cmdProbe.Tell(r.Exception);
                        });
                        return true;
                    case Deliver deliver:
                        Persist(new DeliverRequested(deliver.Payload), r =>
                        {
                            if (r.IsFailure) cmdProbe.Tell(r.Exception);
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
                        this.state = this.state.Add(s);
                        evtProbe.Tell((new State(this.state), LastVectorTimestamp, CurrentVersion, LastSequenceNr));
                        return true;
                    case DeliverRequested r:
                        Deliver(LastSequenceNr.ToString(), Message(r.Payload), cmdProbe.Path);
                        return true;
                    default: return false;
                }
            }

            protected override bool OnSnapshot(object message)
            {
                if (message is State s)
                {
                    this.state = s.Value;
                    evtProbe.Tell((new State(state), LastVectorTimestamp, CurrentVersion, LastSequenceNr));
                    return true;
                }
                else return false;
            }

            private object Message(object payload) => (payload, LastVectorTimestamp, CurrentVersion, LastSequenceNr);
        }

        #endregion

        private static readonly Config
            TestConfig = ConfigurationFactory.ParseString(@"
                eventuate.log.write-timeout = 1s
                akka.loglevel = DEBUG");

        private static readonly TimeSpan Timeout = TimeSpan.FromMilliseconds(200);

        private readonly int instanceId;
        private readonly TestProbe logProbe;
        private readonly TestProbe cmdProbe;
        private readonly TestProbe evtProbe;

        public EventsourcedActorSpec(ITestOutputHelper output) : base(config: TestConfig, output: output)
        {
            this.instanceId = EventsourcedView.InstanceIdCounter.Current;
            this.logProbe = CreateTestProbe();
            this.cmdProbe = CreateTestProbe();
            this.evtProbe = CreateTestProbe();
        }

        private IActorRef UnrecoveredEventsourcedActor(bool stateSync = true) =>
            Sys.ActorOf(Props.Create(() =>
                new TestEventsourcedActor(logProbe.Ref, cmdProbe.Ref, evtProbe.Ref, stateSync)));

        private IActorRef UnrecoveredSnapshotActor() =>
            Sys.ActorOf(Props.Create(() => new TestSnapshotActor(logProbe.Ref, cmdProbe.Ref, evtProbe.Ref)));

        private IActorRef RecoveredEventsourcedActor(bool stateSync = true) =>
            ProcessRecover(UnrecoveredEventsourcedActor(stateSync));

        private IActorRef RecoveredSnapshotActor() => ProcessRecover(UnrecoveredSnapshotActor());

        private IActorRef RecoveredStashingActor(IActorRef probe, bool stateSync) =>
            ProcessRecover(Sys.ActorOf(Props.Create(() => new TestStashingActor(logProbe.Ref, probe, stateSync))));

        private IActorRef ProcessRecover(IActorRef actor)
        {
            logProbe.ExpectMsg(new LoadSnapshot("A", instanceId));
            logProbe.Sender.Tell(new LoadSnapshotSuccess(null, instanceId));
            logProbe.ExpectMsg(new Replay(actor, instanceId, 1));
            logProbe.Sender.Tell(new ReplaySuccess(Array.Empty<DurableEvent>(), 0, instanceId));
            return actor;
        }

        private void ProcessWrite(long sequenceNr)
        {
            var write = logProbe.ExpectMsg<Write>();
            logProbe.Sender.Tell(new WriteSuccess(new[] {Event(write.Events.First().Payload, sequenceNr)},
                write.CorrelationId, instanceId));
        }

        [Fact]
        public void EventsourcedActor_in_stateSync_mode_must_stash_further_commands_while_persistence_is_in_progress()
        {
            var actor = RecoveredEventsourcedActor(stateSync: true);
            actor.Tell(new Cmd("a", 2));
            actor.Tell(new Ping(1));
            actor.Tell(new Ping(2));

            var write = logProbe.ExpectMsg<Write>();
            write.Events.First().Payload.Should().Be("a-1");
            write.Events.ElementAt(1).Payload.Should().Be("a-2");
            logProbe.Sender.Tell(new WriteSuccess(new[] {Event("a-1", 1), Event("a-2", 2)}, write.CorrelationId,
                instanceId));

            evtProbe.ExpectMsg(("a-1", Timestamp(1), Timestamp(1), 1L));
            evtProbe.ExpectMsg(("a-2", Timestamp(2), Timestamp(2), 2L));
            cmdProbe.ExpectMsg(new Pong(1));
            cmdProbe.ExpectMsg(new Pong(2));
        }

        [Fact]
        public void
            EventsourcedActor_in_stateSync_mode_must_process_further_commands_if_persist_is_aborted_by_exception_in_persist_handler()
        {
            var actor = RecoveredEventsourcedActor(stateSync: true);
            actor.Tell(new Cmd("a", 2));
            actor.Tell(new Cmd("b", 2));

            var write1 = logProbe.ExpectMsg<Write>();
            logProbe.Sender.Tell(new WriteSuccess(new[] {Event("boom", 1), Event("a-2", 2)}, write1.CorrelationId,
                instanceId));

            var next = instanceId + 1;
            logProbe.ExpectMsg(new LoadSnapshot(EmitterIdA, next));
            logProbe.Sender.Tell(new LoadSnapshotSuccess(null, next));
            logProbe.ExpectMsg(new Replay(actor, next, 1));
            logProbe.Sender.Tell(new ReplaySuccess(new[] {Event("a-1", 1), Event("a-2", 2)}, 2, next));
            logProbe.ExpectMsg(new Replay(null, next, 3));
            logProbe.Sender.Tell(new ReplaySuccess(Array.Empty<DurableEvent>(), 2, next));

            var write2 = logProbe.ExpectMsg<Write>();
            write2.Events.First().Payload.Should().Be("b-1");
            write2.Events.ElementAt(1).Payload.Should().Be("b-2");
            logProbe.Sender.Tell(new WriteSuccess(new[] {Event("b-1", 3), Event("b-2", 4)}, write2.CorrelationId,
                next));

            evtProbe.ExpectMsg(("a-1", Timestamp(1), Timestamp(1), 1L));
            evtProbe.ExpectMsg(("a-2", Timestamp(2), Timestamp(2), 2L));
            evtProbe.ExpectMsg(("b-1", Timestamp(3), Timestamp(3), 3L));
            evtProbe.ExpectMsg(("b-2", Timestamp(4), Timestamp(4), 4L));
        }

        [Fact]
        public void EventsourcedActor_in_stateSync_mode_must_fix_182()
        {
            var probe = CreateTestProbe();
            var actor = RecoveredStashingActor(probe.Ref, stateSync: true);

            actor.Tell(new Cmd("c"));
            actor.Tell("stash-on");
            actor.Tell(new Ping(1));
            actor.Tell("stash-off");
            actor.Tell(new Ping(2));
            actor.Tell("unstash");
            actor.Tell(new Ping(3));

            ProcessWrite(1);

            probe.ExpectMsg("c");
            probe.ExpectMsg(new Pong(2));
            probe.ExpectMsg(new Pong(1));
            probe.ExpectMsg(new Pong(3));
        }

        [Fact]
        public void
            EventsourcedActor_in_stateSync_mode_must_support_user_stash_unstash_operations_that_are_within_write()
        {
            var probe = CreateTestProbe();
            var actor = RecoveredStashingActor(probe.Ref, stateSync: true);

            actor.Tell(new Cmd("a"));
            actor.Tell("stash-on");
            actor.Tell(new Ping(1));
            actor.Tell("stash-off");
            actor.Tell(new Ping(2));
            actor.Tell("unstash");

            ProcessWrite(1);

            actor.Tell(new Cmd("b"));

            ProcessWrite(1);

            probe.ExpectMsg("a");
            probe.ExpectMsg(new Pong(2));
            probe.ExpectMsg(new Pong(1));
            probe.ExpectMsg("b");
        }

        [Fact]
        public void
            EventsourcedActor_in_stateSync_mode_must_support_repeated_user_stash_unstash_operations_that_are_within_write()
        {
            var probe = CreateTestProbe();
            var actor = RecoveredStashingActor(probe.Ref, stateSync: true);

            actor.Tell(new Cmd("a"));
            actor.Tell("stash-on");
            actor.Tell(new Ping(1));
            actor.Tell("stash-off");
            actor.Tell(new Ping(2));
            actor.Tell("unstash");
            actor.Tell(new Ping(3));
            actor.Tell("stash-on");
            actor.Tell(new Ping(4));
            actor.Tell("stash-off");
            actor.Tell(new Ping(5));
            actor.Tell("unstash");

            ProcessWrite(1);

            actor.Tell(new Cmd("b"));

            ProcessWrite(2);

            probe.ExpectMsg("a");
            probe.ExpectMsg(new Pong(2));
            probe.ExpectMsg(new Pong(1));
            probe.ExpectMsg(new Pong(3));
            probe.ExpectMsg(new Pong(5));
            probe.ExpectMsg(new Pong(4));
            probe.ExpectMsg("b");
        }

        [Fact]
        public void
            EventsourcedActor_in_stateSync_mode_must_support_user_stash_unstash_operations_that_overlap_with_write()
        {
            var probe = CreateTestProbe();
            var actor = RecoveredStashingActor(probe.Ref, stateSync: true);

            actor.Tell(new Cmd("a"));
            actor.Tell("stash-on");
            actor.Tell(new Ping(1));
            actor.Tell("stash-off");
            actor.Tell(new Ping(2));

            ProcessWrite(1);

            actor.Tell("unstash");
            actor.Tell(new Cmd("b"));

            ProcessWrite(2);

            probe.ExpectMsg("a");
            probe.ExpectMsg(new Pong(2));
            probe.ExpectMsg(new Pong(1));
            probe.ExpectMsg("b");
        }

        [Fact]
        public void EventsourcedActor_in_stateSync_mode_must_support_user_stash_unstash_operations_that_span_writes()
        {
            var probe = CreateTestProbe();
            var actor = RecoveredStashingActor(probe.Ref, stateSync: true);

            actor.Tell(new Cmd("a"));
            actor.Tell("stash-on");
            actor.Tell(new Ping(1));
            actor.Tell("stash-off");
            actor.Tell(new Ping(2));

            ProcessWrite(1);

            actor.Tell(new Cmd("b"));
            actor.Tell("unstash");

            ProcessWrite(2);

            probe.ExpectMsg("a");
            probe.ExpectMsg(new Pong(2));
            probe.ExpectMsg("b");
            probe.ExpectMsg(new Pong(1));
        }

        [Fact]
        public void EventsourcedActor_in_stateSync_mode_must_support_user_unstash_operations_in_event_handler()
        {
            var probe = CreateTestProbe();
            var actor = RecoveredStashingActor(probe.Ref, stateSync: true);

            actor.Tell(new Cmd("a"));
            actor.Tell("stash-on");
            actor.Tell(new Ping(1));
            actor.Tell("stash-off");
            actor.Tell(new Ping(2));

            ProcessWrite(1);

            actor.Tell(new Cmd("unstash"));

            ProcessWrite(2);

            actor.Tell(new Cmd("b"));

            ProcessWrite(3);

            probe.ExpectMsg("a");
            probe.ExpectMsg(new Pong(2));
            probe.ExpectMsg(new Pong(1));
            probe.ExpectMsg("b");
        }

        [Fact]
        public void
            EventsourcedActor_in_stateSync_mode_must_support_user_stash_unstash_operations_that_are_within_write_where_unstash_is_the_last_operation()
        {
            var probe = CreateTestProbe();
            var actor = RecoveredStashingActor(probe.Ref, stateSync: true);

            actor.Tell(new Cmd("a"));
            actor.Tell("stash-on");
            actor.Tell(new Ping(1));
            actor.Tell("stash-off");
            actor.Tell(new Ping(2));
            actor.Tell("unstash");

            ProcessWrite(1);

            probe.ExpectMsg("a");
            probe.ExpectMsg(new Pong(2));
            probe.ExpectMsg(new Pong(1));
        }

        [Fact]
        public void
            EventsourcedActor_in_stateSync_mode_must_support_user_stash_unstash_operations_that_are_outside_write_where_unstash_is_the_last_operation()
        {
            var probe = CreateTestProbe();
            var actor = RecoveredStashingActor(probe.Ref, stateSync: true);

            actor.Tell(new Ping(1));
            actor.Tell("stash-on");
            actor.Tell(new Ping(2));
            actor.Tell("stash-off");
            actor.Tell(new Ping(3));
            actor.Tell("unstash");

            probe.ExpectMsg(new Pong(1));
            probe.ExpectMsg(new Pong(3));
            probe.ExpectMsg(new Pong(2));
        }

        [Fact]
        public void EventsourcedActor_in_stateSync_mode_must_support_user_stash_operations_under_failure_conditions()
        {
            var probe = CreateTestProbe();
            var actor = RecoveredStashingActor(probe.Ref, stateSync: true);

            actor.Tell(new Cmd("a"));
            actor.Tell("stash-on");
            actor.Tell(new Ping(1));
            actor.Tell("stash-off");
            actor.Tell("boom");
            actor.Tell(new Ping(2));

            ProcessWrite(1);
            probe.ExpectMsg("a");

            var next = instanceId + 1;
            logProbe.ExpectMsg(new LoadSnapshot("A", next));
            logProbe.Sender.Tell(new LoadSnapshotSuccess(null, next));
            logProbe.ExpectMsg(new Replay(actor, next, 1));
            logProbe.Sender.Tell(new ReplaySuccess(new[] {Event("a", 1)}, 1, next));
            logProbe.ExpectMsg(new Replay(null, next, 2));
            logProbe.Sender.Tell(new ReplaySuccess(Array.Empty<DurableEvent>(), 1, next));

            probe.ExpectMsg("a");
            probe.ExpectMsg(new Pong(1));
            probe.ExpectMsg(new Pong(2));
        }

        [Fact]
        public void EventsourcedActor_in_stateSync_mode_must_stop_during_write_if_its_event_log_is_stopped()
        {
            var actor = Watch(RecoveredEventsourcedActor(stateSync: true));
            actor.Tell(new Cmd("a"));
            logProbe.ExpectMsg<Write>();
            Sys.Stop(logProbe.Ref);
            ExpectTerminated(actor);
        }

        [Fact]
        public void EventsourcedActor_must_process_further_commands_while_persistence_is_in_progress()
        {
            var actor = RecoveredEventsourcedActor(stateSync: false);
            actor.Tell(new Cmd("a", 2));
            actor.Tell(new Ping(1));
            var write = logProbe.ExpectMsg<Write>();
            write.Events.ElementAt(0).Payload.Should().Be("a-1");
            write.Events.ElementAt(1).Payload.Should().Be("a-2");
            logProbe.Sender.Tell(new WriteSuccess(new[] {Event("a-1", 1), Event("a-2", 2)}, write.CorrelationId,
                instanceId));

            cmdProbe.ExpectMsg(new Pong(1));
            evtProbe.ExpectMsg(("a-1", Timestamp(1), Timestamp(1), 1L));
            evtProbe.ExpectMsg(("a-2", Timestamp(2), Timestamp(2), 2L));
        }

        [Fact]
        public void
            EventsourcedActor_must_process_further_commands_if_persist_is_aborted_by_exception_in_command_handler()
        {
            var actor = RecoveredEventsourcedActor(stateSync: false);
            actor.Tell(new Cmd("a", 2));
            actor.Tell("boom");
            actor.Tell(new Cmd("b", 2));

            var write1 = logProbe.ExpectMsg<Write>();
            logProbe.Sender.Tell(new WriteSuccess(new[] {Event("boom", 1), Event("a-2", 2)}, write1.CorrelationId,
                instanceId));

            var next = instanceId + 1;
            logProbe.ExpectMsg(new LoadSnapshot("A", next));
            logProbe.Sender.Tell(new LoadSnapshotSuccess(null, next));
            logProbe.ExpectMsg(new Replay(actor, next, 1));
            logProbe.Sender.Tell(new ReplaySuccess(new[] {Event("a-1", 1), Event("a-2", 2)}, 2, next));
            logProbe.ExpectMsg(new Replay(null, next, 3));
            logProbe.Sender.Tell(new ReplaySuccess(Array.Empty<DurableEvent>(), 2, next));

            var write2 = logProbe.ExpectMsg<Write>();
            write2.Events.ElementAt(0).Payload.Should().Be("b-1");
            write2.Events.ElementAt(1).Payload.Should().Be("b-2");
            logProbe.Sender.Tell(new WriteSuccess(new[] {Event("b-1", 3), Event("b-2", 4)}, write2.CorrelationId,
                next));

            evtProbe.ExpectMsg(("a-1", Timestamp(1), Timestamp(1), 1L));
            evtProbe.ExpectMsg(("a-2", Timestamp(2), Timestamp(2), 2L));
            evtProbe.ExpectMsg(("b-1", Timestamp(3), Timestamp(3), 3L));
            evtProbe.ExpectMsg(("b-2", Timestamp(4), Timestamp(4), 4L));
        }

        [Fact]
        public void EventsourcedActor_must_fix_182()
        {
            var probe = CreateTestProbe();
            var actor = RecoveredStashingActor(probe.Ref, stateSync: false);

            actor.Tell(new Cmd("c"));
            actor.Tell("stash-on");
            actor.Tell(new Ping(1));
            actor.Tell("stash-off");
            actor.Tell(new Ping(2));
            actor.Tell("unstash");
            actor.Tell(new Ping(3));

            ProcessWrite(1);

            probe.ExpectMsg(new Pong(2));
            probe.ExpectMsg(new Pong(1));
            probe.ExpectMsg(new Pong(3));
            probe.ExpectMsg("c");
        }

        [Fact]
        public void EventsourcedActor_must_support_user_stash_unstash_operations_that_are_within_write()
        {
            var probe = CreateTestProbe();
            var actor = RecoveredStashingActor(probe.Ref, stateSync: false);

            actor.Tell(new Cmd("a"));
            actor.Tell("stash-on");
            actor.Tell(new Ping(1));
            actor.Tell("stash-off");
            actor.Tell(new Ping(2));
            actor.Tell("unstash");

            ProcessWrite(1);

            actor.Tell(new Cmd("b"));

            ProcessWrite(2);

            probe.ExpectMsg(new Pong(2));
            probe.ExpectMsg(new Pong(1));
            probe.ExpectMsg("a");
            probe.ExpectMsg("b");
        }

        [Fact]
        public void EventsourcedActor_must_support_user_stash_unstash_operations_that_overlap_with_write()
        {
            var probe = CreateTestProbe();
            var actor = RecoveredStashingActor(probe.Ref, stateSync: false);

            actor.Tell(new Cmd("a"));
            actor.Tell("stash-on");
            actor.Tell(new Ping(1));
            actor.Tell("stash-off");
            actor.Tell(new Ping(2));

            ProcessWrite(1);

            actor.Tell("unstash");
            actor.Tell(new Cmd("b"));

            ProcessWrite(2);

            probe.ExpectMsg(new Pong(2));
            probe.ExpectMsg("a");
            probe.ExpectMsg(new Pong(1));
            probe.ExpectMsg("b");
        }

        [Fact]
        public void EventsourcedActor_must_support_user_stash_unstash_operations_that_span_writes()
        {
            var probe = CreateTestProbe();
            var actor = RecoveredStashingActor(probe.Ref, stateSync: false);

            actor.Tell(new Cmd("a"));
            actor.Tell("stash-on");
            actor.Tell(new Ping(1));
            actor.Tell("stash-off");
            actor.Tell(new Ping(2));

            ProcessWrite(1);

            actor.Tell(new Cmd("b"));
            actor.Tell("unstash");

            ProcessWrite(2);

            probe.ExpectMsg(new Pong(2));
            probe.ExpectMsg("a");
            probe.ExpectMsg(new Pong(1));
            probe.ExpectMsg("b");
        }

        [Fact]
        public void EventsourcedActor_must_support_user_unstash_operations_in_event_handler()
        {
            var probe = CreateTestProbe();
            var actor = RecoveredStashingActor(probe.Ref, stateSync: false);

            actor.Tell(new Cmd("a"));
            actor.Tell("stash-on");
            actor.Tell(new Ping(1));
            actor.Tell("stash-off");
            actor.Tell(new Ping(2));

            ProcessWrite(1);

            actor.Tell(new Cmd("unstash"));

            ProcessWrite(2);

            actor.Tell(new Cmd("b"));

            ProcessWrite(3);

            probe.ExpectMsg(new Pong(2));
            probe.ExpectMsg("a");
            probe.ExpectMsg(new Pong(1));
            probe.ExpectMsg("b");
        }

        [Fact]
        public void EventsourcedActor_must_support_user_stash_unstash_operations_where_unstash_is_the_last_operation()
        {
            var probe = CreateTestProbe();
            var actor = RecoveredStashingActor(probe.Ref, stateSync: false);

            actor.Tell(new Cmd("a"));
            actor.Tell("stash-on");
            actor.Tell(new Ping(1));
            actor.Tell("stash-off");
            actor.Tell(new Ping(2));
            actor.Tell("unstash");

            probe.ExpectMsg(new Pong(2));
            probe.ExpectMsg(new Pong(1));
        }

        [Fact]
        public void EventsourcedActor_must_support_user_stash_operations_under_failure_conditions()
        {
            var probe = CreateTestProbe();
            var actor = RecoveredStashingActor(probe.Ref, stateSync: false);

            actor.Tell(new Cmd("a"));
            actor.Tell("stash-on");
            actor.Tell(new Ping(1));
            actor.Tell("stash-off");
            actor.Tell("boom");
            actor.Tell(new Ping(2));

            ProcessWrite(1); // ignored

            var next = instanceId + 1;
            logProbe.ExpectMsg(new LoadSnapshot("A", next));
            logProbe.Sender.Tell(new LoadSnapshotSuccess(null, next));
            logProbe.ExpectMsg(new Replay(actor, next, 1));
            logProbe.Sender.Tell(new ReplaySuccess(new[] {Event("a", 1)}, 1, next));
            logProbe.ExpectMsg(new Replay(null, next, 2));
            logProbe.Sender.Tell(new ReplaySuccess(Array.Empty<DurableEvent>(), 1, next));

            probe.ExpectMsg("a"); // from replay
            probe.ExpectMsg(new Pong(1));
            probe.ExpectMsg(new Pong(2));
        }

        [Fact]
        public void EventsourcedActor_in_any_mode_must_handle_remote_events_while_persistence_is_in_progress()
        {
            var actor = RecoveredEventsourcedActor(stateSync: true);
            actor.Tell(new Cmd("a", 2));
            var write = logProbe.ExpectMsg<Write>();
            write.Events.ElementAt(0).Payload.Should().Be("a-1");
            write.Events.ElementAt(1).Payload.Should().Be("a-2");

            var eventB1 = new DurableEvent("b-1", EmitterIdB, null, ImmutableHashSet<string>.Empty,
                default, Timestamp(0, 1), LogIdB, LogIdA, 1L);
            var eventB2 = new DurableEvent("b-2", EmitterIdB, null, ImmutableHashSet<string>.Empty, 
                default, Timestamp(0, 2), LogIdB, LogIdA, 2);

            var eventA1 = new DurableEvent("a-1", EmitterIdA, null, ImmutableHashSet<string>.Empty, 
                default, Timestamp(3, 0), LogIdA, LogIdA, 3);
            var eventA2 = new DurableEvent("a-2", EmitterIdA, null, ImmutableHashSet<string>.Empty, 
                default, Timestamp(4, 0), LogIdA, LogIdA, 4);

            actor.Tell(new Written(eventB1));
            actor.Tell(new Written(eventB2));
            logProbe.Sender.Tell(new WriteSuccess(new[] {eventA1, eventA2}, write.CorrelationId, instanceId));

            evtProbe.ExpectMsg(("b-1", Timestamp(0, 1), Timestamp(0, 1), 1L));
            evtProbe.ExpectMsg(("b-2", Timestamp(0, 2), Timestamp(0, 2), 2L));
            evtProbe.ExpectMsg(("a-1", Timestamp(3, 0), Timestamp(3, 2), 3L));
            evtProbe.ExpectMsg(("a-2", Timestamp(4, 0), Timestamp(4, 2), 4L));
        }

        [Fact]
        public void EventsourcedActor_in_any_mode_must_invoke_persist_handler_in_correct_order()
        {
            var actor = RecoveredEventsourcedActor(stateSync: true);
            actor.Tell("test-handler-order");

            var write = logProbe.ExpectMsg<Write>();
            write.Events.ElementAt(0).Payload.Should().Be("a");
            write.Events.ElementAt(1).Payload.Should().Be("b");
            logProbe.Sender.Tell(
                new WriteSuccess(new[] {Event("a", 1), Event("b", 2)}, write.CorrelationId, instanceId));

            evtProbe.ExpectMsg(("a", Timestamp(1), Timestamp(1), 1L));
            cmdProbe.ExpectMsg(("a-1", Timestamp(1), Timestamp(1), 1L));

            evtProbe.ExpectMsg(("b", Timestamp(2), Timestamp(2), 2L));
            cmdProbe.ExpectMsg(("b-2", Timestamp(2), Timestamp(2), 2L));
        }

        [Fact]
        public void EventsourcedActor_in_any_mode_must_additionally_invoke_onLast_handler_for_multi_persist()
        {
            var actor = RecoveredEventsourcedActor(stateSync: true);
            actor.Tell("test-multi-persist");

            var write = logProbe.ExpectMsg<Write>();
            write.Events.ElementAt(0).Payload.Should().Be("a");
            write.Events.ElementAt(1).Payload.Should().Be("b");
            write.Events.ElementAt(2).Payload.Should().Be("c");
            logProbe.Sender.Tell(new WriteSuccess(new []{Event("a", 1), Event("b", 2), Event("c", 3)}, write.CorrelationId, instanceId));

            evtProbe.ExpectMsg(("a", Timestamp(1), Timestamp(1), 1L));
            cmdProbe.ExpectMsg(("a", Timestamp(1), Timestamp(1), 1L));
            
            evtProbe.ExpectMsg(("b", Timestamp(2), Timestamp(2), 2L));
            cmdProbe.ExpectMsg(("b", Timestamp(2), Timestamp(2), 2L));
            
            evtProbe.ExpectMsg(("c", Timestamp(3), Timestamp(3), 3L));
            cmdProbe.ExpectMsg(("c", Timestamp(3), Timestamp(3), 3L));
            cmdProbe.ExpectMsg(("c", Timestamp(3), Timestamp(3), 3L));
        }

        [Fact]
        public void EventsourcedActor_in_any_mode_must_report_failed_writes_to_persist_handler()
        {
            var actor = RecoveredEventsourcedActor(stateSync: true);
            actor.Tell(new Cmd("a", 2));

            var write = logProbe.ExpectMsg<Write>();
            var event1 = write.Events.ElementAt(0);
            var event2 = write.Events.ElementAt(1);
            logProbe.Sender.Tell(new WriteFailure(new []{event1, event2}, TestException.Instance, write.CorrelationId, instanceId));

            cmdProbe.ExpectMsg(((Exception)TestException.Instance, event1.VectorTimestamp, event1.VectorTimestamp, event1.LocalSequenceNr));
            cmdProbe.ExpectMsg(((Exception)TestException.Instance, event2.VectorTimestamp, event2.VectorTimestamp, event2.LocalSequenceNr));
        }

        [Fact]
        public void EventsourcedActor_in_any_mode_must_not_send_empty_write_commands_to_log()
        {
            var actor = RecoveredEventsourcedActor(stateSync: true);
            actor.Tell(new Ping(1));
            actor.Tell(new Cmd("a", 2));
            var write = logProbe.ExpectMsg<Write>();
            write.Events.ElementAt(0).Payload.Should().Be("a-1");
            write.Events.ElementAt(1).Payload.Should().Be("a-2");
        }

        [Fact]
        public void EventsourcedActor_in_any_mode_must_not_update_clock_if_event_is_not_handled()
        {
            var actor = RecoveredEventsourcedActor(stateSync: true);

            actor.Tell(new Written(Event2B));
            actor.Tell(new Written(new DurableEvent("x", Event2C.EmitterId, Event2C.EmitterAggregateId,
                Event2C.CustomDestinationAggregateIds, Event2C.SystemTimestamp, 
                Event2C.VectorTimestamp, Event2C.ProcessId, Event2C.LocalLogId, Event2C.LocalSequenceNr)));
            actor.Tell("status");
            actor.Tell(new Written(Event2D));

            evtProbe.ExpectMsg(("b", Event2B.VectorTimestamp, Timestamp(0, 1), Event2B.LocalSequenceNr));
            cmdProbe.ExpectMsg(("status", Event2B.VectorTimestamp, Timestamp(0, 1), Event2B.LocalSequenceNr));
            evtProbe.ExpectMsg(("d", Event2D.VectorTimestamp, Timestamp(0, 3), Event2D.LocalSequenceNr));
        }

        [Fact]
        public void EventsourcedActor_in_any_mode_must_must_dispatch_unhandled_commands_to_the_unhandled_method()
        {
            var actor = RecoveredEventsourcedActor(stateSync: true);
            actor.Tell("unhandled-command");
            cmdProbe.ExpectMsg("unhandled-command");
        }

        [Fact]
        public void EventsourcedActor_in_any_mode_must_ignore_duplicate_replies_from_event_log()
        {
            var actor = RecoveredEventsourcedActor(stateSync: true);
            actor.Tell(new Cmd("a", 1), cmdProbe.Ref);

            var write = logProbe.ExpectMsg<Write>();
            var e = write.Events.ElementAt(0);
            var event1 = new DurableEvent(e.Payload, e.EmitterId, e.EmitterAggregateId, e.CustomDestinationAggregateIds,
                e.SystemTimestamp, e.VectorTimestamp, e.ProcessId, "logA", 1, e.DeliveryId, e.PersistOnEventSequenceNr, e.PersistOnEventId);
            
            logProbe.Sender.Tell(new WriteSuccess(new []{event1}, write.CorrelationId, instanceId));
            evtProbe.ExpectMsg((event1.Payload.ToString(), event1.VectorTimestamp, event1.VectorTimestamp, event1.LocalSequenceNr));
            logProbe.Sender.Tell(new WriteSuccess(new []{event1}, write.CorrelationId, instanceId));
            evtProbe.ExpectNoMsg(Timeout);
        }

        [Fact]
        public void EventsourcedActor_in_any_mode_must_apply_Write_replies_and_Written_messages_in_received_order()
        {
            var probe = CreateTestProbe();
            var actor = RecoveredStashingActor(probe.Ref, stateSync: false);
            
            actor.Tell(new Cmd("a", 1));
            actor.Tell(new Cmd("b", 1));
            actor.Tell(new Cmd("c", 1));
            
            ProcessWrite(1);
            actor.Tell(new Written(Event("x", 2)));
            ProcessWrite(3);
            actor.Tell(new Written(Event("y", 4)));
            ProcessWrite(5);
            actor.Tell(new Written(Event("z", 6)));

            probe.ExpectMsg("a");
            probe.ExpectMsg("x");
            probe.ExpectMsg("b");
            probe.ExpectMsg("y");
            probe.ExpectMsg("c");
            probe.ExpectMsg("z");
        }

        [Fact]
        public void EventsourcedActor_in_any_mode_must_ignore_WriteSuccess_duplicates()
        {
            var probe = CreateTestProbe();
            var actor = RecoveredStashingActor(probe.Ref, stateSync: false);
            
            actor.Tell(new Cmd("a", 1));
            actor.Tell(new Cmd("b", 1));

            var write1 = logProbe.ExpectMsg<Write>();
            logProbe.Sender.Tell(new WriteSuccess(new[]{Event(write1.Events.First().Payload, 1)}, write1.CorrelationId, instanceId));
            logProbe.Sender.Tell(new WriteSuccess(new[]{Event(write1.Events.First().Payload, 1)}, write1.CorrelationId, instanceId));

            var write2 = logProbe.ExpectMsg<Write>();
            logProbe.Sender.Tell(new WriteSuccess(new[]{Event(write2.Events.First().Payload, 2)}, write2.CorrelationId, instanceId));

            probe.ExpectMsg("a");
            probe.ExpectMsg("b");
        }

        [Fact]
        public void EventsourcedActor_in_any_mode_must_ignore_WriteFailure_duplicates()
        {
            var probe = CreateTestProbe();
            var actor = RecoveredStashingActor(probe.Ref, stateSync: false);

            var e1 = new Exception("1");
            var e2 = new Exception("2");
            
            actor.Tell(new Cmd("a", 1));
            actor.Tell(new Cmd("b", 1));

            var write1 = logProbe.ExpectMsg<Write>();
            logProbe.Sender.Tell(new WriteFailure(write1.Events, e1, write1.CorrelationId, instanceId));
            logProbe.Sender.Tell(new WriteFailure(write1.Events, e1, write1.CorrelationId, instanceId));

            var write2 = logProbe.ExpectMsg<Write>();
            
            logProbe.Sender.Tell(new WriteFailure(write2.Events, e2, write2.CorrelationId, instanceId));

            probe.ExpectMsg(e1);
            probe.ExpectMsg(e2);
        }

        [Fact]
        public void EventsourcedActor_must_recover_from_snapshots()
        {
            var actor = UnrecoveredSnapshotActor();
            var s = ImmutableArray.Create("a", "b");
            var snapshot = new Snapshot(new State(s), "A", Event("b", 2), Timestamp(2, 4), 2);

            logProbe.ExpectMsg(new LoadSnapshot("A", instanceId));
            logProbe.Sender.Tell(new LoadSnapshotSuccess(snapshot, instanceId));
            logProbe.ExpectMsg(new Replay(actor, instanceId, 3));
            logProbe.Sender.Tell(new ReplaySuccess(Array.Empty<DurableEvent>(), 2, instanceId));
            evtProbe.ExpectMsg((new State(s), Timestamp(2), Timestamp(2, 4), 2L));
        }

        [Fact]
        public void EventsourcedActor_must_recover_from_snapshot_and_remaining_events()
        {
            var actor = UnrecoveredSnapshotActor();
            var s = ImmutableArray.Create("a", "b");
            var snapshot = new Snapshot(new State(s), "A", Event("b", 2), Timestamp(2, 4), 2);

            logProbe.ExpectMsg(new LoadSnapshot("A", instanceId));
            logProbe.Sender.Tell(new LoadSnapshotSuccess(snapshot, instanceId));
            logProbe.ExpectMsg(new Replay(actor, instanceId, 3));
            logProbe.Sender.Tell(new ReplaySuccess(new []{Event("c", 3), Event("d", 4)}, 4, instanceId));
            
            evtProbe.ExpectMsg((new State("a", "b"), Timestamp(2), Timestamp(2, 4), 2L));
            evtProbe.ExpectMsg((new State("a", "b", "c"), Timestamp(3), Timestamp(3, 4), 3L));
            evtProbe.ExpectMsg((new State("a", "b", "c", "d"), Timestamp(4), Timestamp(4, 4), 4L));
        }

        [Fact]
        public void EventsourcedActor_must_recover_from_snapshot_and_deliver_unconfirmed_messages()
        {
            var actor = UnrecoveredSnapshotActor();
            var unconfirmed = ImmutableHashSet<DeliveryAttempt>.Empty
                .Add(new DeliveryAttempt("3", "x", cmdProbe.Ref.Path))
                .Add(new DeliveryAttempt("4", "y", cmdProbe.Ref.Path));
            var s = ImmutableArray.Create("a", "b");
            var snapshot = new Snapshot(new State(s), "A", Event("b", 2), Timestamp(2, 4), 2, deliveryAttempts: unconfirmed);

            logProbe.ExpectMsg(new LoadSnapshot("A", instanceId));
            logProbe.Sender.Tell(new LoadSnapshotSuccess(snapshot, instanceId));
            logProbe.ExpectMsg(new Replay(actor, instanceId, 3));
            logProbe.Sender.Tell(new ReplaySuccess(Array.Empty<DurableEvent>(), 2, instanceId));
            evtProbe.ExpectMsg((new State(s), Timestamp(2), Timestamp(2, 4), 2L));
            cmdProbe.ExpectMsg("x");
            cmdProbe.ExpectMsg("y");
        }

        [Fact]
        public void EventsourcedActor_must_recover_from_scratch_if_onSnapshot_doesnt_handle_loaded_snapshot()
        {
            var actor = UnrecoveredSnapshotActor();
            var snapshot = new Snapshot("foo", EmitterIdA, Event("b", 2), Timestamp(2, 4), 2);
            
            logProbe.ExpectMsg(new LoadSnapshot(EmitterIdA, instanceId));
            logProbe.Sender.Tell(new LoadSnapshotSuccess(snapshot, instanceId));
            logProbe.ExpectMsg(new Replay(actor, instanceId, 1));
            logProbe.Sender.Tell(new ReplaySuccess(new []{Event("a", 1), Event("b", 2)}, 2, instanceId));
            evtProbe.ExpectMsg((new State("a"), Timestamp(1), Timestamp(1), 1L));
            evtProbe.ExpectMsg((new State("a", "b"), Timestamp(2), Timestamp(2), 2L));
        }

        [Fact]
        public void EventsourcedActor_must_save_snapshot()
        {
            var event1 = new DurableEvent("x", EmitterIdB, null, ImmutableHashSet<string>.Empty, default, Timestamp(0, 1), LogIdB, LogIdA, 1);
            var event2 = new DurableEvent("a", EmitterIdA, null, ImmutableHashSet<string>.Empty, default, Timestamp(2, 1), LogIdA, LogIdA, 2);
            var event3 = new DurableEvent("b", EmitterIdA, null, ImmutableHashSet<string>.Empty, default, Timestamp(3, 1), LogIdA, LogIdA, 3);

            var actor = RecoveredSnapshotActor();
            actor.Tell(new Written(event1));
            evtProbe.ExpectMsg((new State("x"), Timestamp(0, 1), Timestamp(0, 1), 1L));
            actor.Tell(new Cmd("a"));
            actor.Tell(new Cmd("b"));

            var write1 = logProbe.ExpectMsg<Write>();
            logProbe.Sender.Tell(new WriteSuccess(new []{event2}, write1.CorrelationId, instanceId));
            
            var write2 = logProbe.ExpectMsg<Write>();
            logProbe.Sender.Tell(new WriteSuccess(new []{event3}, write2.CorrelationId, instanceId));

            evtProbe.ExpectMsg((new State("x", "a"), Timestamp(2, 1), Timestamp(2, 1), 2L));
            evtProbe.ExpectMsg((new State("x", "a", "b"), Timestamp(3, 1), Timestamp(3, 1), 3L));
            
            actor.Tell("snap", ActorRefs.NoSender);
            
            var snapshot = new Snapshot(new State(ImmutableArray.Create("x", "a", "b")), "A", event3, Timestamp(3,1), 3);
            logProbe.ExpectMsg(new SaveSnapshot(snapshot, Sys.DeadLetters, instanceId));
            logProbe.Sender.Tell(new SaveSnapshotSuccess(snapshot.Metadata, instanceId));
            cmdProbe.ExpectMsg(snapshot.Metadata);
        }

        [Fact]
        public void EventsourcedActor_must_save_snapshot_with_unconfirmed_messages()
        {
            var actor = RecoveredSnapshotActor();
            actor.Tell(new Cmd("a"));
            actor.Tell(new Cmd("b"));
            actor.Tell(new Deliver("x"));
            actor.Tell(new Deliver("y"));

            var write1 = logProbe.ExpectMsg<Write>();
            logProbe.Sender.Tell(new WriteSuccess(new []{Event("a", 1)}, write1.CorrelationId, instanceId));
            
            var write2 = logProbe.ExpectMsg<Write>();
            logProbe.Sender.Tell(new WriteSuccess(new []{Event("b", 2)}, write2.CorrelationId, instanceId));
            
            var write3 = logProbe.ExpectMsg<Write>();
            logProbe.Sender.Tell(new WriteSuccess(new []{Event(new DeliverRequested("x"), 3)}, write3.CorrelationId, instanceId));

            var write4 = logProbe.ExpectMsg<Write>();
            logProbe.Sender.Tell(new WriteSuccess(new []{Event(new DeliverRequested("y"), 4)}, write4.CorrelationId, instanceId));


            evtProbe.ExpectMsg((new State("a"), Timestamp(1), Timestamp(1), 1L));
            evtProbe.ExpectMsg((new State("a", "b"), Timestamp(2), Timestamp(2), 2L));
            cmdProbe.ExpectMsg(((object)"x", Timestamp(3), Timestamp(3), 3L));
            cmdProbe.ExpectMsg(((object)"y", Timestamp(4), Timestamp(4), 4L));
            actor.Tell("snap", ActorRefs.NoSender);

            var unconfirmed = ImmutableHashSet<DeliveryAttempt>.Empty
                .Add(new DeliveryAttempt("3", ((object)"x", Timestamp(3), Timestamp(3), 3L), cmdProbe.Ref.Path))
                .Add(new DeliveryAttempt("4", ((object)"y", Timestamp(4), Timestamp(4), 4L), cmdProbe.Ref.Path));
            var snapshot = new Snapshot(new State(ImmutableArray.Create("a", "b")), EmitterIdA,
                Event(new DeliverRequested("y"), 4), Timestamp(4), 4L, 
                deliveryAttempts: unconfirmed);
            
            var actual = logProbe.ExpectMsg<SaveSnapshot>();
            actual.InstanceId.Should().Be(instanceId);
            actual.Initiator.Should().Be(Sys.DeadLetters);
            actual.Snapshot.Should().Be(snapshot);
            logProbe.Sender.Tell(new SaveSnapshotSuccess(snapshot.Metadata, instanceId));
            cmdProbe.ExpectMsg(snapshot.Metadata);
        }

        [Fact]
        public void EventsourcedActor_must_not_save_the_same_snapshot_concurrently()
        {
            var actor = RecoveredSnapshotActor();
            actor.Tell("snap");
            actor.Tell("snap");
            cmdProbe.ExpectMsg<IllegalActorStateException>();
        }
    }
}