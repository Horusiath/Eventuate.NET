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
using Akka.Actor;
using Akka.TestKit;
using Akka.TestKit.Xunit2;
using Eventuate.EventsourcingProtocol;
using FluentAssertions;
using FluentAssertions.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace Eventuate.Tests
{
    public class EventsourcedViewSpec : TestKit
    {
        internal const string EmitterIdA = "A";
        internal const string EmitterIdB = "B";
        internal const string LogIdA = "logA";
        internal const string LogIdB = "logB";

        #region internal classes

        internal sealed class TestEventsourcedView : EventsourcedView
        {
            private readonly IActorRef msgProbe;

            public TestEventsourcedView(IActorRef logProbe, IActorRef msgProbe, int? customReplayBatchSize,
                long? replayFromSequenceNr = null)
            {
                this.msgProbe = msgProbe;
                this.ReplayBatchSize = customReplayBatchSize ?? base.ReplayBatchSize;
                this.EventLog = logProbe;
                this.ReplayFromSequenceNr = replayFromSequenceNr;
            }

            public override int ReplayBatchSize { get; }
            protected override long? ReplayFromSequenceNr { get; }
            public override string Id => EmitterIdA;
            public override IActorRef EventLog { get; }

            protected override bool OnCommand(object message)
            {
                switch (message)
                {
                    case "boom":
                        throw TestException.Instance;
                    case Ping p:
                        msgProbe.Tell(new Pong(p.I));
                        return true;
                    default: return false;
                }
            }

            protected override bool OnEvent(object evt)
            {
                switch (evt)
                {
                    case "boom": throw TestException.Instance;
                    case string s:
                        msgProbe.Tell((s, LastVectorTimestamp, LastSequenceNr));
                        return true;
                    default: return false;
                }
            }
        }

        internal sealed class TestStashingView : EventsourcedView
        {
            private readonly IActorRef msgProbe;
            private bool stashing = false;

            public TestStashingView(IActorRef logProbe, IActorRef msgProbe)
            {
                this.msgProbe = msgProbe;
                this.EventLog = logProbe;
            }

            public override string Id => EmitterIdA;
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
                    case Ping p when stashing:
                        StashCommand();
                        return true;
                    case Ping p:
                        msgProbe.Tell(new Pong(p.I));
                        return true;
                    default: return false;
                }
            }

            protected override bool OnEvent(object evt)
            {
                switch (evt)
                {
                    case "unstash":
                        UnstashAll();
                        return true;
                    default: return false;
                }
            }
        }

        internal sealed class TestCompletionView : EventsourcedView
        {
            private readonly IActorRef msgProbe;

            public TestCompletionView(IActorRef logProbe, IActorRef msgProbe)
            {
                this.msgProbe = msgProbe;
                this.EventLog = logProbe;
            }

            public override string Id => EmitterIdA;
            public override IActorRef EventLog { get; }

            protected override void OnRecovery(Exception failure = null) =>
                msgProbe.Tell(failure ?? (object) "success");

            protected override bool OnCommand(object message) => true;
            protected override bool OnEvent(object evt) => true;
        }

        internal sealed class TestBehaviorView : EventsourcedView
        {
            private readonly IActorRef msgProbe;
            private int total = 0;

            public TestBehaviorView(IActorRef logProbe, IActorRef msgProbe)
            {
                this.msgProbe = msgProbe;
                this.EventLog = logProbe;
            }

            private bool Add(object msg)
            {
                if (msg is int i)
                {
                    msgProbe.Tell(total += i);
                    return true;
                }

                return false;
            }

            private bool Sub(object msg)
            {
                if (msg is int i)
                {
                    msgProbe.Tell(total -= i);
                    return true;
                }

                return false;
            }

            private Receive OrElse(Receive l, Receive r) => m => l(m) || r(m);

            public Receive Change(BehaviorContext context) => message =>
            {
                switch (message)
                {
                    case "add":
                        context.Become(OrElse(Add, Change(context)));
                        return true;
                    case "sub":
                        context.Become(OrElse(Sub, Change(context)));
                        return true;
                    default: return false;
                }
            };

            public override string Id => EmitterIdA;
            public override IActorRef EventLog { get; }
            protected override bool OnCommand(object message) => OrElse(Add, Change(CommandContext))(message);
            protected override bool OnEvent(object evt) => OrElse(Add, Change(EventContext))(evt);
        }

        internal sealed class TestGuardingView : EventsourcedView
        {
            private readonly IActorRef msgProbe;

            public TestGuardingView(IActorRef logProbe, IActorRef msgProbe)
            {
                this.msgProbe = msgProbe;
                this.EventLog = logProbe;
            }

            public override string Id => EmitterIdA;
            public override IActorRef EventLog { get; }

            protected override bool OnCommand(object message)
            {
                switch (message)
                {
                    case "last":
                        msgProbe.Tell(LastHandledEvent);
                        return true;
                    default: return false;
                }
            }

            protected override bool OnEvent(object evt)
            {
                switch (evt)
                {
                    case "e1" when LastEmitterId == "x":
                        msgProbe.Tell("handled");
                        return true;
                    case "e2" when LastEmitterId == "y":
                        msgProbe.Tell("handled");
                        return true;
                    default: return false;
                }
            }
        }

        #endregion

        internal static VectorTime Timestamp(long a = 0, long b = 0)
        {
            if (a == 0 && b == 0) return VectorTime.Zero;
            if (a == 0) return new VectorTime((LogIdB, b));
            if (b == 0) return new VectorTime((LogIdA, a));
            return new VectorTime((LogIdA, a), (LogIdB, b));
        }

        internal static DurableEvent Event(object payload, long sequenceNr, string emitterId = null) =>
            new DurableEvent(payload, emitterId ?? EmitterIdA, null, ImmutableHashSet<string>.Empty, DateTime.MinValue,
                Timestamp(sequenceNr), LogIdA, LogIdA, sequenceNr);

        private static readonly DurableEvent Event1A = Event("a", 1);
        private static readonly DurableEvent Event1B = Event("b", 2);
        private static readonly DurableEvent Event1C = Event("c", 3);

        internal static readonly DurableEvent Event2A = new DurableEvent("a", EmitterIdA, null, ImmutableHashSet<string>.Empty, DateTime.MinValue,
            Timestamp(1, 0), LogIdA, LogIdA, 1);
        internal static readonly DurableEvent Event2B = new DurableEvent("b", EmitterIdB, null, ImmutableHashSet<string>.Empty, DateTime.MinValue,
            Timestamp(0, 1), LogIdB, LogIdA, 2);
        internal static readonly DurableEvent Event2C = new DurableEvent("c", EmitterIdB, null, ImmutableHashSet<string>.Empty, DateTime.MinValue,
            Timestamp(0, 2), LogIdB, LogIdA, 3);
        internal static readonly DurableEvent Event2D = new DurableEvent("d", EmitterIdB, null, ImmutableHashSet<string>.Empty, DateTime.MinValue,
            Timestamp(0, 3), LogIdB, LogIdA, 4);

        private readonly int instanceId;
        private readonly TestProbe logProbe;
        private readonly TestProbe msgProbe;

        public EventsourcedViewSpec(ITestOutputHelper output) : base(config: TestHelpers.Config, output: output)
        {
            this.instanceId = EventsourcedView.InstanceIdCounter.Current;
            this.logProbe = CreateTestProbe();
            this.msgProbe = CreateTestProbe();
        }

        private IActorRef UnrecoveredEventsourcedView() =>
            Sys.ActorOf(Props.Create(() => new TestEventsourcedView(logProbe.Ref, msgProbe.Ref, null, null)));

        private IActorRef UnrecoveredEventsourcedView(int customBatchReplaySize) =>
            Sys.ActorOf(Props.Create(() =>
                new TestEventsourcedView(logProbe.Ref, msgProbe.Ref, customBatchReplaySize, null)));

        private IActorRef UnrecoveredCompletionView() =>
            Sys.ActorOf(Props.Create(() => new TestCompletionView(logProbe.Ref, msgProbe.Ref)));

        private IActorRef RecoveredCompletionView() => ProcessRecover(UnrecoveredCompletionView());

        private IActorRef RecoveredStashingView() =>
            ProcessRecover(Sys.ActorOf(Props.Create(() => new TestStashingView(logProbe.Ref, msgProbe.Ref))));

        private IActorRef RecoveredBehaviorView() =>
            ProcessRecover(Sys.ActorOf(Props.Create(() => new TestBehaviorView(logProbe.Ref, msgProbe.Ref))));

        private IActorRef RecoveredGuardingView() =>
            ProcessRecover(Sys.ActorOf(Props.Create(() => new TestGuardingView(logProbe.Ref, msgProbe.Ref))));

        private IActorRef ReplayControllingActor(long? sequenceNr) =>
            Sys.ActorOf(Props.Create(() => new TestEventsourcedView(logProbe.Ref, msgProbe.Ref, null, sequenceNr)));

        private IActorRef ProcessRecover(IActorRef actor, int? instanceId = null)
        {
            var iid = instanceId ?? this.instanceId;
            logProbe.ExpectMsg(new LoadSnapshot(EmitterIdA, iid));
            logProbe.Sender.Tell(new LoadSnapshotSuccess(null, iid));
            logProbe.ExpectMsg(new Replay(actor, iid, 1));
            actor.Tell(new ReplaySuccess(Array.Empty<DurableEvent>(), 0, iid));
            return actor;
        }

        [Fact]
        public void EventsourcedView_must_recover_from_replayed_events()
        {
            var actor = UnrecoveredEventsourcedView();

            logProbe.ExpectMsg(new LoadSnapshot(EmitterIdA, instanceId));
            logProbe.Sender.Tell(new LoadSnapshotSuccess(null, instanceId));
            logProbe.ExpectMsg(new Replay(actor, instanceId, 1));
            logProbe.Sender.Tell(new ReplaySuccess(new[] {Event1A, Event1B}, Event1B.LocalSequenceNr, instanceId));

            msgProbe.ExpectMsg(("a", Event1A.VectorTimestamp, Event1A.LocalSequenceNr));
            msgProbe.ExpectMsg(("b", Event1B.VectorTimestamp, Event1B.LocalSequenceNr));
        }

        [Fact]
        public void EventsourcedView_must_recover_from_events_replayed_in_batches()
        {
            var actor = UnrecoveredEventsourcedView(2);

            logProbe.ExpectMsg(new LoadSnapshot(EmitterIdA, instanceId));
            logProbe.Sender.Tell(new LoadSnapshotSuccess(null, instanceId));
            logProbe.ExpectMsg(new Replay(actor, instanceId, 1, 2));
            logProbe.Sender.Tell(new ReplaySuccess(new[] {Event1A, Event1B}, Event1B.LocalSequenceNr, instanceId));
            logProbe.ExpectMsg(new Replay(null, instanceId, Event1B.LocalSequenceNr + 1L, 2));
            logProbe.Sender.Tell(new ReplaySuccess(new[] {Event1C}, Event1C.LocalSequenceNr, instanceId));

            msgProbe.ExpectMsg(("a", Event1A.VectorTimestamp, Event1A.LocalSequenceNr));
            msgProbe.ExpectMsg(("b", Event1B.VectorTimestamp, Event1B.LocalSequenceNr));
            msgProbe.ExpectMsg(("c", Event1C.VectorTimestamp, Event1C.LocalSequenceNr));
        }

        [Fact]
        public void EventsourcedView_must_retry_recovery_on_failure()
        {
            var actor = UnrecoveredEventsourcedView();

            logProbe.ExpectMsg(new LoadSnapshot(EmitterIdA, instanceId));
            logProbe.Sender.Tell(new LoadSnapshotSuccess(null, instanceId));
            logProbe.ExpectMsg(new Replay(actor, instanceId, 1));
            logProbe.Sender.Tell(new ReplaySuccess(new []{Event1A, Event("boom", 2), Event1C}, Event1C.LocalSequenceNr, instanceId));

            logProbe.ExpectMsg(new LoadSnapshot(EmitterIdA, instanceId + 1));
            logProbe.Sender.Tell(new LoadSnapshotSuccess(null, instanceId + 1));
            logProbe.ExpectMsg(new Replay(actor, instanceId + 1, 1));
            logProbe.Sender.Tell(new ReplaySuccess(new []{Event1A, Event1B, Event1C}, Event1C.LocalSequenceNr, instanceId + 1));

            msgProbe.ExpectMsg(("a", Event1A.VectorTimestamp, Event1A.LocalSequenceNr));
            msgProbe.ExpectMsg(("a", Event1A.VectorTimestamp, Event1A.LocalSequenceNr));
            msgProbe.ExpectMsg(("b", Event1B.VectorTimestamp, Event1B.LocalSequenceNr));
            msgProbe.ExpectMsg(("c", Event1C.VectorTimestamp, Event1C.LocalSequenceNr));
        }

        [Fact]
        public void EventsourcedView_must_replay_from_application_defined_sequence_number_not_load_snapshot()
        {
            var actor = ReplayControllingActor(2);

            logProbe.ExpectMsg(new Replay(actor, instanceId, 2));

            logProbe.Sender.Tell(new ReplaySuccess(new[] {Event1B, Event1C}, Event1C.LocalSequenceNr, instanceId));
            
            msgProbe.ExpectMsg(("b", Event1B.VectorTimestamp, Event1B.LocalSequenceNr));
            msgProbe.ExpectMsg(("c", Event1C.VectorTimestamp, Event1C.LocalSequenceNr));
        }

        [Fact]
        public void EventsourcedView_must_replay_from_application_defined_sequence_number_load_snapshot()
        {
            ProcessRecover(ReplayControllingActor(null));
        }

        [Fact]
        public void EventsourcedView_must_stash_commands_during_recovery_and_handle_them_after_initial_recovery()
        {
            var actor = UnrecoveredEventsourcedView();
            
            logProbe.ExpectMsg(new LoadSnapshot(EmitterIdA, instanceId));
            logProbe.Sender.Tell(new LoadSnapshotSuccess(null, instanceId));
            logProbe.ExpectMsg(new Replay(actor, instanceId, 1));
            
            actor.Tell(new Ping(1));
            actor.Tell(new Ping(2));
            actor.Tell(new Ping(3));
            
            logProbe.Sender.Tell(new ReplaySuccess(new []{Event1A, Event1B}, Event1B.LocalSequenceNr, instanceId));
            logProbe.ExpectMsg(new Replay(null, instanceId, Event1B.LocalSequenceNr + 1));
            logProbe.Sender.Tell(new ReplaySuccess(Array.Empty<DurableEvent>(), Event1B.LocalSequenceNr, instanceId));

            msgProbe.ExpectMsg(("a", Event1A.VectorTimestamp, Event1A.LocalSequenceNr));
            msgProbe.ExpectMsg(("b", Event1B.VectorTimestamp, Event1B.LocalSequenceNr));
            msgProbe.ExpectMsg(new Pong(1));
            msgProbe.ExpectMsg(new Pong(2));
            msgProbe.ExpectMsg(new Pong(3));
        }

        [Fact]
        public void EventsourcedView_must_stash_commands_during_recovery_and_handle_them_after_retried_recovery()
        {
            var actor = UnrecoveredEventsourcedView();
            
            logProbe.ExpectMsg(new LoadSnapshot(EmitterIdA, instanceId));
            logProbe.Sender.Tell(new LoadSnapshotSuccess(null, instanceId));
            logProbe.ExpectMsg(new Replay(actor, instanceId, 1));
            
            actor.Tell(new Ping(1));
            actor.Tell(new Ping(2));
            
            logProbe.Sender.Tell(new ReplaySuccess(new []{Event1A, Event("boom", 2), Event1C}, Event1C.LocalSequenceNr, instanceId));

            var nextInstanceId = instanceId + 1;
            logProbe.ExpectMsg(new LoadSnapshot(EmitterIdA, nextInstanceId));
            logProbe.Sender.Tell(new LoadSnapshotSuccess(null, nextInstanceId));
            logProbe.ExpectMsg(new Replay(actor, nextInstanceId, 1));
            logProbe.Sender.Tell(new ReplaySuccess(new []{Event1A, Event1B, Event1C}, Event1C.LocalSequenceNr, nextInstanceId));
            logProbe.ExpectMsg(new Replay(null, nextInstanceId, Event1C.LocalSequenceNr + 1));
            logProbe.Sender.Tell(new ReplaySuccess(Array.Empty<DurableEvent>(), Event1C.LocalSequenceNr, nextInstanceId));

            msgProbe.ExpectMsg(("a", Event1A.VectorTimestamp, Event1A.LocalSequenceNr));
            msgProbe.ExpectMsg(("a", Event1A.VectorTimestamp, Event1A.LocalSequenceNr));
            msgProbe.ExpectMsg(("b", Event1B.VectorTimestamp, Event1B.LocalSequenceNr));
            msgProbe.ExpectMsg(("c", Event1C.VectorTimestamp, Event1C.LocalSequenceNr));
            msgProbe.ExpectMsg(new Pong(1));
            msgProbe.ExpectMsg(new Pong(2));
        }

        [Fact]
        public void EventsourcedView_must_stash_live_events_consumed_during_recovery()
        {
            var actor = UnrecoveredEventsourcedView();
            
            logProbe.ExpectMsg(new LoadSnapshot(EmitterIdA, instanceId));
            logProbe.Sender.Tell(new LoadSnapshotSuccess(null, instanceId));
            logProbe.ExpectMsg(new Replay(actor, instanceId, 1));
            
            actor.Tell(new Written(Event2C));    // live event
            actor.Tell(new Written(Event2D));    // live event
            
            logProbe.Sender.Tell(new ReplaySuccess(new []{ Event2A, Event2B}, Event2B.LocalSequenceNr, instanceId));
            logProbe.ExpectMsg(new Replay(null, instanceId, Event2B.LocalSequenceNr + 1));
            logProbe.Sender.Tell(new ReplaySuccess(Array.Empty<DurableEvent>(), Event2B.LocalSequenceNr, instanceId));

            msgProbe.ExpectMsg(("a", Event2A.VectorTimestamp, Event2A.LocalSequenceNr));
            msgProbe.ExpectMsg(("b", Event2B.VectorTimestamp, Event2B.LocalSequenceNr));
            msgProbe.ExpectMsg(("c", Event2C.VectorTimestamp, Event2C.LocalSequenceNr));
            msgProbe.ExpectMsg(("d", Event2D.VectorTimestamp, Event2D.LocalSequenceNr));
        }

        [Fact]
        public void EventsourcedView_must_ignore_live_events_targetting_previous_incarnations()
        {
            var actor = UnrecoveredEventsourcedView();
            var next = instanceId + 1;
            
            logProbe.ExpectMsg(new LoadSnapshot(EmitterIdA, instanceId));
            logProbe.Sender.Tell(new LoadSnapshotSuccess(null, instanceId));
            logProbe.ExpectMsg(new Replay(actor, instanceId, 1));
            
            logProbe.Sender.Tell(new ReplaySuccess(new []{Event2A, Event2B}, Event2B.LocalSequenceNr, instanceId));
            logProbe.ExpectMsg(new Replay(null, instanceId, Event2B.LocalSequenceNr + 1));
            logProbe.Sender.Tell(new ReplaySuccess(Array.Empty<DurableEvent>(), Event2B.LocalSequenceNr, instanceId));
            
            actor.Tell("boom");
            actor.Tell(new Written(Event2C));    // live event

            msgProbe.ExpectMsg(("a", Event2A.VectorTimestamp, Event2A.LocalSequenceNr));
            msgProbe.ExpectMsg(("b", Event2B.VectorTimestamp, Event2B.LocalSequenceNr));

            logProbe.ExpectMsg(new LoadSnapshot(EmitterIdA, next));
            logProbe.Sender.Tell(new LoadSnapshotSuccess(null, next));
            logProbe.ExpectMsg(new Replay(actor, next, 1));
            logProbe.Sender.Tell(new ReplaySuccess(new []{Event2A, Event2B, Event2C}, Event2C.LocalSequenceNr, next));
            logProbe.ExpectMsg(new Replay(null, next, Event2C.LocalSequenceNr + 1));
            logProbe.Sender.Tell(new ReplaySuccess(Array.Empty<DurableEvent>(), Event2C.LocalSequenceNr, next));
            
            actor.Tell(new Written(Event2D));    // live event

            msgProbe.ExpectMsg(("a", Event2A.VectorTimestamp, Event2A.LocalSequenceNr));
            msgProbe.ExpectMsg(("b", Event2B.VectorTimestamp, Event2B.LocalSequenceNr));
            msgProbe.ExpectMsg(("c", Event2C.VectorTimestamp, Event2C.LocalSequenceNr));
            msgProbe.ExpectMsg(("d", Event2D.VectorTimestamp, Event2D.LocalSequenceNr));
        }

        [Fact]
        public void EventsourcedView_must_support_user_stash_and_unstash_operations()
        {
            var actor = RecoveredStashingView();
            
            actor.Tell(new Ping(1));
            actor.Tell("stash-on");
            actor.Tell(new Ping(2));
            actor.Tell("stash-off");
            actor.Tell(new Ping(3));
            actor.Tell("unstash");
            actor.Tell(new Ping(4));

            msgProbe.ExpectMsg(new Pong(1));
            msgProbe.ExpectMsg(new Pong(3));
            msgProbe.ExpectMsg(new Pong(2));
            msgProbe.ExpectMsg(new Pong(4));
        }

        [Fact]
        public void EventsourcedView_must_support_user_unstash_operation_in_event_handler()
        {
            var actor = RecoveredStashingView();
            
            actor.Tell(new Ping(1));
            actor.Tell("stash-on");
            actor.Tell(new Ping(2));
            actor.Tell("stash-off");
            actor.Tell(new Ping(3));
            actor.Tell(new Written(Event("unstash", 1)));
            actor.Tell(new Ping(4));
            
            msgProbe.ExpectMsg(new Pong(1));
            msgProbe.ExpectMsg(new Pong(3));
            msgProbe.ExpectMsg(new Pong(2));
            msgProbe.ExpectMsg(new Pong(4));
        }

        [Fact]
        public void EventsourcedView_must_support_user_stash_unstash_operations_where_unstash_is_the_last_operation()
        {
            var actor = RecoveredStashingView();
            
            actor.Tell(new Ping(1));
            actor.Tell("stash-on");
            actor.Tell(new Ping(2));
            actor.Tell("stash-off");
            actor.Tell(new Ping(3));
            actor.Tell("unstash");
            
            msgProbe.ExpectMsg(new Pong(1));
            msgProbe.ExpectMsg(new Pong(3));
            msgProbe.ExpectMsg(new Pong(2));
        }

        [Fact]
        public void
            EventsourcedView_must_support_user_stash_unstash_operations_under_failure_conditions_fail_before_stash()
        {
            var actor = RecoveredStashingView();
            
            actor.Tell(new Ping(1));
            actor.Tell("boom");
            actor.Tell("stash-on");
            actor.Tell(new Ping(2));
            actor.Tell("stash-off");
            actor.Tell(new Ping(3));
            actor.Tell("unstash");
            actor.Tell(new Ping(4));

            ProcessRecover(actor, instanceId + 1);
    
            msgProbe.ExpectMsg(new Pong(1));
            msgProbe.ExpectMsg(new Pong(3));
            msgProbe.ExpectMsg(new Pong(2));
            msgProbe.ExpectMsg(new Pong(4));
        }

        [Fact]
        public void
            EventsourcedView_must_support_user_stash_unstash_operations_under_failure_conditions_fail_after_stash()
        {
            var actor = RecoveredStashingView();
            
            actor.Tell(new Ping(1));
            actor.Tell("stash-on");
            actor.Tell(new Ping(2));
            actor.Tell("boom");
            actor.Tell(new Ping(3));
            actor.Tell("unstash");
            actor.Tell(new Ping(4));
            
            ProcessRecover(actor, instanceId + 1);
    
            msgProbe.ExpectMsg(new Pong(1));
            msgProbe.ExpectMsg(new Pong(2));
            msgProbe.ExpectMsg(new Pong(3));
            msgProbe.ExpectMsg(new Pong(4));
        }

        [Fact]
        public void EventsourcedView_must_call_the_recovery_completion_handler_with_Success_if_recovery_succeeds()
        {
            RecoveredCompletionView();
            msgProbe.ExpectMsg("success");
        }

        [Fact]
        public void EventsourcedView_must_call_the_recovery_completion_handler_with_Failure_if_recovery_fails()
        {
            var actor = UnrecoveredCompletionView();

            logProbe.ExpectMsg(new LoadSnapshot(EmitterIdA, instanceId));
            logProbe.Sender.Tell(new LoadSnapshotSuccess(null, instanceId));
            logProbe.ExpectMsg(new Replay(actor, instanceId, 1));
            actor.Tell(new ReplayFailure(TestException.Instance, 1, instanceId));
            msgProbe.ExpectMsg(TestException.Instance);
        }

        [Fact]
        public void EventsourcedView_must_support_command_handler_behavior_changes()
        {
            var actor = RecoveredBehaviorView();
            
            actor.Tell(1);
            actor.Tell(2);
            actor.Tell("sub");
            actor.Tell(3);

            msgProbe.ExpectMsg(1);
            msgProbe.ExpectMsg(3);
            msgProbe.ExpectMsg(0);
        }

        [Fact]
        public void EventsourcedView_must_support_event_handler_behavior_changes()
        {
            var actor = RecoveredBehaviorView();
            
            actor.Tell(new Written(Event(1, 1)));
            actor.Tell(new Written(Event(2, 2)));
            actor.Tell(new Written(Event("sub", 3)));
            actor.Tell(new Written(Event(3, 4)));

            msgProbe.ExpectMsg(1);
            msgProbe.ExpectMsg(3);
            msgProbe.ExpectMsg(0);
        }

        [Fact]
        public void EventsourcedView_must_stop_during_recovery_if_its_event_log_is_stopped()
        {
            var actor = Watch(UnrecoveredEventsourcedView());
            Sys.Stop(logProbe.Ref);
            ExpectTerminated(actor);
        }

        [Fact]
        public void EventsourcedView_must_stop_after_recovery_if_its_event_log_is_stopped()
        {
            var actor = Watch(ProcessRecover(UnrecoveredEventsourcedView()));
            Sys.Stop(logProbe.Ref);
            ExpectTerminated(actor);
        }

        [Fact]
        public void EventsourcedView_must_support_usage_of_last_methods_in_pattern_guards_when_guard_evaluates_to_true()
        {
            var actor = RecoveredGuardingView();
            var event1 = Event("e1", 1, emitterId: "x");

            actor.Tell(new Written(event1));
            msgProbe.ExpectMsg("handled");

            actor.Tell("last");
            msgProbe.ExpectMsg<DurableEvent>().Payload.Should().Be("e1");
        }

        [Fact]
        public void
            EventsourcedView_must_support_usage_of_last_methods_in_pattern_guards_when_guard_evaluates_to_false()
        {
            var actor = RecoveredGuardingView();

            var event1 = Event("e1", 1L, emitterId: "x");
            var event2 = Event("e2", 1L, emitterId: "x");

            actor.Tell(new Written(event1));
            actor.Tell(new Written(event2));
            msgProbe.ExpectMsg("handled");

            actor.Tell("last");
            msgProbe.ExpectMsg<DurableEvent>().Payload.Should().Be("e1");
        }
    }

    public class EventsourcedViewReplaySpec : TestKit
    {
        private static readonly string TestConfig = @"
            eventuate.log.replay-retry-max = 5
            eventuate.log.replay-retry-delay = 5ms";

        private readonly int instanceId;
        private readonly TestProbe logProbe;
        private readonly TestProbe msgProbe;

        public EventsourcedViewReplaySpec(ITestOutputHelper output) : base(TestConfig, output)
        {
            this.instanceId = EventsourcedView.InstanceIdCounter.Current;
            this.logProbe = CreateTestProbe();
            this.msgProbe = CreateTestProbe();
        }

        private IActorRef UnrecoveredCompletionView() =>
            Sys.ActorOf(Props.Create(() => new EventsourcedViewSpec.TestCompletionView(logProbe.Ref, msgProbe.Ref)));

        [Fact]
        public void EventsourcedView_must_retry_replay_on_failure()
        {
            var actor = UnrecoveredCompletionView();

            logProbe.ExpectMsg(new LoadSnapshot("A", instanceId));
            logProbe.Sender.Tell(new LoadSnapshotSuccess(null, instanceId));

            logProbe.ExpectMsg(new Replay(actor, instanceId, 1));
            actor.Tell(new ReplayFailure(TestException.Instance, 1, instanceId));

            for (int i = 0; i < 5; i++)
            {
                logProbe.ExpectMsg(new Replay(null, instanceId, 1));
                actor.Tell(new ReplayFailure(TestException.Instance, 1, instanceId));
            }

            msgProbe.ExpectMsg(TestException.Instance);
        }

        [Fact]
        public void EventsourcedView_must_successfully_finish_recovery_after_replay_retry()
        {
            var actor = UnrecoveredCompletionView();

            logProbe.ExpectMsg(new LoadSnapshot("A", instanceId));
            logProbe.Sender.Tell(new LoadSnapshotSuccess(null, instanceId));

            logProbe.ExpectMsg(new Replay(actor, instanceId, 1));
            actor.Tell(new ReplayFailure(TestException.Instance, 1, instanceId));

            logProbe.ExpectMsg(new Replay(null, instanceId, 1));
            actor.Tell(new ReplaySuccess(Array.Empty<DurableEvent>(), 0, instanceId));

            msgProbe.ExpectMsg("success");
        }
    }
}