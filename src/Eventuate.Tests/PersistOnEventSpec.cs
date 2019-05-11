#region copyright
// -----------------------------------------------------------------------
//  <copyright file="PersistOnEventSpec.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using Akka.Actor;
using Akka.TestKit;
using Akka.TestKit.Xunit2;
using Eventuate.EventsourcingProtocol;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Eventuate.Tests
{
    public class PersistOnEventSpec : TestKit
    {
        #region internal classes

        internal sealed class TestEventsourcedActor : PersistOnEventActor
        {
            private readonly IActorRef persistProbe;
            private readonly IActorRef deliverProbe;

            public TestEventsourcedActor(IActorRef eventLog, IActorRef persistProbe, IActorRef deliverProbe, bool stateSync)
            {
                this.persistProbe = persistProbe;
                this.deliverProbe = deliverProbe;
                EventLog = eventLog;
                StateSync = stateSync;
            }

            public override bool StateSync { get; }
            public override string Id => "A";
            public override IActorRef EventLog { get; }
            protected override bool OnCommand(object message)
            {
                switch (message)
                {
                    case "snap": 
                        Save("foo", r =>
                        {
                            if (r.IsFailure) throw r.Exception;
                        });
                        return true;
                    case "boom": throw TestException.Instance;
                    default: return false;
                }
            }

            protected override bool OnEvent(object message)
            {
                switch (message)
                {
                    case "a":
                    case "boom":
                        PersistOnEvent("a-1");
                        PersistOnEvent("a-2");
                        return true;
                    case "b":
                        PersistOnEvent("c");
                        PersistOnEvent("c-1");
                        return true;
                    case "c":
                        PersistOnEvent("c-2");
                        return true;
                    case "x":
                        PersistOnEvent("x-1", ImmutableHashSet<string>.Empty.Add("14"));
                        PersistOnEvent("x-2", ImmutableHashSet<string>.Empty.Add("15"));
                        return true;
                    case string e when !IsRecovering && new []{"a-1", "a-2", "c-1", "c-2"}.Contains(e):
                        persistProbe.Tell(e);
                        return true;
                    case string _: return true;
                    default: return false;
                }
            }

            protected override bool OnSnapshot(object message) => message == "foo";
            internal override void ReceiveEvent(DurableEvent e)
            {
                base.ReceiveEvent(e);
                if (e.Payload == "boom")
                {
                    // after restart, there is
                    // a PersistOnEventRequest
                    // duplicate in the mailbox
                    throw TestException.Instance;
                }
                else deliverProbe.Tell(UnconfirmedRequests);
            }
        }

        #endregion
        
        private static readonly TimeSpan Timeout = TimeSpan.FromMilliseconds(200);
        
        private static VectorTime Timestamp(long a = 0, long b = 0)
        {
            if (a == 0 && b == 0) return VectorTime.Zero;
            if (a == 0) return new VectorTime(("B", b));
            if (b == 0) return new VectorTime(("A", a));
            return new VectorTime(("A", a), ("B", b));
        }

        private static DurableEvent Event(object payload, long sequenceNr, DurableEvent persistOnEventEvent = null, string emitterId = null) =>
            new DurableEvent(payload, emitterId ?? "A", null, 
                ImmutableHashSet<string>.Empty, default, 
                Timestamp(sequenceNr), "logA", "logA", sequenceNr, null, 
                persistOnEventEvent?.LocalSequenceNr, persistOnEventEvent?.Id);

        private readonly int instanceId;
        private readonly TestProbe logProbe;
        private readonly TestProbe persistProbe;
        private readonly TestProbe deliverProbe;
        
        public PersistOnEventSpec(ITestOutputHelper output) : base(output: output)
        {
            this.instanceId = EventsourcedView.InstanceIdCounter.Current;
            this.logProbe = CreateTestProbe();
            this.persistProbe = CreateTestProbe();
            this.deliverProbe = CreateTestProbe();
        }

        private IActorRef UnrecoveredTestActor(bool stateSync) =>
            Sys.ActorOf(Props.Create(() => new TestEventsourcedActor(logProbe.Ref, persistProbe.Ref, deliverProbe.Ref, stateSync)));

        private IActorRef RecoveredTestActor(bool stateSync) =>
            ProcessRecover(UnrecoveredTestActor(stateSync));

        private IActorRef ProcessRecover(IActorRef actor, int? instanceId = null, params DurableEvent[] events)
        {
            var iid = instanceId ?? this.instanceId;
            logProbe.ExpectMsg(new LoadSnapshot("A", iid));
            logProbe.Sender.Tell(new LoadSnapshotSuccess(null, iid));

            var last = events?.LastOrDefault();
            if (last is null)
            {
                logProbe.ExpectMsg(new Replay(actor, iid, 1L));
                logProbe.Sender.Tell(new ReplaySuccess(events, 0L, iid));
            }
            else
            {
                logProbe.ExpectMsg(new Replay(actor, iid, 1L));
                logProbe.Sender.Tell(new ReplaySuccess(events, last.LocalSequenceNr, iid));
                logProbe.ExpectMsg(new Replay(null, iid, last.LocalSequenceNr+1));
                logProbe.Sender.Tell(new ReplaySuccess(Array.Empty<DurableEvent>(), last.LocalSequenceNr, iid));
                
            }
            
            return actor;
        }

        [Fact]
        public void PersistOnEventActor_must_support_persistence_in_event_handler()
        {
            var actor = RecoveredTestActor(stateSync: true);
            var eventA = Event("a", 1L);
            actor.Tell(new Written(eventA));

            deliverProbe.ExpectMsg<ImmutableHashSet<long>>().Should().BeEquivalentTo(1L);

            var write = logProbe.ExpectMsg<Write>();
            var events = write.Events.ToArray();
            events[0].PersistOnEventId.Should().Be(eventA.Id);
            events[1].PersistOnEventId.Should().Be(eventA.Id);
            
            logProbe.Sender.Tell(new WriteSuccess(new[]
            {
                Event(events[0].Payload, 2L, eventA),
                Event(events[1].Payload, 3L, eventA),
            }, write.CorrelationId, instanceId));

            deliverProbe.ExpectMsg<ImmutableHashSet<long>>().Should().BeEmpty();
            deliverProbe.ExpectMsg<ImmutableHashSet<long>>().Should().BeEmpty();
            
            persistProbe.ExpectMsg("a-1");
            persistProbe.ExpectMsg("a-2");
        }

        [Fact]
        public void PersistOnEventActor_must_support_cascading_persistence()
        {
            var actor = RecoveredTestActor(stateSync: true);
            var eventB = Event("b", 1L);
            actor.Tell(new Written(eventB));

            deliverProbe.ExpectMsg<ImmutableHashSet<long>>().Should().BeEquivalentTo(1L);

            var write1 = logProbe.ExpectMsg<Write>();
            var events = write1.Events.ToArray();
            events[0].PersistOnEventId.Should().Be(eventB.Id);
            events[1].PersistOnEventId.Should().Be(eventB.Id);

            var eventC = Event(events[0].Payload, 2L, eventB);
            var eventC2 = Event(events[1].Payload, 3L, eventB);
            logProbe.Sender.Tell(new WriteSuccess(new[] {eventC, eventC2}, write1.CorrelationId, instanceId));

            deliverProbe.ExpectMsg<ImmutableHashSet<long>>().Should().BeEquivalentTo(2L);
            deliverProbe.ExpectMsg<ImmutableHashSet<long>>().Should().BeEquivalentTo(2L);
            persistProbe.ExpectMsg("c-1");
            
            var write2 = logProbe.ExpectMsg<Write>();
            events = write2.Events.ToArray();
            events[0].PersistOnEventId.Should().Be(eventC.Id);
            
            logProbe.Sender.Tell(new WriteSuccess(new[]{Event(events[0].Payload, 4L, eventC)}, write2.CorrelationId, instanceId));
            
            deliverProbe.ExpectMsg<ImmutableHashSet<long>>().Should().BeEmpty();
            persistProbe.ExpectMsg("c-2");
        }

        [Fact]
        public void PersistOnEventActor_must_confirm_persistence_with_self_emitted_events_only()
        {
            var actor = RecoveredTestActor(stateSync: true);
            var eventA = Event("a", 1L);
            actor.Tell(new Written(eventA));

            deliverProbe.ExpectMsg<ImmutableHashSet<long>>().Should().BeEquivalentTo(1L);
            
            var write = logProbe.ExpectMsg<Write>();
            var events = write.Events.ToArray();
            
            actor.Tell(new Written(Event("x-1", 2L, eventA, "B")));
            actor.Tell(new Written(Event("x-2", 3L, eventA, "B")));
            
            deliverProbe.ExpectMsg<ImmutableHashSet<long>>().Should().BeEquivalentTo(1L);
            deliverProbe.ExpectMsg<ImmutableHashSet<long>>().Should().BeEquivalentTo(1L);
            
            logProbe.Sender.Tell(new WriteSuccess(new[]
            {
                Event(events[0].Payload, 4L, eventA),
                Event(events[1].Payload, 5L, eventA),
            }, write.CorrelationId, instanceId));
            
            deliverProbe.ExpectMsg<ImmutableHashSet<long>>().Should().BeEmpty();
            deliverProbe.ExpectMsg<ImmutableHashSet<long>>().Should().BeEmpty();

            persistProbe.ExpectMsg("a-1");
            persistProbe.ExpectMsg("a-2");
        }

        [Fact]
        public void PersistOnEventActor_must_re_attempt_persistence_on_failed_write_after_restart()
        {
            var actor = RecoveredTestActor(stateSync: true);
            var eventA = Event("a", 1L);
            actor.Tell(new Written(eventA));

            deliverProbe.ExpectMsg<ImmutableHashSet<long>>().Should().BeEquivalentTo(1L);
            
            var write1 = logProbe.ExpectMsg<Write>();
            var events = write1.Events.ToArray();
            events[0].PersistOnEventId.Should().Be(eventA.Id);
            events[1].PersistOnEventId.Should().Be(eventA.Id);
            
            // application crash and restart
            logProbe.Sender.Tell(new WriteFailure(new[]
            {
                Event(events[0].Payload, 0L, eventA),
                Event(events[1].Payload, 0L, eventA),
            }, TestException.Instance, write1.CorrelationId, instanceId));

            var next = instanceId + 1;

            ProcessRecover(actor, next, Event("a", 1L));
            
            deliverProbe.ExpectMsg<ImmutableHashSet<long>>().Should().BeEquivalentTo(1L);
            
            var write2 = logProbe.ExpectMsg<Write>();
            events = write2.Events.ToArray();
            events[0].PersistOnEventId.Should().Be(eventA.Id);
            events[1].PersistOnEventId.Should().Be(eventA.Id);
            
            logProbe.Sender.Tell(new WriteSuccess(new[]
            {
                Event(events[0].Payload, 2L, eventA),
                Event(events[1].Payload, 3L, eventA),
            }, write2.CorrelationId, next));

            deliverProbe.ExpectMsg<ImmutableHashSet<long>>().Should().BeEmpty();
            deliverProbe.ExpectMsg<ImmutableHashSet<long>>().Should().BeEmpty();

            persistProbe.ExpectMsg("a-1");
            persistProbe.ExpectMsg("a-2");
        }

        [Fact]
        public void PersistOnEventActor_must_not_re_attempt_persistence_on_successful_write_after_restart()
        {
            var actor = RecoveredTestActor(stateSync: true);
            var eventA = Event("a", 1L);
            actor.Tell(new Written(eventA));

            deliverProbe.ExpectMsg<ImmutableHashSet<long>>().Should().BeEquivalentTo(1L);
            
            var write = logProbe.ExpectMsg<Write>();
            var events = write.Events.ToArray();
            events[0].PersistOnEventId.Should().Be(eventA.Id);
            events[1].PersistOnEventId.Should().Be(eventA.Id);
            
            logProbe.Sender.Tell(new WriteSuccess(new[]
            {
                Event(events[0].Payload, 2L, eventA),
                Event(events[1].Payload, 3L, eventA),
            }, write.CorrelationId, instanceId));
            
            deliverProbe.ExpectMsg<ImmutableHashSet<long>>().Should().BeEmpty();
            deliverProbe.ExpectMsg<ImmutableHashSet<long>>().Should().BeEmpty();
            
            persistProbe.ExpectMsg("a-1");
            persistProbe.ExpectMsg("a-2");
            
            actor.Tell("boom");
            ProcessRecover(actor, instanceId + 1,
                Event("a", 1L),
                Event(events[0].Payload, 2L, eventA),
                Event(events[1].Payload, 3L, eventA));
            
            deliverProbe.ExpectMsg<ImmutableHashSet<long>>().Should().BeEquivalentTo(1L);
            deliverProbe.ExpectMsg<ImmutableHashSet<long>>().Should().BeEmpty();
            deliverProbe.ExpectMsg<ImmutableHashSet<long>>().Should().BeEmpty();
            
            persistProbe.ExpectNoMsg(Timeout);
            persistProbe.ExpectNoMsg(Timeout);
        }

        [Fact]
        public void
            PersistOnEventActor_must_not_re_attempt_persistence_on_successful_write_of_events_without_PersistOnEventReference_after_restart()
        {
            var actor = RecoveredTestActor(stateSync: true);
            var eventA = Event("a", 1L);
            actor.Tell(new Written(eventA));

            deliverProbe.ExpectMsg<ImmutableHashSet<long>>().Should().BeEquivalentTo(1L);
            
            var write = logProbe.ExpectMsg<Write>();
            var events = write.Events.ToArray();
            events[0].PersistOnEventId.Should().Be(eventA.Id);
            events[1].PersistOnEventId.Should().Be(eventA.Id);

            var persistedOnA = new[]
            {
                Event(events[0].Payload, 2L),
                Event(events[0].Payload, 2L, eventA)
            };
            logProbe.Sender.Tell(new WriteSuccess(persistedOnA, write.CorrelationId, instanceId));
            
            deliverProbe.ExpectMsg<ImmutableHashSet<long>>().Should().BeEmpty();
            deliverProbe.ExpectMsg<ImmutableHashSet<long>>().Should().BeEmpty();

            persistProbe.ExpectMsg("a-1");
            persistProbe.ExpectMsg("a-2");
            
            actor.Tell("boom");
            ProcessRecover(actor, instanceId + 1, new[] {eventA, persistedOnA[0], persistedOnA[1]});

            deliverProbe.ExpectMsg<ImmutableHashSet<long>>().Should().BeEquivalentTo(1L);
            deliverProbe.ExpectMsg<ImmutableHashSet<long>>().Should().BeEmpty();
            deliverProbe.ExpectMsg<ImmutableHashSet<long>>().Should().BeEmpty();
            
            persistProbe.ExpectNoMsg(Timeout);
            persistProbe.ExpectNoMsg(Timeout);
        }

        [Fact]
        public void PersistOnEventActor_must_support_idempotent_event_persistence()
        {
            var actor = RecoveredTestActor(stateSync: true);
            actor.Tell(new Written(Event("boom", 1L)));

            var eventA = Event("a", 1L);
            ProcessRecover(actor, instanceId, eventA);
            
            var write = logProbe.ExpectMsg<Write>();
            var events = write.Events.ToArray();
            logProbe.Sender.Tell(new WriteSuccess(new[]
                {
                    Event(events[0].Payload, 2L, eventA),
                    Event(events[1].Payload, 3L, eventA)
                }, write.CorrelationId, instanceId + 1));
            
            logProbe.ExpectNoMsg(Timeout);
        }

        [Fact]
        public void PersistOnEventActor_must_save_snapshot_with_PersistOnEvent_requests()
        {
            var actor = RecoveredTestActor(stateSync: false);
            actor.Tell(new Written(Event("x", 1L)));

            var write = logProbe.ExpectMsg<Write>();
            
            actor.Tell("snap");

            var save = logProbe.ExpectMsg<SaveSnapshot>();
            var expected = new PersistOnEventRequest(1L, new EventId("logA", 1L), ImmutableArray.Create(
                new PersistOnEventInvocation("x-1", ImmutableHashSet<string>.Empty.Add("14")),
                new PersistOnEventInvocation("x-2", ImmutableHashSet<string>.Empty.Add("15"))), 
                instanceId);

            save.Snapshot.PersistOnEventRequests.First().Should().Be(expected);
        }

        [Fact]
        public void PersistOnEventActor_must_recover_from_snapshot_with_PersistOnEvent_requests_whose_execution_failed()
        {
            var actor = RecoveredTestActor(stateSync: false);
            actor.Tell(new Written(Event("x", 1L)));

            var write1 = logProbe.ExpectMsg<Write>();
            
            actor.Tell("snap");
            
            var save = logProbe.ExpectMsg<SaveSnapshot>();
            
            actor.Tell("boom");

            var next = instanceId + 1;
            logProbe.ExpectMsg(new LoadSnapshot("A", next));
            logProbe.Sender.Tell(new LoadSnapshotSuccess(save.Snapshot, next));
            logProbe.ExpectMsg(new Replay(actor, next, 2L));
            logProbe.Sender.Tell(new ReplaySuccess(Array.Empty<DurableEvent>(), 1L, next));
            
            var write2 = logProbe.ExpectMsg<Write>();
            write1.Events.Should().BeEquivalentTo(write2.Events);
        }

        [Fact]
        public void
            PersistOnEventActor_must_recover_from_snapshot_with_PersistOnEvent_requests_whose_execution_succeeded()
        {
            var actor = RecoveredTestActor(stateSync: false);
            var eventX = Event("x", 1L);
            actor.Tell(new Written(eventX));
            
            var write1 = logProbe.ExpectMsg<Write>();
            var events = write1.Events.ToArray();
            var written = new[]
            {
                Event(events[0].Payload, 2L, eventX),
                Event(events[1].Payload, 3L, eventX),
            };

            actor.Tell("snap");
            
            var save = logProbe.ExpectMsg<SaveSnapshot>();
            
            actor.Tell("boom");
            var next = instanceId + 1;

            logProbe.ExpectMsg(new LoadSnapshot("A", next));
            logProbe.Sender.Tell(new LoadSnapshotSuccess(save.Snapshot, next));
            logProbe.ExpectMsg(new Replay(actor, next, 2L));
            logProbe.Sender.Tell(new ReplaySuccess(written, 3L, next));
            logProbe.ExpectMsg(new Replay(null, next, 4L));
            logProbe.Sender.Tell(new ReplaySuccess(Array.Empty<DurableEvent>(), 3L, next));
            logProbe.ExpectNoMsg(Timeout);
        }

        [Fact]
        public void
            PersistOnEventActor_must_recover_from_snapshot_with_PersistOnEvent_requests_without_PersistOnEventReferences_whose_execution_succeeded()
        {
            var actor = RecoveredTestActor(stateSync: false);
            var eventX = Event("x", 1L);
            actor.Tell(new Written(eventX));
            
            var write1 = logProbe.ExpectMsg<Write>();
            var events = write1.Events.ToArray();
            var written = new[]
            {
                Event(events[0].Payload, 2L, eventX),
                Event(events[1].Payload, 3L, eventX),
            };

            actor.Tell("snap");
            
            var save = logProbe.ExpectMsg<SaveSnapshot>();
            var s = save.Snapshot;
            var snapshotWithoutReferences = new Snapshot(s.Payload, s.EmitterId, s.LastEvent, s.CurrentTime, s.SequenceNr, s.DeliveryAttempts, 
                persistOnEventRequests: s.PersistOnEventRequests.Select(r => new PersistOnEventRequest(r.PersistOnEventSequenceNr, null, r.Invocations, r.InstanceId)).ToImmutableHashSet());
            
            actor.Tell("boom");
            var next = instanceId + 1;
            
            logProbe.ExpectMsg(new LoadSnapshot("A", next));
            logProbe.Sender.Tell(new LoadSnapshotSuccess(snapshotWithoutReferences, next));
            logProbe.ExpectMsg(new Replay(actor, next, 2L));
            logProbe.Sender.Tell(new ReplaySuccess(written, 3L, next));
            logProbe.ExpectMsg(new Replay(null, next, 4L));
            logProbe.Sender.Tell(new ReplaySuccess(Array.Empty<DurableEvent>(), 3L, next));
            logProbe.ExpectNoMsg(Timeout);
        }

        [Fact]
        public void PersistOnEventActor_must_be_tolerant_to_changing_actor_paths_across_incarnations()
        {
            var actor = UnrecoveredTestActor(stateSync: false);
            var path = ActorPath.Parse("akka://test/user/invalid");
            var requests = ImmutableHashSet<PersistOnEventRequest>.Empty
                .Add(new PersistOnEventRequest(3L, new EventId("p-2", 2L), ImmutableArray.Create(new PersistOnEventInvocation("y")), instanceId))
                .Add(new PersistOnEventRequest(4L, null, ImmutableArray.Create(new PersistOnEventInvocation("z")), instanceId));
            var snapshot = new Snapshot("foo", "A", Event("x", 2), Timestamp(2), 2, persistOnEventRequests: requests);

            logProbe.ExpectMsg(new LoadSnapshot("A", instanceId));
            logProbe.Sender.Tell(new LoadSnapshotSuccess(snapshot, instanceId));
            logProbe.ExpectMsg(new Replay(actor, instanceId, 3L));
            logProbe.Sender.Tell(new ReplaySuccess(Array.Empty<DurableEvent>(), 2L, instanceId));

            var write1 = logProbe.ExpectMsg<Write>();
            var write2 = logProbe.ExpectMsg<Write>();

            write1.Events.First().Payload.Should().Be("y");
            write2.Events.First().Payload.Should().Be("z");
        }
    }
}