#region copyright
// -----------------------------------------------------------------------
//  <copyright file="EventsourcedProcessorSpec.cs" company="Bartosz Sypytkowski">
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
using Eventuate.ReplicationProtocol;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Eventuate.Tests
{
    public class EventsourcedProcessorSpec : TestKit
    {
        #region internal classes

        internal class StatelessTestProcessor2 : StatelessTestProcessor
        {
            private readonly ImmutableHashSet<string> customDestinationAggregateIds;

            public StatelessTestProcessor2(IActorRef srcProbe, IActorRef trgProbe, IActorRef appProbe, ImmutableHashSet<string> customDestinationAggregateIds) : base(srcProbe, trgProbe, appProbe)
            {
                this.customDestinationAggregateIds = customDestinationAggregateIds;
            }

            protected override DurableEvent PostProcessDurableEvent(DurableEvent e)
            {
                var e2 = new DurableEvent(
                    payload: e.Payload,
                    emitterId: e.EmitterId,
                    emitterAggregateId: e.EmitterAggregateId,
                    customDestinationAggregateIds: customDestinationAggregateIds,
                    systemTimestamp: e.SystemTimestamp,
                    vectorTimestamp: e.VectorTimestamp,
                    processId: e.ProcessId,
                    localLogId: e.LocalLogId,
                    localSequenceNr: e.LocalSequenceNr,
                    deliveryId: e.DeliveryId,
                    persistOnEventSequenceNr: e.PersistOnEventSequenceNr,
                    persistOnEventId: e.PersistOnEventId);
                return base.PostProcessDurableEvent(e);
            }

            public override IEnumerable<object> ProcessEvent(object domainEvent)
            {
                if (Equals(domainEvent, "x")) yield return domainEvent;
            }
        }
        
        internal class StatelessTestProcessor : EventsourcedProcessor
        {
            private readonly IActorRef appProbe;
            private ImmutableArray<string> processedEvents = ImmutableArray<string>.Empty;

            public StatelessTestProcessor(IActorRef srcProbe, IActorRef trgProbe, IActorRef appProbe)
            {
                this.appProbe = appProbe;
                this.EventLog = srcProbe;
                this.TargetEventLog = trgProbe;
            }

            public override string Id => "B";
            public override IActorRef EventLog { get; }
            public override IActorRef TargetEventLog { get; }
            public override int ReplayBatchSize => 2;

            protected override bool OnCommand(object message)
            {
                if (Equals(message,"state"))
                {
                    appProbe.Tell(processedEvents);
                    return true;
                }

                else return false;
            }

            public override IEnumerable<object> ProcessEvent(object domainEvent)
            {
                switch (domainEvent)
                {
                    case "x": return Enumerable.Repeat<object>("x", 4);
                    case "y": return Enumerable.Repeat<object>("y", 5);
                    case "z": return Enumerable.Repeat<object>("z", 11);
                    default:
                        processedEvents = processedEvents.Add(domainEvent.ToString());
                        return new[] {$"{domainEvent}-1", $"{domainEvent}-2"};
                }
            }

            public override void WriteSuccess(long write)
            {
                appProbe.Tell(write);
                base.WriteSuccess(write);
            }

            public override void WriteFailure(Exception cause)
            {
                appProbe.Tell(cause);
                base.WriteFailure(cause);
            }
        }

        internal sealed class StatefulTestProcessor : StatefulProcessor
        {
            private readonly IActorRef appProbe;
            private ImmutableArray<string> processedEvents = ImmutableArray<string>.Empty;

            public StatefulTestProcessor(IActorRef srcProbe, IActorRef trgProbe, IActorRef appProbe)
            {
                this.appProbe = appProbe;
                this.EventLog = srcProbe;
                this.TargetEventLog = trgProbe;
            }

            public override string Id => "B";
            public override IActorRef EventLog { get; }
            public override IActorRef TargetEventLog { get; }
            public override int ReplayBatchSize => 2;

            protected override bool OnCommand(object message)
            {
                if (Equals(message, "state"))
                {
                    appProbe.Tell(processedEvents);
                    return true;
                }

                else return false;
            }

            public override IEnumerable<object> ProcessEvent(object domainEvent)
            {
                switch (domainEvent)
                {
                    case "x": return Enumerable.Repeat<object>("x", 4);
                    case "y": return Enumerable.Repeat<object>("y", 5);
                    case "z": return Enumerable.Repeat<object>("z", 11);
                    default:
                        processedEvents = processedEvents.Add(domainEvent.ToString());
                        return new[] {$"{domainEvent}-1", $"{domainEvent}-2"};
                }
            }

            public override void WriteSuccess(long write)
            {
                appProbe.Tell(write);
                base.WriteSuccess(write);
            }

            public override void WriteFailure(Exception cause)
            {
                appProbe.Tell(cause);
                base.WriteFailure(cause);
            }
        }

        #endregion

        private static VectorTime Timestamp(long a = 0, long b = 0)
        {
            if (a == 0 && b == 0) return VectorTime.Zero;
            if (a == 0) return new VectorTime(("B", b));
            if (b == 0) return new VectorTime(("A", a));
            return new VectorTime(("A", a), ("B", b));
        }

        private static DurableEvent Event(object payload, long sequenceNr, DurableEvent persistOnEventEvent = null,
            string emitterId = null) =>
            new DurableEvent(payload, emitterId ?? "A", null,
                ImmutableHashSet<string>.Empty, default,
                Timestamp(sequenceNr), "A", "A", sequenceNr, null,
                persistOnEventEvent?.LocalSequenceNr, persistOnEventEvent?.Id);

        private static DurableEvent Update(DurableEvent e, object payload = null) =>
            new DurableEvent(
                payload: payload ?? e.Payload,
                emitterId: "B",
                emitterAggregateId: e.EmitterAggregateId,
                customDestinationAggregateIds: e.CustomDestinationAggregateIds,
                systemTimestamp: e.SystemTimestamp,
                vectorTimestamp: e.VectorTimestamp,
                processId: string.Empty,
                localLogId: string.Empty,
                localSequenceNr: 0L,
                deliveryId: e.DeliveryId,
                persistOnEventSequenceNr: e.PersistOnEventSequenceNr,
                persistOnEventId: e.PersistOnEventId);
        
        private static DurableEvent Copy(DurableEvent e, VectorTime vectorTime, ImmutableHashSet<string> customDestinationAggregateIds = null) =>
            new DurableEvent(
                payload: e.Payload,
                emitterId: e.EmitterId,
                emitterAggregateId: e.EmitterAggregateId,
                customDestinationAggregateIds: customDestinationAggregateIds ?? e.CustomDestinationAggregateIds,
                systemTimestamp: e.SystemTimestamp,
                vectorTimestamp: vectorTime,
                processId: e.ProcessId,
                localLogId: e.LocalLogId,
                localSequenceNr: e.LocalSequenceNr,
                deliveryId: e.DeliveryId,
                persistOnEventSequenceNr: e.PersistOnEventSequenceNr,
                persistOnEventId: e.PersistOnEventId);

        private static readonly DurableEvent eventA = Event("a", 1);
        private static readonly DurableEvent eventB = Event("b", 2);
        private static readonly DurableEvent eventC = Event("c", 3);

        private static readonly DurableEvent eventA1 = Update(eventA, "a-1");
        private static readonly DurableEvent eventA2 = Update(eventA, "a-2");
        private static readonly DurableEvent eventB1 = Update(eventB, "b-1");
        private static readonly DurableEvent eventB2 = Update(eventB, "b-2");
        private static readonly DurableEvent eventC1 = Update(eventC, "c-1");
        private static readonly DurableEvent eventC2 = Update(eventC, "c-2");

        private readonly int instanceId;
        private readonly TestProbe srcProbe;
        private readonly TestProbe trgProbe;
        private readonly TestProbe appProbe;

        private const string Config = @"
            akka.loglevel = DEBUG
            akka.log-dead-letters = on
            eventuate.log.write-batch-size = 10
            akka.test.single-expect-default = 20s

            eventuate.log {
              replay-retry-delay = 5ms
              replay-retry-max = 0
            }";
        
        public EventsourcedProcessorSpec(ITestOutputHelper output) : base(config: Config,
            output: output)
        {
            
            this.instanceId = EventsourcedView.InstanceIdCounter.Current;
            this.srcProbe = this.CreateTestProbe("srcProbe");
            this.trgProbe = this.CreateTestProbe("trgProbe");
            this.appProbe = this.CreateTestProbe("appProbe");
        }

        private IActorRef UnrecoveredStatelessProcessor() => Sys.ActorOf(Props.Create(() =>
            new StatelessTestProcessor(srcProbe.Ref, trgProbe.Ref, appProbe.Ref)));

        private IActorRef UnrecoveredStatefulProcessor() => Sys.ActorOf(Props.Create(() =>
            new StatefulTestProcessor(srcProbe.Ref, trgProbe.Ref, appProbe.Ref)));

        private IActorRef RecoveredStatelessProcessor()
        {
            var actor = UnrecoveredStatelessProcessor();
            ProcessRead(0);
            ProcessReplay(actor, 1);
            return actor;
        }

        private IActorRef RecoveredStatefulProcessor()
        {
            var actor = UnrecoveredStatefulProcessor();
            ProcessRead(0);
            ProcessLoad(actor);
            ProcessReplay(actor, 1);
            return actor;
        }

        private void ProcessLoad(IActorRef actor, int? instanceId = null)
        {
            var iid = instanceId ?? this.instanceId;
            srcProbe.ExpectMsg(new LoadSnapshot("B", iid));
            srcProbe.Sender.Tell(new LoadSnapshotSuccess(null, iid));
        }

        private void ProcessReplay(IActorRef actor, long fromSequenceNr, int? instanceId = null)
        {
            var iid = instanceId ?? this.instanceId;
            srcProbe.ExpectMsg(new Replay(actor, iid, fromSequenceNr, 2));
            srcProbe.Sender.Tell(new ReplaySuccess(Array.Empty<DurableEvent>(), 0L, iid));
        }

        private void ProcessRead(long progress, bool success = true)
        {
            trgProbe.ExpectMsg(new GetReplicationProgress("B"));
            if (success)
                ProcessResult(new GetReplicationProgressSuccess("B", progress, VectorTime.Zero));
            else
                ProcessResult(new GetReplicationProgressFailure(TestException.Instance));
        }

        private void ProcessPartialWrite(long progress, bool success = true, params DurableEvent[] events)
        {
            var metadata =
                ImmutableDictionary<string, ReplicationMetadata>.Empty.Add("B",
                    new ReplicationMetadata(progress, VectorTime.Zero));
            trgProbe.ExpectMsg(new ReplicationWrite(events, metadata));
            if (success)
                ProcessResult(new ReplicationWriteSuccess(events, metadata));
            else
                ProcessResult(new ReplicationWriteFailure(TestException.Instance));
        }

        private void ProcessWrite(long progress, bool success = true, params DurableEvent[] events)
        {
            ProcessPartialWrite(progress, success, events);
            if (success)
                appProbe.ExpectMsg(progress);
            else
                appProbe.ExpectMsg<AggregateException>().InnerException.Should().Be(TestException.Instance);
        }

        private void ProcessResult(object result) => trgProbe.Sender.Tell(result);

        [Fact]
        public void StatefulProcessor_must_recover()
        {
            RecoveredStatefulProcessor();
        }

        [Fact]
        public void StatefulProcessor_must_restart_on_failed_read_by_default()
        {
            var actor = UnrecoveredStatefulProcessor();
            ProcessRead(0, success: false);
            ProcessRead(0);
            ProcessLoad(actor, instanceId + 1);
            ProcessReplay(actor, 1, instanceId + 1);
        }

        [Fact]
        public void StatefulProcessor_must_recover_on_failed_write_by_default()
        {
            var actor = UnrecoveredStatefulProcessor();
            ProcessRead(0);
            ProcessLoad(actor);
            ProcessReplay(actor, 1);
            actor.Tell(new Written(eventA));
            ProcessWrite(1, success: false, eventA1, eventA2);
            ProcessRead(0);
            ProcessLoad(actor, instanceId + 1);
            ProcessReplay(actor, 1, instanceId + 1);
            actor.Tell(new Written(eventA));
            ProcessWrite(1, true, eventA1, eventA2);
        }

        [Fact]
        public void StatefulProcessor_must_write_to_target_log_during_recovery()
        {
            var actor = UnrecoveredStatefulProcessor();
            ProcessRead(0);
            ProcessLoad(actor);
            srcProbe.ExpectMsg(new Replay(actor, instanceId, 1, 2));
            srcProbe.Sender.Tell(new ReplaySuccess(new []{eventA, eventB}, eventB.LocalSequenceNr, instanceId));
            ProcessWrite(2, true, eventA1, eventA2, eventB1, eventB2);
            srcProbe.ExpectMsg(new Replay(null, instanceId, eventB.LocalSequenceNr + 1, 2));
            srcProbe.Sender.Tell(new ReplaySuccess(new []{eventC}, eventC.LocalSequenceNr, instanceId));
            ProcessWrite(3, true, eventC1, eventC2);
            srcProbe.ExpectMsg(new Replay(null, instanceId, eventC.LocalSequenceNr + 1, 2));
            srcProbe.Sender.Tell(new ReplaySuccess(Array.Empty<DurableEvent>(), eventC.LocalSequenceNr, instanceId));
        }

        [Fact]
        public void StatefulProcessor_must_write_to_target_log_and_process_concurrently()
        {
            var actor = RecoveredStatefulProcessor();
            actor.Tell(new Written(eventA));
            actor.Tell(new Written(eventB));
            actor.Tell(new Written(eventC));
            ProcessWrite(1, true, eventA1, eventA2);
            ProcessWrite(3, true, eventB1, eventB2, eventC1, eventC2);
        }

        [Fact]
        public void
            StatefulProcessor_must_exclude_events_from_write_with_sequenceNr_less_than_or_equal_to_storedSequenceNr()
        {
            var actor = UnrecoveredStatefulProcessor();
            ProcessRead(3);
            ProcessLoad(actor);
            ProcessReplay(actor, 1,  instanceId);
            actor.Tell(new Written(eventA));
            appProbe.ExpectMsg(3L);
        }

        [Fact]
        public void StatefulProcessor_must_include_events_to_write_with_sequenceNr_greater_than_storedSequenceNr()
        {
            var actor = UnrecoveredStatefulProcessor();
            ProcessRead(2);
            ProcessLoad(actor);
            ProcessReplay(actor, 1,  instanceId);
            actor.Tell(new Written(eventA));
            appProbe.ExpectMsg(2L);
            actor.Tell(new Written(eventB));
            appProbe.ExpectMsg(2L);
            actor.Tell(new Written(eventC));
            ProcessWrite(3, true, eventC1, eventC2);
            actor.Tell("state");
            appProbe.ExpectMsg<ImmutableArray<string>>().Should().BeEquivalentTo("a", "b", "c");
        }

        [Fact]
        public void StatefulProcessor_must_write_events_with_current_vector_time()
        {
            var actor = RecoveredStatefulProcessor();
            actor.Tell(new Written(Copy(eventA, Timestamp(1, 0))));
            actor.Tell(new Written(Copy(eventB, Timestamp(0, 1))));
            ProcessWrite(1, true, 
                Copy(eventA1, Timestamp(1, 0)),
                Copy(eventA2, Timestamp(1, 0)));
            ProcessWrite(2, true, 
                Copy(eventB1, Timestamp(1, 1)),
                Copy(eventB2, Timestamp(1, 1)));
        }

        [Fact]
        public void EventsourcedProcessor_must_resume()
        {
            RecoveredStatelessProcessor();
        }

        [Fact]
        public void EventsourcedProcessor_must_resume_on_failed_read_by_default()
        {
            var actor = UnrecoveredStatelessProcessor();
            ProcessRead(3);
            ProcessReplay(actor, 4);
        }

        [Fact]
        public void EventsourcedProcessor_must_resume_on_failed_write_by_default()
        {
            var actor = RecoveredStatelessProcessor();
            actor.Tell(new Written(eventA));
            ProcessWrite(1, false, eventA1, eventA2);
            ProcessRead(3);
            ProcessReplay(actor, 4, instanceId + 1);
        }

        [Fact]
        public void EventsourcedProcessor_must_write_events_with_source_event_vector_time()
        {
            var actor = RecoveredStatelessProcessor();
            actor.Tell(new Written(Copy(eventA, Timestamp(1, 0))));
            actor.Tell(new Written(Copy(eventB, Timestamp(0, 1))));
            
            ProcessWrite(1, true, 
                Copy(eventA1, Timestamp(1, 0)),
                Copy(eventA2, Timestamp(1, 0)));
            ProcessWrite(2, true, 
                Copy(eventB1, Timestamp(0, 1)),
                Copy(eventB2, Timestamp(0, 1)));
        }

        [Fact]
        public void
            EventsourcedProcessor_must_write_events_in_multiple_batches_if_the_number_of_generated_events_during_processing_since_the_last_write_is_greater_than_settings_writeBatchSize()
        {
            var actor = RecoveredStatelessProcessor();

            var evt1 = Copy(Event("x", 1), Timestamp(1, 0));
            var evt2 = Copy(Event("x", 2), Timestamp(2, 0));
            var evt3 = Copy(Event("x", 3), Timestamp(3, 0));
            var evt4 = Copy(Event("x", 4), Timestamp(4, 0));
            
            actor.Tell(new Written(evt1));    // ensure that a write is in progress when processing the next events
            actor.Tell(new Written(evt2));
            actor.Tell(new Written(evt3));
            actor.Tell(new Written(evt4));
            
            // process first write
            ProcessWrite(1, true, Enumerable.Repeat(Update(evt1), 4).ToArray());
            
            // process remaining writes
            ProcessPartialWrite(1, true, Enumerable.Repeat(Update(evt2), 4).Union(Enumerable.Repeat(Update(evt3), 4)).ToArray());
            ProcessWrite(4, true, Enumerable.Repeat(Update(evt4), 4).ToArray());
        }

        [Fact]
        public void
            EventsourcedProcessor_must_write_events_in_single_batch_if_the_number_of_generated_events_during_processing_since_the_last_write_is_equal_to_settings_writeBatchSize()
        { 
            var actor = RecoveredStatelessProcessor();

            var evt1 = Copy(Event("x", 1), Timestamp(1, 0));
            var evt2 = Copy(Event("y", 2), Timestamp(2, 0));
            var evt3 = Copy(Event("y", 3), Timestamp(3, 0));
            
            actor.Tell(new Written(evt1));    // ensure that a write is in progress when processing the next events
            actor.Tell(new Written(evt2));
            actor.Tell(new Written(evt3));
            
            // process first write
            ProcessWrite(1, true, Enumerable.Repeat(Update(evt1), 4).ToArray());
            
            // process remaining writes
            ProcessWrite(3, true, Enumerable.Repeat(Update(evt2), 5).Union(Enumerable.Repeat(Update(evt3), 5)).ToArray());
        }

        [Fact]
        public void
            EventsourcedProcessor_must_allow_batch_sizes_greater_than_settings_writeBatchSize_if_that_batch_was_generated_from_single_input_event_1()
        {
            var actor = RecoveredStatelessProcessor();

            var evt1 = Copy(Event("x", 1), Timestamp(1, 0));
            var evt2 = Copy(Event("x", 2), Timestamp(2, 0));
            var evt3 = Copy(Event("z", 3), Timestamp(3, 0));
            
            actor.Tell(new Written(evt1));    // ensure that a write is in progress when processing the next events
            actor.Tell(new Written(evt2));
            actor.Tell(new Written(evt3));
            
            // process first write
            ProcessWrite(1, true, Enumerable.Repeat(Update(evt1), 4).ToArray());
            
            // process remaining writes
            ProcessPartialWrite(1, true, Enumerable.Repeat(Update(evt2), 4).ToArray());
            ProcessWrite(3, true, Enumerable.Repeat(Update(evt3), 11).ToArray());
        }

        [Fact]
        public void
            EventsourcedProcessor_must_allow_batch_sizes_greater_than_settings_writeBatchSize_if_that_batch_was_generated_from_single_input_event_2()
        {
            var actor = RecoveredStatelessProcessor();

            var evt1 = Copy(Event("z", 1), Timestamp(1, 0));
            
            actor.Tell(new Written(evt1));
            ProcessWrite(1, true, Enumerable.Repeat(Update(evt1), 11).ToArray());
        }

        [Fact]
        public void
            EventsourcedProcessor_must_allow_to_set_CustomDestinationAggregateIds_using_PostProcessDurableEvent()
        {
            var customDestinationAggregateIds = ImmutableHashSet<string>.Empty.Add("aggregateId1").Add("aggregateId2");
            
            // create and recover processor
            var actor = Sys.ActorOf(Props.Create(() =>
                new StatelessTestProcessor2(srcProbe.Ref, trgProbe.Ref, appProbe.Ref, customDestinationAggregateIds)));
            ProcessRead(0);
            ProcessReplay(actor, 1);

            var evt1 = Event("x", 1);
            var expectedEvents = Copy(Update(evt1), evt1.VectorTimestamp, customDestinationAggregateIds);
            
            actor.Tell(new Written(evt1));
            
            ProcessWrite(1, true, expectedEvents);
        }
    }
}