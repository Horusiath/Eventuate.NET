#region copyright
// -----------------------------------------------------------------------
//  <copyright file="EventsourcedViewSpec.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Collections.Generic;
using System.Linq;
using Akka.Actor;
using Akka.TestKit;
using Akka.TestKit.Xunit2;
using Xunit;
using Xunit.Abstractions;

namespace Eventuate.Tests.Abstractions
{
    public abstract class EventsourcedProcessorIntegrationSpec : TestKit
    {
        private readonly string sourceLogId;
        private readonly string targetLogId;
        private readonly IActorRef sourceLog;
        private readonly IActorRef targetLog;
        private readonly TestProbe sourceProbe;
        private readonly TestProbe targetProbe;
        private readonly TestProbe processorEventProbe;
        private readonly TestProbe processorProgressProbe;
        private readonly IActorRef a1;
        private readonly IActorRef a2;

        #region internal classes

        sealed class SampleActor : EventsourcedActor
        {
            private readonly IActorRef probe;

            public SampleActor(string id, IActorRef eventLog, IActorRef probe)
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
                    case string s:
                        Persist(s, _ => { });
                        return true;
                    default: return false;
                }
            }

            protected override bool OnEvent(object message)
            {
                probe.Tell((message.ToString(), LastVectorTimestamp));
                return true;
            }
        }
        
        sealed class StatelessSampleProcessor : EventsourcedProcessor
        {
            private readonly IActorRef eventProbe;
            private readonly IActorRef progressProbe;

            public StatelessSampleProcessor(string id, IActorRef eventLog, IActorRef targetEventLog, IActorRef eventProbe, IActorRef progressProbe)
            {
                this.eventProbe = eventProbe;
                this.progressProbe = progressProbe;
                Id = id;
                EventLog = eventLog;
                TargetEventLog = targetEventLog;
            }

            public override string Id { get; }
            public override IActorRef EventLog { get; }
            protected override bool OnCommand(object message)
            {
                switch (message)
                {
                    case "boom": 
                        eventProbe.Tell("died");
                        throw new IntergrationTestException();
                    case "snap":
                        Save("", result =>
                        {
                            if (result.IsSuccess) eventProbe.Tell("snapped");
                        });
                        return true;
                    default: return false;
                }
            }

            public override IActorRef TargetEventLog { get; }
            public override IEnumerable<object> ProcessEvent(object domainEvent)
            {
                if (domainEvent is string s && !s.Contains("processed"))
                {
                    eventProbe.Tell(s);
                    return new object[] {$"{s}-processed-1", $"{s}-processed-2"};
                }
                else return Enumerable.Empty<object>();
            }

            protected override bool OnSnapshot(object message) => true;

            public override void WriteSuccess(long progress)
            {
                base.WriteSuccess(progress);
                progressProbe.Tell(progress);
            }
        }
        
        sealed class StatefulSampleProcessor : StatefulProcessor
        {
            private readonly IActorRef eventProbe;
            private readonly IActorRef progressProbe;

            public StatefulSampleProcessor(string id, IActorRef eventLog, IActorRef targetEventLog, IActorRef eventProbe, IActorRef progressProbe)
            {
                this.eventProbe = eventProbe;
                this.progressProbe = progressProbe;
                Id = id;
                EventLog = eventLog;
                TargetEventLog = targetEventLog;
            }

            public override string Id { get; }
            public override IActorRef EventLog { get; }
            protected override bool OnCommand(object message)
            {
                switch (message)
                {
                    case "boom": 
                        eventProbe.Tell("died");
                        throw new IntergrationTestException();
                    case "snap":
                        Save("", result =>
                        {
                            if (result.IsSuccess) eventProbe.Tell("snapped");
                        });
                        return true;
                    default: return false;
                }
            }

            public override IActorRef TargetEventLog { get; }
            public override IEnumerable<object> ProcessEvent(object domainEvent)
            {
                if (domainEvent is string s && !s.Contains("processed"))
                {
                    eventProbe.Tell(s);
                    return new object[] {$"{s}-processed-1", $"{s}-processed-2"};
                }
                else return Enumerable.Empty<object>();
            }

            protected override bool OnSnapshot(object message) => true;

            public override void WriteSuccess(long progress)
            {
                base.WriteSuccess(progress);
                progressProbe.Tell(progress);
            }
        }

        #endregion

        protected EventsourcedProcessorIntegrationSpec(Func<string, Props> targetLogProps, Func<string, Props> sourceLogProps, string sourceLogId, ITestOutputHelper output) : base(output: output)
        {
            this.sourceLogId = sourceLogId;
            this.sourceLog = Sys.ActorOf(sourceLogProps(sourceLogId));
            
            this.targetLogId = $"{sourceLogId}_target";
            this.targetLog = Sys.ActorOf(targetLogProps(targetLogId));

            this.sourceProbe = CreateTestProbe("source-probe");
            this.targetProbe = CreateTestProbe("target-probe");
            
            this.processorEventProbe = CreateTestProbe("event-probe");
            this.processorProgressProbe = CreateTestProbe("progress-probe");

            this.a1 = Sys.ActorOf(Props.Create(() => new SampleActor("a1", sourceLog, sourceProbe.Ref)));
            this.a2 = Sys.ActorOf(Props.Create(() => new SampleActor("a2", targetLog, targetProbe.Ref)));
        }

        private IActorRef StatelessProcessor() =>
            Sys.ActorOf(Props.Create(() => new StatelessSampleProcessor("p", sourceLog, targetLog, processorEventProbe.Ref, processorProgressProbe.Ref)));

        private IActorRef StatefulProcessor(IActorRef source, IActorRef target) =>
            Sys.ActorOf(Props.Create(() => new StatefulSampleProcessor("p", source, target, processorEventProbe.Ref, processorProgressProbe.Ref)));

        private void WaitProgressWrite(long progress)
        {
            processorProgressProbe.FishForMessage(n => n is long num && num == progress);
        }

        [Fact]
        public void StatefulProcessor_must_write_processed_events_to_target_log_and_recover_from_scratch()
        {
            var p = StatefulProcessor(sourceLog, targetLog);
            
            a1.Tell("a");
            a1.Tell("b");
            a1.Tell("c");

            processorEventProbe.ExpectMsg("a");
            processorEventProbe.ExpectMsg("b");
            processorEventProbe.ExpectMsg("c");

            targetProbe.ExpectMsg(("a-processed-1", new VectorTime((sourceLogId, 1L), (targetLogId, 1L))));
            targetProbe.ExpectMsg(("a-processed-2", new VectorTime((sourceLogId, 1L), (targetLogId, 2L))));
            targetProbe.ExpectMsg(("b-processed-1", new VectorTime((sourceLogId, 2L), (targetLogId, 3L))));
            targetProbe.ExpectMsg(("b-processed-2", new VectorTime((sourceLogId, 2L), (targetLogId, 4L))));
            targetProbe.ExpectMsg(("c-processed-1", new VectorTime((sourceLogId, 3L), (targetLogId, 5L))));
            targetProbe.ExpectMsg(("c-processed-2", new VectorTime((sourceLogId, 3L), (targetLogId, 6L))));
            
            WaitProgressWrite(3L);
            
            p.Tell("boom");

            processorEventProbe.ExpectMsg("died"); // Fix #306: Make sure "boom" is processed before "d"
            
            a1.Tell("d");
            
            processorEventProbe.ExpectMsg("a");
            processorEventProbe.ExpectMsg("b");
            processorEventProbe.ExpectMsg("c");
            processorEventProbe.ExpectMsg("d");
            
            targetProbe.ExpectMsg(("d-processed-1", new VectorTime((sourceLogId, 4L), (targetLogId, 7L))));
            targetProbe.ExpectMsg(("d-processed-2", new VectorTime((sourceLogId, 4L), (targetLogId, 8L))));
        }
        
        [Fact]
        public void StatefulProcessor_must_write_processed_events_to_target_log_and_recover_from_snapshot()
        {
            var p = StatefulProcessor(sourceLog, targetLog);
            
            a1.Tell("a");
            a1.Tell("b");

            processorEventProbe.ExpectMsg("a");
            processorEventProbe.ExpectMsg("b");
            
            p.Tell("snap");
            
            processorEventProbe.ExpectMsg("snapped");
            
            a1.Tell("c");

            processorEventProbe.ExpectMsg("c");

            targetProbe.ExpectMsg(("a-processed-1", new VectorTime((sourceLogId, 1L), (targetLogId, 1L))));
            targetProbe.ExpectMsg(("a-processed-2", new VectorTime((sourceLogId, 1L), (targetLogId, 2L))));
            targetProbe.ExpectMsg(("b-processed-1", new VectorTime((sourceLogId, 2L), (targetLogId, 3L))));
            targetProbe.ExpectMsg(("b-processed-2", new VectorTime((sourceLogId, 2L), (targetLogId, 4L))));
            targetProbe.ExpectMsg(("c-processed-1", new VectorTime((sourceLogId, 3L), (targetLogId, 5L))));
            targetProbe.ExpectMsg(("c-processed-2", new VectorTime((sourceLogId, 3L), (targetLogId, 6L))));
            
            WaitProgressWrite(3L);
            
            p.Tell("boom");

            processorEventProbe.ExpectMsg("died"); // Fix #306: Make sure "boom" is processed before "d"
            
            a1.Tell("d");
            
            processorEventProbe.ExpectMsg("c");
            processorEventProbe.ExpectMsg("d");
            
            targetProbe.ExpectMsg(("d-processed-1", new VectorTime((sourceLogId, 4L), (targetLogId, 7L))));
            targetProbe.ExpectMsg(("d-processed-2", new VectorTime((sourceLogId, 4L), (targetLogId, 8L))));
        }
        
        [Fact]
        public void StatelessProcessor_must_write_processed_events_to_target_log_and_resume_from_stored_position()
        {
            var p = StatefulProcessor(sourceLog, targetLog);
            
            a1.Tell("a");
            a1.Tell("b");
            a1.Tell("c");

            processorEventProbe.ExpectMsg("a");
            processorEventProbe.ExpectMsg("b");
            processorEventProbe.ExpectMsg("c");
            
            targetProbe.ExpectMsg(("a-processed-1", new VectorTime((sourceLogId, 1L), (targetLogId, 1L))));
            targetProbe.ExpectMsg(("a-processed-2", new VectorTime((sourceLogId, 1L), (targetLogId, 2L))));
            targetProbe.ExpectMsg(("b-processed-1", new VectorTime((sourceLogId, 2L), (targetLogId, 3L))));
            targetProbe.ExpectMsg(("b-processed-2", new VectorTime((sourceLogId, 2L), (targetLogId, 4L))));
            targetProbe.ExpectMsg(("c-processed-1", new VectorTime((sourceLogId, 3L), (targetLogId, 5L))));
            targetProbe.ExpectMsg(("c-processed-2", new VectorTime((sourceLogId, 3L), (targetLogId, 6L))));
            
            WaitProgressWrite(3L);
            
            p.Tell("boom");

            processorEventProbe.ExpectMsg("died"); // Fix #306: Make sure "boom" is processed before "d"
            
            a1.Tell("d");
            
            processorEventProbe.ExpectMsg("d");
            
            targetProbe.ExpectMsg(("d-processed-1", new VectorTime((sourceLogId, 4L), (targetLogId, 7L))));
            targetProbe.ExpectMsg(("d-processed-2", new VectorTime((sourceLogId, 4L), (targetLogId, 8L))));
        }
    }
}