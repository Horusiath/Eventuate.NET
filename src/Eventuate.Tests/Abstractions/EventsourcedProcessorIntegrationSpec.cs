#region copyright
// -----------------------------------------------------------------------
//  <copyright file="EventsourcedViewSpec.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System.Collections.Generic;
using System.Linq;
using Akka.Actor;
using Akka.TestKit.Xunit2;
using Xunit.Abstractions;

namespace Eventuate.Tests.Abstractions
{
    public class EventsourcedProcessorIntegrationSpec : TestKit
    {
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
        
        public EventsourcedProcessorIntegrationSpec(ITestOutputHelper output) : base(output: output)
        {
        }
        
        //TODO:
    }
}