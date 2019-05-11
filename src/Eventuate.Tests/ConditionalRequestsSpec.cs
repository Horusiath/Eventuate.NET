#region copyright
// -----------------------------------------------------------------------
//  <copyright file="ConditionalRequestsSpec.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using Akka.Actor;
using Akka.TestKit.Xunit2;
using Eventuate.EventsourcingProtocol;
using Xunit;
using Xunit.Abstractions;

namespace Eventuate.Tests
{
    public class ConditionalRequestsSpec : TestKit
    {
        #region internal classes

        internal sealed class ConditionalRequestReceiver : ConditionalRequestActor
        {
            public ConditionalRequestReceiver(IActorRef eventLog)
            {
                EventLog = eventLog;
            }

            public override string Id => "test";
            public override IActorRef EventLog { get; }
            protected override bool OnCommand(object message)
            {
                switch (message)
                {
                    case VectorTime t: VersionChanged(t); return true;
                    default: Sender.Tell($"re: {message}"); return true;
                }
            }

            protected override bool OnEvent(object message) => true;
        }

        #endregion
        
        private static VectorTime Timestamp(long timeA, long timeB) => new VectorTime(("A", timeA), ("B", timeB));

        private readonly int instanceId;
        private readonly IActorRef receiver;
        
        public ConditionalRequestsSpec(ITestOutputHelper output) : base(output: output)
        {
            var probe = CreateTestProbe();
            this.instanceId = EventsourcedView.InstanceIdCounter.Current;
            this.receiver = Sys.ActorOf(Props.Create(() => new ConditionalRequestReceiver(probe.Ref)));

            probe.ExpectMsg(new LoadSnapshot("test", instanceId));
            probe.Sender.Tell(new LoadSnapshotSuccess(null, instanceId));
            probe.ExpectMsg(new Replay(receiver, instanceId, 1L));
            probe.Sender.Tell(new ReplaySuccess(Array.Empty<DurableEvent>(), 0L, instanceId));
        }

        [Fact]
        public void ConditionalRequests_must_send_conditional_request_immediately_if_condition_is_already_met()
        {
            receiver.Tell(new ConditionalRequest(Timestamp(0,0), "a"));
            ExpectMsg("re: a");
        }
        
        [Fact]
        public void ConditionalRequests_must_delay_conditional_request_if_condition_is_not_met()
        {
            receiver.Tell(new ConditionalRequest(Timestamp(0, 0), "a"));
            receiver.Tell(new ConditionalRequest(Timestamp(1, 0), "b"));
            receiver.Tell(new ConditionalRequest(Timestamp(0, 0), "c"));
            ExpectMsg("re: a");
            ExpectMsg("re: c");
        }

        [Fact]
        public void ConditionalRequests_must_send_conditional_request_later_if_condition_is_met_after_update()
        {
            receiver.Tell(new ConditionalRequest(Timestamp(0, 0), "a"));
            receiver.Tell(new ConditionalRequest(Timestamp(1, 0), "b"));
            receiver.Tell(new ConditionalRequest(Timestamp(0, 0), "c"));
            receiver.Tell(Timestamp(1, 0));
            ExpectMsg("re: a");
            ExpectMsg("re: c");
            ExpectMsg("re: b");
        }

        [Fact]
        public void ConditionalRequests_must_send_delayed_conditional_request_in_correct_order_if_condition_is_met()
        {
            receiver.Tell(new ConditionalRequest(Timestamp(1, 0), "a"));
            receiver.Tell(new ConditionalRequest(Timestamp(2, 0), "b"));
            receiver.Tell(new ConditionalRequest(Timestamp(3, 0), "c"));
            receiver.Tell(new ConditionalRequest(Timestamp(0, 0), "x"));
            receiver.Tell(Timestamp(3, 0));
            ExpectMsg("re: x");
            ExpectMsg("re: a");
            ExpectMsg("re: b");
            ExpectMsg("re: c");
        }

        [Fact]
        public void ConditionalRequests_must_send_delayed_conditional_request_in_batches_1()
        {
            for (int i = 1; i <= 1000; i++)
            {
                receiver.Tell(new ConditionalRequest(Timestamp(i, 0), i));
                receiver.Tell(Timestamp(i, 0));
            }
            
            for (int i = 1; i <= 1000; i++)
                ExpectMsg($"re: {i}");
        }
        
        [Fact]
        public void ConditionalRequests_must_send_delayed_conditional_request_in_batches_2()
        {
            for (int i = 1; i <= 1000; i++)
                receiver.Tell(new ConditionalRequest(Timestamp(i, 0), i));
            
            for (int i = 1; i <= 1000; i++)
                receiver.Tell(Timestamp(i, 0));

            for (int i = 1; i <= 1000; i++)
                ExpectMsg($"re: {i}");
        }
    }
}