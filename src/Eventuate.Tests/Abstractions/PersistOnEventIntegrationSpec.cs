#region copyright
// -----------------------------------------------------------------------
//  <copyright file="EventsourcedViewSpec.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using Akka.Actor;
using Akka.TestKit;
using Akka.TestKit.Xunit2;
using Xunit;
using Xunit.Abstractions;

namespace Eventuate.Tests.Abstractions
{
    public abstract class PersistOnEventIntegrationSpec : TestKit
    {
        #region internal classes

        private readonly struct Ping
        {
            public int Num { get; }

            public Ping(int num)
            {
                Num = num;
            }
        }
        
        private readonly struct Pong
        {
            public int Num { get; }

            public Pong(int num)
            {
                Num = num;
            }
        }
        
        sealed class PingActor : PersistOnEventActor
        {
            private readonly IActorRef probe;

            public PingActor(string id, IActorRef eventLog, IActorRef probe)
            {
                this.probe = probe;
                Id = id;
                EventLog = eventLog;
            }

            public override string Id { get; }
            public override IActorRef EventLog { get; }
            protected override bool OnCommand(object message)
            {
                if (message is "serve")
                {
                    Persist(new Ping(1), _ => {});
                    return true;
                }
                else return false;
            }

            protected override bool OnEvent(object message)
            {
                switch (message)
                {
                    case Pong pong:
                        if (pong.Num == 10)
                            probe.Tell("done");
                        else
                            PersistOnEvent(new Ping(pong.Num + 1));
                        return true;
                    default: return false;
                }
            }
        }

        sealed class PongActor : PersistOnEventActor
        {
            private readonly IActorRef probe;

            public PongActor(string id, IActorRef eventLog, IActorRef probe)
            {
                this.probe = probe;
                Id = id;
                EventLog = eventLog;
            }

            public override string Id { get; }
            public override IActorRef EventLog { get; }
            protected override bool OnCommand(object message) => false;

            protected override bool OnEvent(object message)
            {
                if (message is Ping ping)
                {
                    PersistOnEvent(new Pong(ping.Num));
                    return true;
                }
                else return false;
            }
        }
        
        #endregion
        
        private readonly TestProbe probe;

        protected PersistOnEventIntegrationSpec(ITestOutputHelper output) : base(output: output)
        {
            probe = CreateTestProbe();
        }
        
        protected abstract IActorRef Log { get; }

        [Fact]
        public void Two_eventsourced_actors_can_play_event_ping_pong()
        {
            var ping = Sys.ActorOf(Props.Create(() => new PingActor("ping", Log, probe.Ref)));
            var pong = Sys.ActorOf(Props.Create(() => new PongActor("pong", Log, probe.Ref)));
            
            ping.Tell("serve");
            probe.ExpectMsg("done");
        }
    }
}