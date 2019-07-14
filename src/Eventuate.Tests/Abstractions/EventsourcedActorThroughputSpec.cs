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
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Akka.Actor;
using Akka.TestKit;
using Akka.TestKit.Xunit2;
using FluentAssertions.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace Eventuate.Tests.Abstractions
{
    public abstract class EventsourcedActorThroughputSpec : TestKit
    {
        #region internal classes

        sealed class Writer1 : EventsourcedActor
        {
            private readonly IActorRef probe;
            private readonly Stopwatch stopwatch = new Stopwatch();
            private int num = 0;

            public Writer1(string id, IActorRef eventLog, bool stateSync, IActorRef probe)
            {
                this.probe = probe;
                Id = id;
                EventLog = eventLog;
                StateSync = stateSync;
            }

            public override string Id { get; }
            public override IActorRef EventLog { get; }
            public override bool StateSync { get; }

            protected override bool OnCommand(object message)
            {
                switch (message)
                {
                    case "stats":
                        probe.Tell($"{1000.0 * num / stopwatch.ElapsedMilliseconds} events/sec.");
                        return true;
                    case string s:
                        Persist(s, result => result.ThrowIfFailure());
                        return true;
                    default: return false;
                }
            }

            protected override bool OnEvent(object message)
            {
                switch (message)
                {
                    case "start":
                        stopwatch.Start();
                        return true;
                    case "stop":
                        stopwatch.Stop();
                        probe.Tell(num);
                        return true;
                    case string s:
                        num++;
                        return true;
                    default: return false;
                }
            }
        }

        sealed class Writer2 : EventsourcedActor
        {
            private readonly IActorRef collector;

            public Writer2(string id, IActorRef eventLog, IActorRef collector)
            {
                this.collector = collector;
                Id = id;
                EventLog = eventLog;
            }

            public override string Id { get; }
            public override IActorRef EventLog { get; }
            protected override bool OnCommand(object message)
            {
                if (message is string s)
                {
                    Persist(s, result =>
                    {
                        if (result.TryGetValue(out var v)) collector.Tell(v);
                        else throw result.Exception;
                    });
                    return true;
                }
                else return false;
            }

            protected override bool OnEvent(object message) => message is "ignore";
        }
        
        sealed class Collector : ActorBase
        {
            private readonly int expectedReceives;
            private readonly IActorRef probe;
            private readonly Stopwatch stopwatch = new Stopwatch();
            private int num = 0;

            public Collector(int expectedReceives, IActorRef probe)
            {
                this.expectedReceives = expectedReceives;
                this.probe = probe;
            }

            protected override bool Receive(object message)
            {
                switch (message)
                {
                    case "stats":
                        probe.Tell($"{(1000.0 * num / stopwatch.ElapsedMilliseconds)} events/sec.");
                        return true;
                    case string s when num == 0:
                        stopwatch.Start();
                        num++;
                        return true;
                    case string s:
                        num++;
                        if (num == expectedReceives)
                        {
                            stopwatch.Stop();
                            probe.Tell(num);
                        }
                        return true;
                    default: return false;
                }
            }
        }
        
        #endregion
        
        private const int Operations = 1000;
        private static readonly TimeSpan Timeout = 60.Seconds();
        
        private readonly TestProbe probe;
        private readonly ImmutableArray<string> events;

        protected EventsourcedActorThroughputSpec(ITestOutputHelper output) : base(output: output)
        {
            probe = CreateTestProbe();
            events = Enumerable.Range(1, Operations).Select(i => $"e-{i}").ToImmutableArray();
        }

        protected abstract IActorRef Log { get; }
        
        private void Run(IActorRef actor)
        {
            actor.Tell("start");
            foreach (var e in events)
                actor.Tell(e);
            actor.Tell("stop");

            probe.ExpectMsg(Operations, Timeout);
            actor.Tell("stats");
            Sys.Log.Info("STATS: {0}", probe.ReceiveOne(Timeout));
        }

        [Fact]
        public void EventsourcedActor_configured_with_stateSync_on_must_have_some_acceptable_throughtput()
        {
            var props = Props.Create(() => new Writer1("p", Log, true, probe.Ref));
            Run(Sys.ActorOf(props));
        }
        
        [Fact]
        public void EventsourcedActor_configured_with_stateSync_off_must_have_some_acceptable_throughtput()
        {
            var props = Props.Create(() => new Writer1("p", Log, false, probe.Ref));
            Run(Sys.ActorOf(props));
        }

        [Fact]
        public void Several_EventsourcedActor_configured_with_stateSync_on_must_have_some_reasonable_overall_throughtput()
        {
            var probe = CreateTestProbe();

            var numActors = 10;
            var numWrites = numActors * Operations;

            var collector = Sys.ActorOf(Props.Create(() => new Collector(numWrites, probe.Ref)));
            var actors = Enumerable.Range(1, numActors)
                .Select(i => Sys.ActorOf(Props.Create(() => new Writer2($"p-{i}", Log, collector))))
                .ToImmutableArray();

            foreach (var e in events)
            foreach (var actor in actors)
            {
                actor.Tell(e);
            }

            probe.ExpectMsg(numWrites, Timeout);
            collector.Tell("stats");
            Sys.Log.Info("STATS: {0}", probe.ReceiveOne(Timeout));
        }
    }
}