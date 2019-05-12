#region copyright
// -----------------------------------------------------------------------
//  <copyright file="FailureDetectorSpec.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using Akka.Actor;
using Akka.TestKit.Xunit2;
using System;
using System.Collections.Generic;
using System.Text;
using FluentAssertions;
using FluentAssertions.Extensions;
using Xunit;
using Xunit.Abstractions;
using static Eventuate.ReplicationEndpoint;

namespace Eventuate.Tests
{
    public class FailureDetectorSpec : TestKit
    {
        //private readonly IActorRef failureDetector;

        public FailureDetectorSpec(ITestOutputHelper output) : base(output: output)
        {
        }

        [Fact]
        public void A_replication_failure_detector_must_publish_availability_events_to_the_actor_systems_event_stream()
        {
            var probe = CreateTestProbe();
            var cause1 = new Exception("1");
            var cause2 = new Exception("2");

            Sys.EventStream.Subscribe(probe.Ref, typeof(Available));
            Sys.EventStream.Subscribe(probe.Ref, typeof(Unavailable));

            var failureDetector = Sys.ActorOf(Props.Create(() => new FailureDetector("A", "L1", 500.Milliseconds())));
            
            failureDetector.Tell(FailureDetectorChange.AvailabilityDetected);
            probe.ExpectMsg(new Available("A", "L1"));
            failureDetector.Tell(FailureDetectorChange.FailureDetected(cause1));
            // time passes ...
            probe.ExpectMsg(new Unavailable("A", "L1", new[] {cause1}));
            failureDetector.Tell(FailureDetectorChange.AvailabilityDetected);
            failureDetector.Tell(FailureDetectorChange.AvailabilityDetected); // second AvailabilityDetected within limit doesn't publish another Available
            probe.ExpectMsg(new Available("A", "L1"));
            failureDetector.Tell(FailureDetectorChange.FailureDetected(cause2));
            // time passes ...
            probe.ExpectMsg(new Unavailable("A", "L1", new[] {cause2}));
            // time passes ...
            probe.ExpectMsg(new Unavailable("A", "L1", Array.Empty<Exception>()));
        }
    }
}
