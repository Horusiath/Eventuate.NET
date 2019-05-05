using Akka.Actor;
using Akka.TestKit.Xunit2;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;
using Xunit.Abstractions;
using static Eventuate.ReplicationEndpoint;

namespace Eventuate.Tests
{
    public class FailureDetectorSpec : TestKit
    {
        private readonly IActorRef failureDetector;

        public FailureDetectorSpec(ITestOutputHelper output) : base(output: output)
        {
            this.failureDetector = Sys.ActorOf(Props.Create(() => new FailureDetector("A", "L1", TimeSpan.FromMilliseconds(500))));
        }

        [Fact]
        public void A_replication_failure_detector_must_publish_availability_events_to_the_actor_systems_event_stream()
        {
            var cause1 = new Exception("1");
            var cause2 = new Exception("2");

            Sys.EventStream.Subscribe(TestActor, typeof(Available));
            Sys.EventStream.Subscribe(TestActor, typeof(Unavailable));

            failureDetector.Tell(FailureDetectorChange.AvailabilityDetected);
            ExpectMsg(new Available("A", "L1"));
            failureDetector.Tell(FailureDetectorChange.FailureDetected(cause1));
            // time passes ...
            ExpectMsg(new Unavailable("A", "L1", new[] {cause1}));
            failureDetector.Tell(FailureDetectorChange.AvailabilityDetected);
            failureDetector.Tell(FailureDetectorChange.AvailabilityDetected); // second AvailabilityDetected within limit doesn't publish another Available
            ExpectMsg(new Available("A", "L1"));
            failureDetector.Tell(FailureDetectorChange.FailureDetected(cause2));
            // time passes ...
            ExpectMsg(new Unavailable("A", "L1", new[] {cause2}));
            // time passes ...
            ExpectMsg(new Unavailable("A", "L1", Array.Empty<Exception>()));
        }
    }
}
