using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.TestKit.Xunit2;
using Eventuate.Streams;
using Xunit.Abstractions;
using Akka.Streams.TestKit;
using Akka.Util;
using FluentAssertions;
using Xunit;

namespace Eventuate.Tests.Streams
{
    public class BatchWriteStageSpec : TestKit
    {
        private readonly ActorMaterializer materializer;
        private readonly DurableEventWriterSettings settings;
        private readonly TestPublisher.Probe<ImmutableArray<DurableEvent>> src;
        private readonly TestSubscriber.Probe<IEnumerable<DurableEvent>> snk;

        public BatchWriteStageSpec(ITestOutputHelper output) : base(output: output)
        {
            this.materializer = Sys.Materializer();
            this.settings = new DurableEventWriterSettings(Sys.Settings.Config);

            this.src = this.CreatePublisherProbe<ImmutableArray<DurableEvent>>();
            this.snk = this.CreateSubscriberProbe<IEnumerable<DurableEvent>>();

            Source.FromPublisher(this.src)
                .Via(Flow.FromGraph(new BatchWriteStage(ec => Writer(ec))))
                .RunWith(Sink.FromSubscriber(this.snk), this.materializer);
        }

        private static int Random() => ThreadLocalRandom.Current.Next(100);
        
        private async Task<IEnumerable<DurableEvent>> Writer(IEnumerable<DurableEvent> events)
        {
            if (events.Any(e => e.Payload == "boom"))
                throw TestException.Instance;
            else
            {
                await Task.Delay(Random());
                return events;
            }
        }

        [Fact]
        public void BatchWriterStage_must_write_batches_sequentially()
        {
            var b1 = new[] {"a", "b", "c"}.Select(p => new DurableEvent(p)).ToImmutableArray();
            var b2 = new[] {"d", "e", "f"}.Select(p => new DurableEvent(p)).ToImmutableArray();
            var b3 = new[] {"g", "h", "i"}.Select(p => new DurableEvent(p)).ToImmutableArray();

            snk.Request(3);
            src.SendNext(b1);
            src.SendNext(b2);
            src.SendNext(b3);
            snk.ExpectNext().Should().BeEquivalentTo(b1);
            snk.ExpectNext().Should().BeEquivalentTo(b2);
            snk.ExpectNext().Should().BeEquivalentTo(b3);
        }
        
        [Fact]
        public void BatchWriterStage_must_fail_if_writer_fails()
        {
            var b = new[] {"a", "boom", "c"}.Select(p => new DurableEvent(p)).ToImmutableArray();

            snk.Request(3);
            src.SendNext(b);
            snk.ExpectError()
                .InnerException.Should().Be(TestException.Instance);
        }
    }
}