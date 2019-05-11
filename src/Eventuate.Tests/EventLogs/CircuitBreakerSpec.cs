#region copyright
// -----------------------------------------------------------------------
//  <copyright file="CircuitBreakerSpec.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.TestKit;
using Akka.TestKit.Xunit2;
using Eventuate.EventLogs;
using Eventuate.EventsourcingProtocol;
using FluentAssertions;
using FluentAssertions.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace Eventuate.Tests.EventLogs
{
    public class CircuitBreakerSpec : TestKit
    {
        private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(3);
        private const string LogId = "logId";
        private static readonly Exception TestLogFailureException = new Exception("event-log-failure in test");
        
        internal sealed class TestLog : ReceiveActor
        {
            public TestLog()
            {
                ReceiveAny(msg => Sender.Tell($"re-{msg}"));
            }
        }

        private readonly IActorRef breaker;
        private readonly TestProbe probe;
        
        public CircuitBreakerSpec(ITestOutputHelper output) : base(output: output)
        {
            this.breaker = Sys.ActorOf(Props.Create(() => new CircuitBreaker(Props.Create<TestLog>(), false)));
            this.probe = CreateTestProbe();
        }

        [Fact]
        public async Task CircuitBreaker_must_be_closed_after_initialization()
        {
            (await breaker.Ask("a", Timeout)).Should().Be("re-a");
        }
        
        [Fact]
        public async Task CircuitBreaker_must_be_closed_after_initial_failure()
        {
            breaker.Tell(ServiceEvent.Failed(LogId, 0, TestLogFailureException));
            (await breaker.Ask("a", Timeout)).Should().Be("re-a");
        }
        
        [Fact]
        public async Task CircuitBreaker_must_open_after_first_failed_retry()
        {
            breaker.Tell(ServiceEvent.Failed(LogId, 1, TestLogFailureException));
            try
            {
                await breaker.Ask("a", Timeout);
            }
            catch (AggregateException e) when (e.Flatten().InnerException is EventLogUnavailableException)
            {
                // as expected   
            }
        }
        
        [Fact]
        public async Task CircuitBreaker_must_close_again_after_service_success()
        {
            breaker.Tell(ServiceEvent.Failed(LogId, 1, TestLogFailureException));
            try
            {
                await breaker.Ask("a", Timeout);
            }
            catch (AggregateException e) when (e.Flatten().InnerException is EventLogUnavailableException)
            {
                // as expected   
            }
            
            breaker.Tell(ServiceEvent.Normal(LogId));
            (await breaker.Ask("a", Timeout)).Should().Be("re-a");
        }
        
        [Fact]
        public async Task CircuitBreaker_must_close_again_after_service_initialization()
        {
            breaker.Tell(ServiceEvent.Failed(LogId, 1, TestLogFailureException));
            try
            {
                await breaker.Ask("a", Timeout);
            }
            catch (AggregateException e) when (e.Flatten().InnerException is EventLogUnavailableException)
            {
                // as expected   
            }
            
            breaker.Tell(ServiceEvent.Initialized(LogId));
            (await breaker.Ask("a", Timeout)).Should().Be("re-a");
        }
        
        [Fact]
        public async Task CircuitBreaker_must_reply_with_special_failure_message_on_Write_requests_if_open()
        {
            var events = new[] {new DurableEvent("a", "emitter")};
            breaker.Tell(ServiceEvent.Failed(LogId, 1, TestLogFailureException));
            breaker.Tell(new Write(events, probe.Ref, probe.Ref, 1, 2));
            probe.ExpectMsg(new WriteFailure(events, CircuitBreaker.Exception, 1, 2));
            probe.Sender.Should().Be(probe.Ref);
        }
        
        [Fact]
        public async Task CircuitBreaker_must_publish_ServiceFailed_once_on_event_stream_when_opened()
        {
            Sys.EventStream.Subscribe(probe.Ref, typeof(ServiceEvent));

            var serviceFailed = ServiceEvent.Failed(LogId, 1, TestLogFailureException);
            breaker.Tell(serviceFailed);
            probe.ExpectMsg(serviceFailed);
            
            breaker.Tell(ServiceEvent.Failed(LogId, 2, TestLogFailureException));
            probe.ExpectNoMsg(300.Milliseconds());
        }

        [Fact]
        public async Task CircuitBreaker_must_publish_ServiceNormal_once_on_event_stream_when_closed()
        {
            Sys.EventStream.Subscribe(probe.Ref, typeof(ServiceEvent));

            breaker.Tell(ServiceEvent.Failed(LogId, 1, TestLogFailureException));
            probe.FishForMessage(msg => msg is ServiceEvent e && e.Type == ServiceEvent.EventType.ServiceFailed);
            breaker.Tell(ServiceEvent.Normal(LogId));
            probe.ExpectMsg(ServiceEvent.Normal(LogId));
            
            breaker.Tell(ServiceEvent.Normal(LogId));
            probe.ExpectNoMsg(300.Milliseconds());
        }
    }
}