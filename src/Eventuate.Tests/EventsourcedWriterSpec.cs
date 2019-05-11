#region copyright
// -----------------------------------------------------------------------
//  <copyright file="EventsourcedWriterSpec.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.TestKit;
using Akka.TestKit.Xunit2;
using Eventuate.EventsourcingProtocol;
using Xunit;
using Xunit.Abstractions;

namespace Eventuate.Tests
{
    public class EventsourcedWriterSpec : TestKit
    {
        #region internal classes

        private sealed class TestEventsourcedWriter : EventsourcedWriter<string, string>
        {
            private readonly IActorRef appProbe;
            private readonly IActorRef rwProbe;
            private readonly long? readSuccessResult;
            private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(3);
            
            public TestEventsourcedWriter(IActorRef eventLog, IActorRef appProbe, IActorRef rwProbe, long? readSuccessResult)
            {
                this.appProbe = appProbe;
                this.rwProbe = rwProbe;
                this.readSuccessResult = readSuccessResult;
                EventLog = eventLog;
            }

            public override string Id => "B";
            public override int ReplayBatchSize => 2;
            public override IActorRef EventLog { get; }

            protected override bool OnCommand(object message)
            {
                appProbe.Tell(message);
                return true;
            }

            protected override bool OnEvent(object message)
            {
                if (message is string s)
                {
                    appProbe.Tell((s, LastSequenceNr));
                    return true;
                }

                return false;
            }

            public override Task<string> Read() => rwProbe.Ask<string>("r", Timeout);
            public override Task<string> Write() => rwProbe.Ask<string>("w", Timeout);
            public override long? ReadSuccess(string result)
            {
                appProbe.Tell(result);
                return readSuccessResult;
            }

            public override void WriteSuccess(string write)
            {
                appProbe.Tell(write);
            }

            public override void ReadFailure(Exception cause)
            {
                appProbe.Tell(cause);
                base.ReadFailure(cause);
            }

            public override void WriteFailure(Exception cause)
            {
                appProbe.Tell(cause);
                base.WriteFailure(cause);
            }
        }

        #endregion

        private static readonly Config TestConfig = ConfigurationFactory.ParseString(@"
          eventuate.log.replay-retry-max = 1
          eventuate.log.replay-retry-delay = 5ms");

        private readonly int instanceId;
        private readonly TestProbe logProbe;
        private readonly TestProbe appProbe;
        private readonly TestProbe rwProbe;
        
        public EventsourcedWriterSpec(ITestOutputHelper output) : base(config: TestConfig, output: output)
        {
            this.instanceId = EventsourcedView.InstanceIdCounter.Current;
            this.logProbe = CreateTestProbe();
            this.appProbe = CreateTestProbe();
            this.rwProbe = CreateTestProbe();
        }

        private IActorRef UnrecoveredEventsourcedWriter(long? readSuccessResult = null) =>
            Sys.ActorOf(Props.Create(() => new TestEventsourcedWriter(logProbe, appProbe, rwProbe, readSuccessResult)));

        private IActorRef RecoveredEventsourcedWriter(long? readSuccessResult = null) =>
            ProcessRecover(UnrecoveredEventsourcedWriter(readSuccessResult));
        
        private IActorRef ProcessRecover(IActorRef actor, long? readSuccessResult = null)
        {
            ProcessRead(Try.Success("rs"));
            if (readSuccessResult.HasValue)
            {
                ProcessReplay(actor, readSuccessResult.Value);
            }
            else
            {
                ProcessLoad(actor);
                ProcessReplay(actor, 1);
            }

            return actor;
        }

        private void ProcessLoad(IActorRef actor, int? iid = null)
        {
            var id = iid ?? instanceId;
            logProbe.ExpectMsg(new LoadSnapshot("B", id));
            logProbe.Sender.Tell(new LoadSnapshotSuccess(null, id));
        }
        
        private void ProcessReplay(IActorRef actor, long fromSequenceNr)
        {
            logProbe.ExpectMsg(new Replay(actor, instanceId, fromSequenceNr, 2));
            logProbe.Sender.Tell(new ReplaySuccess(Array.Empty<DurableEvent>(), 0, instanceId));
        }
        
        private void ProcessRead(Try<string> result)
        {
            rwProbe.ExpectMsg("r");
            ProcessResult(result);
        }
        
        private void ProcessWrite(Try<string> result)
        {
            rwProbe.ExpectMsg("w");
            ProcessResult(result);
        }
        
        private void ProcessResult(Try<string> result)
        {
            if (result.TryGetValue(out var s))
            {
                rwProbe.Sender.Tell(new Status.Success(s));
                appProbe.ExpectMsg(s);
            }
            else
            {
                var err = result.Exception;
                rwProbe.Sender.Tell(new Status.Failure(err));
                appProbe.ExpectMsg(err);
            }
        }
        
        private static VectorTime Timestamp(long a = 0, long b = 0)
        {
            if (a == 0 && b == 0) return VectorTime.Zero;
            if (a == 0) return new VectorTime(("B", b));
            if (b == 0) return new VectorTime(("A", a));
            return new VectorTime(("A", a), ("B", b));
        }

        private static DurableEvent Event(object payload, long sequenceNr, string emitterId = null) =>
            new DurableEvent(payload, emitterId ?? "A", null, ImmutableHashSet<string>.Empty, DateTime.MinValue,
                Timestamp(sequenceNr), "logB", "logA", sequenceNr);
        
        [Fact]
        public void EventsourcedWriter_when_recovering_must_recover_after_initial_read_with_undefined_return_value()
        {
            RecoveredEventsourcedWriter(null);
        }

        [Fact]
        public void EventsourcedWriter_when_recovering_must_restart_on_failed_read_by_default()
        {
            var actor = UnrecoveredEventsourcedWriter();
            ProcessRead(Try.Failure<string>(TestException.Instance));
            ProcessRead(Try.Success("rs"));
            ProcessLoad(actor, instanceId + 1);
            logProbe.ExpectMsg(new Replay(actor, instanceId + 1, 1L, 2));
            logProbe.Sender.Tell(new ReplaySuccess(Array.Empty<DurableEvent>(), 0L, instanceId + 1));
        }
        
        [Fact]
        public void EventsourcedWriter_when_recovering_must_restart_on_failed_write_by_default()
        {
            var actor = UnrecoveredEventsourcedWriter();
            ProcessRead(Try.Success("rs"));
            ProcessLoad(actor);
            logProbe.ExpectMsg(new Replay(actor, instanceId, 1L, 2));
            logProbe.Sender.Tell(new ReplaySuccess(new []{Event("a", 1)}, 1L, instanceId));
            appProbe.ExpectMsg(("a", 1));
            ProcessWrite(Try.Failure<string>(TestException.Instance));
            ProcessRead(Try.Success("rs"));
            
            var next = instanceId + 1;
            ProcessLoad(actor, next);
            logProbe.ExpectMsg(new Replay(actor, next, 1L, 2));
            logProbe.Sender.Tell(new ReplaySuccess(new []{Event("a", 1)}, 1L, next));
            appProbe.ExpectMsg(("a", 1));
            ProcessWrite(Try.Success("ws"));
        }

        [Fact]
        public void EventsourcedWriter_when_recovering_must_trigger_writes_when_recovery_is_suspended_and_completed()
        {
            var actor = UnrecoveredEventsourcedWriter();
            ProcessRead(Try.Success("rs"));
            ProcessLoad(actor);
            logProbe.ExpectMsg(new Replay(actor, instanceId, 1L, 2));
            logProbe.Sender.Tell(new ReplaySuccess(new []{Event("a", 1), Event("b", 2)}, 2L, instanceId));
            appProbe.ExpectMsg(("a", 1));
            appProbe.ExpectMsg(("b", 2));
            ProcessWrite(Try.Success("ws"));
            logProbe.ExpectMsg(new Replay(actor, instanceId, 3L, 2));
            logProbe.Sender.Tell(new ReplaySuccess(new[] {Event("c", 3)}, 3L, instanceId));
            appProbe.ExpectMsg(("c", 3));
            ProcessWrite(Try.Success("ws"));
        }

        [Fact]
        public void EventsourcedWriter_when_recovering_must_stash_commands_while_read_is_in_progress()
        {
            var actor = UnrecoveredEventsourcedWriter();
            actor.Tell("cmd");
            ProcessRead(Try.Success("rs"));
            ProcessLoad(actor);
            ProcessReplay(actor, 1);
            appProbe.ExpectMsg("cmd");
        }
        
        [Fact]
        public void EventsourcedWriter_when_recovering_must_retry_replay_on_failure_and_finally_succeed()
        {
            var actor = UnrecoveredEventsourcedWriter();
            actor.Tell("cmd");
            ProcessRead(Try.Success("rs"));
            ProcessLoad(actor);

            logProbe.ExpectMsg(new Replay(actor, instanceId, 1, 2));
            logProbe.Sender.Tell(new ReplayFailure(TestException.Instance, 1L, instanceId));

            logProbe.ExpectMsg(new Replay(null, instanceId, 1, 2));
            logProbe.Sender.Tell(new ReplaySuccess(Array.Empty<DurableEvent>(), 0L, instanceId));

            appProbe.ExpectMsg("cmd");
        }
        
        [Fact]
        public void EventsourcedWriter_when_recovering_must_retry_replay_on_failure_and_finally_fail()
        {
            var actor = UnrecoveredEventsourcedWriter();
            Watch(actor);
            
            ProcessRead(Try.Success("rs"));
            ProcessLoad(actor);
            

            logProbe.ExpectMsg(new Replay(actor, instanceId, 1, 2));
            logProbe.Sender.Tell(new ReplayFailure(TestException.Instance, 1L, instanceId));
            

            logProbe.ExpectMsg(new Replay(actor, instanceId, 1, 2));
            logProbe.Sender.Tell(new ReplayFailure(TestException.Instance, 1L, instanceId));

            ExpectTerminated(actor);
        }
        
        [Fact]
        public void EventsourcedWriter_when_recovering_must_stash_commands_while_write_is_in_progress_after_suspended_replay()
        {
            var actor = UnrecoveredEventsourcedWriter();
            ProcessRead(Try.Success("rs"));
            ProcessLoad(actor);
            logProbe.ExpectMsg(new Replay(actor, instanceId, 1, 2));
            logProbe.Sender.Tell(new ReplaySuccess(new []{Event("a", 1), Event("b", 2)}, 2L, instanceId));
            actor.Tell("cmd");
            appProbe.ExpectMsg(("a", 1));
            appProbe.ExpectMsg(("b", 2));
            ProcessWrite(Try.Success("ws"));
            logProbe.ExpectMsg(new Replay(null, instanceId, 3, 2));
            logProbe.Sender.Tell(new ReplaySuccess(new []{ Event("c", 3)}, 3L, instanceId));
            appProbe.ExpectMsg(("c", 3));
            ProcessWrite(Try.Success("ws"));
            logProbe.ExpectMsg(new Replay(null, instanceId, 4L, 2));
            logProbe.Sender.Tell(new ReplaySuccess(Array.Empty<DurableEvent>(), 3L, instanceId));
            appProbe.ExpectMsg("cmd");
        }
        
        [Fact]
        public void EventsourcedWriter_when_recovering_must_handle_commands_while_write_is_in_progress_after_completed_replay()
        {
            var actor = UnrecoveredEventsourcedWriter();
            ProcessRead(Try.Success("rs"));
            ProcessLoad(actor);
            ProcessReplay(actor, 1);
            actor.Tell("cmd");
            appProbe.ExpectMsg("cmd");
        }
        
        [Fact]
        public void EventsourcedWriter_when_recovering_must_stop_during_write_if_its_event_log_is_stopped()
        {
            var actor = UnrecoveredEventsourcedWriter();
            ProcessRead(Try.Success("rs"));
            ProcessLoad(actor);
            logProbe.ExpectMsg(new Replay(actor, instanceId, 1, 2));
            logProbe.Sender.Tell(new ReplaySuccess(new []{Event("a", 1), Event("b", 2)}, 2L, instanceId));
            appProbe.ExpectMsg(("a", 1));
            appProbe.ExpectMsg(("b", 2));
            rwProbe.ExpectMsg("w");

            Watch(actor);
            Sys.Stop(logProbe.Ref);
            ExpectTerminated(actor);
        }
        
        [Fact]
        public void EventsourcedWriter_when_resuming_must_replay_after_initial_read_using_the_defined_return_value_as_starting_position()
        {
            RecoveredEventsourcedWriter(3);
        }
        
        [Fact]
        public void EventsourcedWriter_when_resuming_must_stop_during_write_if_its_event_log_is_stopped()
        {
            var actor = UnrecoveredEventsourcedWriter(1);
            ProcessRead(Try.Success("rs"));
            ProcessLoad(actor);
            logProbe.ExpectMsg(new Replay(actor, instanceId, 1, 2));
            logProbe.Sender.Tell(new ReplaySuccess(new []{Event("a", 1), Event("b", 2)}, 2L, instanceId));
            appProbe.ExpectMsg(("a", 1));
            appProbe.ExpectMsg(("b", 2));
            rwProbe.ExpectMsg("w");
            
            Watch(actor);
            Sys.Stop(logProbe.Ref);
            ExpectTerminated(actor);
        }
        
        [Fact]
        public void EventsourcedWriter_when_recovered_must_handle_commands_while_write_is_in_progress()
        {
            var actor = ProcessRecover(UnrecoveredEventsourcedWriter());
            actor.Tell(new Written(Event("a", 1)));    // trigger write
            actor.Tell("cmd");
            appProbe.ExpectMsg(("a", 1));
            appProbe.ExpectMsg("cmd");
            ProcessWrite(Try.Success("ws"));
        }
        
        [Fact]
        public void EventsourcedWriter_when_recovered_must_handle_events_while_write_is_in_progress()
        {
            var actor = ProcessRecover(UnrecoveredEventsourcedWriter());
            actor.Tell(new Written(Event("a", 1)));    // trigger write 1
            actor.Tell(new Written(Event("b", 2)));    // trigger write 2 (after write 1 completed)
            
            appProbe.ExpectMsg(("a", 1));
            appProbe.ExpectMsg(("b", 2));
            ProcessWrite(Try.Success("ws"));
            ProcessWrite(Try.Success("ws"));
        }
        
        [Fact]
        public void EventsourcedWriter_when_recovered_must_stop_during_write_if_its_event_log_is_stopped()
        {
            var actor = ProcessRecover(UnrecoveredEventsourcedWriter());
            actor.Tell(new Written(Event("a", 1)));    // trigger write
            appProbe.ExpectMsg(("a", 1));
            rwProbe.ExpectMsg("w");

            Watch(actor);
            Sys.Stop(logProbe.Ref);
            ExpectTerminated(actor);
        }
    }
}