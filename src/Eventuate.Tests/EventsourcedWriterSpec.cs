using System;
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

    }
}