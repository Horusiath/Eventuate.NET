#region copyright
// -----------------------------------------------------------------------
//  <copyright file="LocationSpecRocksDb.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.TestKit.Xunit2;
using Eventuate.EventLogs;
using Eventuate.Snapshots.Filesystem;
using Eventuate.Tests;
using Eventuate.Tests.EventLogs;
using Xunit.Abstractions;

namespace Eventuate.Rocks.Tests
{
    public abstract class SingleLocationSpecRocksDb : TestKit
    {
        #region internal classes
        
        public sealed class RestarterActor : ReceiveActor
        {
            public readonly struct RestartMsg{}

            public static Props Props(Props props, string? name = null) =>
                Akka.Actor.Props.Create(() => new RestarterActor(props, name));

            public static Task<IActorRef> Restart(IActorRef restarter) =>
                restarter.Ask<IActorRef>(default(RestartMsg));
            
            public RestarterActor(Props props, string? name)
            {
                var child = Context.ActorOf(props, name);
                IActorRef requester = null;
                Receive<RestartMsg>(_ =>
                {
                    requester = Sender;
                    Context.Watch(child);
                    Context.Stop(child);
                });
                Receive<Terminated>(t =>
                {
                    child = Context.ActorOf(props, name);
                    requester.Tell(child);
                });
                ReceiveAny(child.Forward);
            }
        }

        public class TestEventLog : RocksDbEventLog
        {
            public TestEventLog(string id) : this(id, DateTime.MinValue) { }
            public TestEventLog(string id, DateTime currentSystemTime): base(id, "log-test")
            {
                CurrentSystemTime = currentSystemTime;
            }

            protected override DateTime CurrentSystemTime { get; }

            protected override void Unhandled(object message)
            {
                switch (message)
                {
                    case "boom": throw IntegrationTestException.Instance;
                    case "dir": Sender.Tell(Settings.Dir); break;
                    default: base.Unhandled(message); break;
                }
                
            }

            public override Task<BatchReadResult> Read(long fromSequenceNr, long toSequenceNr, int max)
            {
                if (fromSequenceNr == ErrorSequenceNr) 
                    return Task.FromException<BatchReadResult>(IntegrationTestException.Instance);
                
                return base.Read(fromSequenceNr, toSequenceNr, max);
            }

            public override Task<BatchReadResult> Read(long fromSequenceNr, long toSequenceNr, int max, string aggregateId)
            {
                if (fromSequenceNr == ErrorSequenceNr) 
                    return Task.FromException<BatchReadResult>(IntegrationTestException.Instance);

                return base.Read(fromSequenceNr, toSequenceNr, max, aggregateId);
            }

            public override Task<BatchReadResult> ReplicationRead(long fromSequenceNr, long toSequenceNr, int max, int scanLimit, Func<DurableEvent, bool> filter)
            {
                if (fromSequenceNr == ErrorSequenceNr) 
                    return Task.FromException<BatchReadResult>(IntegrationTestException.Instance);

                return base.ReplicationRead(fromSequenceNr, toSequenceNr, max, scanLimit, filter);
            }

            public override Task Write(IReadOnlyCollection<DurableEvent> events, long partition, EventLogClock clock, IActorContext context)
            {
                foreach (var e in events)
                {
                    if (e.Payload.ToString().Contains("boom"))
                        throw IntegrationTestException.Instance;
                }
                return base.Write(events, partition, clock, context);
            }

            protected override long AdjustFromSequenceNr(long sequenceNr)
            {
                return sequenceNr switch
                {
                    ErrorSequenceNr => sequenceNr,
                    IgnoreDeletedSequenceNr => 0,
                    _ => base.AdjustFromSequenceNr(sequenceNr)
                };
            }
        }
        
        #endregion

        public const long ErrorSequenceNr = -1L;
        public const long IgnoreDeletedSequenceNr = -2L;

        private static int logCounter = 0;
        private static int GenerateLogCounter() => Interlocked.Increment(ref logCounter);

        private readonly int logCtr;
        private readonly Lazy<IActorRef> log;

        private readonly string[] storageLocations;
        
        protected SingleLocationSpecRocksDb(ITestOutputHelper output, Config config) : base(config, null, output)
        {
            logCtr = GenerateLogCounter();
            log = new Lazy<IActorRef>(() => Sys.ActorOf(LogProps(LogId)));
            storageLocations = new[]
            {
                Sys.Settings.Config.GetString("eventuate.log.rocksdb.dir"),
                Sys.Settings.Config.GetString("eventuate.snapshot.filesystem.dir")
            };
            CleanupLocations();
            CreateLocations();
        }

        private void CreateLocations()
        {
            foreach (var location in storageLocations)
            {
                if (!Directory.Exists(location))
                    Directory.CreateDirectory(location);
            }
        }

        private void CleanupLocations()
        {
            foreach (var location in storageLocations)
            {
                if (Directory.Exists(location))
                    Directory.Delete(location);
            }
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            CleanupLocations();
        }

        protected IActorRef Log => log.Value;
        protected string LogId => logCtr.ToString();
        protected virtual bool Batching => true;
        protected virtual DateTime CurrentSystemTime => DateTime.MinValue;

        public Props LogProps(string id)
        {
            var props = Props.Create(() => new TestEventLog(id, CurrentSystemTime)).WithDispatcher("eventuate.log.dispatchers.write-dispatcher");
            if (Batching)
                return RestarterActor.Props(Props.Create(() => new BatchingLayer(props)));
            else
                return RestarterActor.Props(props);
        }
    }
    
    public abstract class MultiLocationSpecRocksDb : MultiLocationSpec
    {
        protected static readonly Config ProviderConfigBackup = ConfigurationFactory.ParseString(@"
            eventuate.log.leveldb.dir = target/test-log
            eventuate.log.leveldb.index-update-limit = 3
            eventuate.log.leveldb.deletion-retry-delay = 1 ms");
        
        protected MultiLocationSpecRocksDb() : base(ProviderConfigBackup)
        {
        }
        
        protected MultiLocationSpecRocksDb(Config config) : base(config.WithFallback(ProviderConfigBackup))
        {
        }
        
        public override Props LogFactory(string id) => RocksDbEventLog.Props(id);
    }
}