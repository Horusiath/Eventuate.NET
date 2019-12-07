#region copyright
// -----------------------------------------------------------------------
//  <copyright file="RocksDbEventLogSpec.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Eventuate.EventsourcingProtocol;
using Eventuate.ReplicationProtocol;
using Eventuate.Tests.EventLogs;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Eventuate.Rocks.Tests
{
    public class RocksDbEventLogSpec : EventLogSpec
    {
        private static readonly Config Configuration = ConfigurationFactory.ParseString(@"
            eventuate.log.leveldb.dir = target/test-log
            eventuate.log.leveldb.index-update-limit = 6
            eventuate.log.leveldb.deletion-batch-size = 2
            eventuate.log.leveldb.deletion-retry-delay = 1ms").WithFallback(EventLogSpec.DefaultConfig);
        
        public RocksDbEventLogSpec(ITestOutputHelper output) : base(output, Configuration)
        {
        }

        [Fact]
        public async Task RocksDb_EventLog_must_not_delete_events_required_for_restoring_EventClock()
        {
            this.GenerateEmittedEvents();
            var currentEventLogClock = await Log.Ask<GetEventLogClockSuccess>(new GetEventLogClock());
            await Log.Ask(new Delete(this.generatedEmittedEvents.Last().LocalSequenceNr));
            var restartedLog = await SingleLocationSpecRocksDb.RestarterActor.Restart(Log);
            var restoredEventLogClock = await restartedLog.Ask<GetEventLogClockSuccess>(new GetEventLogClock());
            restoredEventLogClock.Should().Be(currentEventLogClock);
        }

        #region EventLogSpec impl

        

        #endregion
    }
}