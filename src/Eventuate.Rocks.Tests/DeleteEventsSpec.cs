#region copyright
// -----------------------------------------------------------------------
//  <copyright file="DeleteEventsSpec.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using Akka.Configuration;
using Eventuate.EventLogs;
using Xunit.Abstractions;

namespace Eventuate.Rocks.Tests
{
    public class DeleteEventsSpec
    {
        public const string L1 = "L1";
        
        private static EventLogWriter Emitter(ReplicationEndpoint endpoint, string logName) =>
            new EventLogWriter(endpoint.System, $"{endpoint.Id}_Emitter", endpoint.Logs[logName]);

        private static Config Configuration = ConfigurationFactory.ParseString(@"
            eventuate.log.replication.retry-delay = 1s
            eventuate.log.replication.remote-read-timeout = 2s
            eventuate.log.recovery.remote-operation-retry-max = 10
            eventuate.log.recovery.remote-operation-retry-delay = 1s
            eventuate.log.recovery.remote-operation-timeout = 1s
        ");
        
        public DeleteEventsSpec(ITestOutputHelper output)
        {
        }
    }
}