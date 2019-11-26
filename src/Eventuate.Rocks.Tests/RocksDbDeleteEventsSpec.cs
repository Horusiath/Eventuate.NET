#region copyright
// -----------------------------------------------------------------------
//  <copyright file="RocksDbDeleteEventsSpec.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Akka.Configuration;
using Eventuate.EventLogs;
using Eventuate.Tests;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Eventuate.Rocks.Tests
{
    public class RocksDbDeleteEventsSpec : MultiLocationSpecRocksDb
    {
        private static readonly Config SpecConfig = ConfigurationFactory.ParseString(@"
            eventuate.log.replication.retry-delay = 1s
            eventuate.log.replication.remote-read-timeout = 2s
            eventuate.log.recovery.remote-operation-retry-max = 10
            eventuate.log.recovery.remote-operation-retry-delay = 1s
            eventuate.log.recovery.remote-operation-timeout = 1s");

        public const string L1 = "L1";
        
        public RocksDbDeleteEventsSpec(ITestOutputHelper output) : base(output: output, config: SpecConfig) 
        {
        }

        private EventLogWriter Emitter(ReplicationEndpoint endpoint, string logName) => 
            new EventLogWriter(endpoint.System, $"{endpoint.Id}_Emitter", endpoint.Logs[logName]);

        private ReplicationConnection ReplicationConnection(int port) => new ReplicationConnection("127.0.0.1", port);

        [Fact]
        public async Task Deleting_events_must_not_replay_deleted_events_on_restart()
        {
            ReplicationEndpoint NewEndpointA(Location l) =>
                l.Endpoint(ImmutableHashSet.Create(L1), ImmutableHashSet<ReplicationConnection>.Empty, activate: false);
            
            Location NewLocationA() => Location("A", customConfig: SpecConfig);
            var locationA1 = NewLocationA();
            var endpointA1 = NewEndpointA(locationA1);

            var listenerA = locationA1.Listener(endpointA1.Logs[L1]);
            var emitterA = Emitter(endpointA1, L1);

            await emitterA.Write(Enumerable.Range(0, 5));
            listenerA.FishForMessage(msg => msg is int i && i == 5);

            (await endpointA1.Delete(L1, 3, ImmutableHashSet<string>.Empty))
                .Should().Be(3L);
            await locationA1.System.Terminate();

            using var locationA2 = NewLocationA();
            var endpointA2 = NewEndpointA(locationA2);
            locationA2.Listener(endpointA2.Logs[L1]).ExpectMsgAllOf(3, 4, 5);
        }

        [Fact]
        public async Task Conditionally_deleting_events_must_keep_events_available_for_corresponding_remote_log()
        {
            var locationA = Location("A", customConfig: SpecConfig);
            var locationB = Location("B", customConfig: SpecConfig);
            var locationC = Location("C", customConfig: SpecConfig);
            
            var endpointA = locationA.Endpoint(ImmutableHashSet.Create(L1), ImmutableHashSet.Create(ReplicationConnection(locationB.Port), ReplicationConnection(locationC.Port)), activate: false);
            var endpointB = locationA.Endpoint(ImmutableHashSet.Create(L1), ImmutableHashSet.Create(ReplicationConnection(locationA.Port)), activate: false);
            var endpointC = locationA.Endpoint(ImmutableHashSet.Create(L1), ImmutableHashSet.Create(ReplicationConnection(locationA.Port)), activate: false);

            var emitterA = Emitter(endpointA, L1);

            var listenerA = locationA.Listener(endpointA.Logs[L1]);
            var listenerB = locationA.Listener(endpointB.Logs[L1]);
            var listenerC = locationA.Listener(endpointC.Logs[L1]);

            await emitterA.Write(new[] {1, 2, 3, 4, 5});
            listenerA.FishForMessage(x => x is int i && i == 5);

            var r = await endpointA.Delete(L1, 3, ImmutableHashSet.Create(endpointB.Id, endpointC.Id));
            r.Should().Be(3);
            
            endpointA.Activate();
            endpointB.Activate();
            listenerB.ExpectMsgAllOf(1, 2, 3, 4, 5);
            
            endpointC.Activate();
            listenerC.ExpectMsgAllOf(1, 2, 3, 4, 5);
        }
    }
}