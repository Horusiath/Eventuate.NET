#region copyright
// -----------------------------------------------------------------------
//  <copyright file="RocksDbRecoverySpec.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Pattern;
using Akka.TestKit;
using Akka.TestKit.Xunit2;
using Eventuate.EventLogs;
using Eventuate.ReplicationProtocol;
using Eventuate.Tests;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Eventuate.Rocks.Tests
{
    public class RocksDbRecoverySpec : MultiLocationSpecRocksDb
    {
        sealed class ConvergenceView : EventsourcedView
        {
            private readonly int expectedSize;
            private readonly IActorRef probe;
            private ImmutableSortedSet<string> state = ImmutableSortedSet<string>.Empty;

            public ConvergenceView(string id, IActorRef eventLog, int expectedSize, IActorRef probe)
            {
                this.expectedSize = expectedSize;
                this.probe = probe;
                Id = id;
                EventLog = eventLog;
            }

            public override string Id { get; }
            public override IActorRef EventLog { get; }
            protected override bool OnCommand(object message) => true;

            protected override bool OnEvent(object message)
            {
                switch (message)
                {
                    case string s:
                        state = state.Add(s);
                        if (state.Count == expectedSize) probe.Tell(state);
                        return true;
                    default: return false;
                }
            }
        }

        sealed class FailOnGetReplicationProgress : ReceiveActor
        {
            public FailOnGetReplicationProgress(Props logProps, Exception exception)
            {
                var logActor = Context.ActorOf(logProps);
                Receive<GetReplicationProgress>(_ => Sender.Tell(new GetReplicationProgressesFailure(exception)));
                ReceiveAny(logActor.Forward);
            }
        }
        
        sealed class PrefixesFilter : ReplicationFilter
        {
            private readonly string[] prefixes;

            public PrefixesFilter(params string[] prefixes)
            {
                this.prefixes = prefixes;
            }

            public override bool Invoke(DurableEvent durableEvent) => 
                durableEvent.Payload is string s && prefixes.Any(s.StartsWith);
        }
        
        private static EventLogWriter Writer(ReplicationEndpoint endpoint) =>
            new EventLogWriter(endpoint.System, $"Writer-{endpoint.Id}", endpoint.Logs["L1"]);

        private static void AssertConvergence(ImmutableSortedSet<string> expected, params ReplicationEndpoint[] endpoints)
        {
            var probes = new List<TestProbe>();
            foreach (var endpoint in endpoints)
            {
                var probe = new TestProbe(endpoint.System, new XunitAssertions());
                endpoint.System.ActorOf(Props.Create(() =>
                    new ConvergenceView($"p-{endpoint.Id}", endpoint.Logs["L1"], expected.Count, probe.Ref)));
                probes.Add(probe);
            }

            foreach (var probe in probes)
            {
                probe.ExpectMsg(expected);
            }
        }
        
        private static ImmutableHashSet<T> Set<T>(params T[] values) =>
            ImmutableHashSet.Create<T>(values);
        
        private static ReplicationConnection ReplicationConnection(int port) =>
            new ReplicationConnection("127.0.0.1", port);
        private static string RootDirectory(ReplicationTarget target) =>
            new RocksDbSettings(target.Endpoint.System.Settings.Config).Dir;

        private static Task<string> LogDirectory(ReplicationTarget target) =>
            target.Log.Ask<string>("dir");
        
        private static readonly Config SpecConfig = ConfigurationFactory.ParseString(@"
            eventuate.log.replication.retry-delay = 1s
            eventuate.log.replication.remote-read-timeout = 2s
            eventuate.log.recovery.remote-operation-retry-max = 10
            eventuate.log.recovery.remote-operation-retry-delay = 1s
            eventuate.log.recovery.remote-operation-timeout = 1s");
        
        public RocksDbRecoverySpec(ITestOutputHelper output): base(output: output, config: SpecConfig)
        {
        }

        protected int CustomPort = 2555;

        public override Props LogFactory(string id) => SingleLocationSpecRocksDb.TestEventLog.Props(id, batching: true);

        [Fact]
        public async Task Replication_endpoint_recovery_must_disallow_activation_of_endpoint_during_and_after_recovery()
        {
            
            var locationA = Location("A", customConfig: SpecConfig);
            var locationB = Location("B", customConfig: SpecConfig);

            var endpointA = locationA.Endpoint(Set("L1"), Set(ReplicationConnection(locationB.Port)), activate: false);
            locationB.Endpoint(Set("L1"), Set(ReplicationConnection(locationA.Port)));

            var recovery = endpointA.Recover();
            Xunit.Assert.Throws<IllegalStateException>(endpointA.Activate);
            await recovery;
            Xunit.Assert.Throws<IllegalStateException>(endpointA.Activate);
        }
        
        [Fact]
        public async Task Replication_endpoint_recovery_must_fail_when_connected_endpoint_is_unavailable()
        {
            var locationA = Location("A",
                customConfig: ConfigurationFactory.ParseString("eventuate.log.recovery.remote-operation-retry-max = 0")
                    .WithFallback(SpecConfig));
            var endpointA = locationA.Endpoint(Set("L1"), Set(ReplicationConnection(CustomPort)), activate: false);

            var recoveryException = await Xunit.Assert.ThrowsAsync<IllegalStateException>(endpointA.Recover);

            // recoveryException.partialUpdate should be(false) // WHAT??
        }
        
        [Fact]
        public async Task Replication_endpoint_recovery_must_fail_when_resetting_the_replication_progress_fails_for_one_remote_endpoint()
        {
            var GetProgressException = new Exception("GetProgress test-failure");
            var locationA = Location("A", customConfig: SpecConfig);
            var locationB = Location("B", customConfig: SpecConfig);
            var locationC = Location("C", customConfig: SpecConfig,
                logFactory: id => Props.Create(() => new FailOnGetReplicationProgress(LogFactory(id), GetProgressException)));
            var locationD = Location("D", customConfig: SpecConfig, customPort: CustomPort);

            locationA.Endpoint(Set("L1"), Set(ReplicationConnection(locationD.Port)));
            locationB.Endpoint(Set("L1"), Set(ReplicationConnection(locationD.Port)));
            locationC.Endpoint(Set("L1"), Set(ReplicationConnection(locationD.Port)));
            var endpointD = locationD.Endpoint(Set("L1"), Set(ReplicationConnection(locationA.Port), ReplicationConnection(locationB.Port), ReplicationConnection(locationC.Port)), activate: false);

            var expected = await Xunit.Assert.ThrowsAsync<RecoveryException>(endpointD.Recover);
            expected.Message.Should().Contain(GetProgressException.Message);
        }
        
        [Fact]
        public async Task Replication_endpoint_recovery_must_succeed_normally_if_the_endpoint_was_healthy_but_not_convergent_yet()
        {
            var locationB = Location("B", customConfig: SpecConfig);
            Location newLocationA() => Location("A", customConfig: SpecConfig, customPort: CustomPort);
            var locationA1 = newLocationA();

            var endpointB = locationB.Endpoint(Set("L1"), Set(ReplicationConnection(locationA1.Port)), activate: false);
            ReplicationEndpoint newEndpointA(Location l) =>
                l.Endpoint(Set("L1"), Set(ReplicationConnection(locationB.Port)), activate: false);

            var endpointA1 = newEndpointA(locationA1);

            var targetA = endpointA1.Target("L1");
            var targetB = endpointB.Target("L1");

            Write(targetA, new List<string> {"a1", "a2"});
            Write(targetB, new List<string> {"b1", "b2"});
            Replicate(targetA, targetB, 1);
            Replicate(targetB, targetA, 1);

            await locationA1.System.Terminate();

            var locationA2 = newLocationA();
            var endpointA2 = newEndpointA(locationA2);

            endpointB.Activate();
            await endpointA2.Recover();

            AssertConvergence(ImmutableSortedSet.Create("a1", "a2", "b1", "b2"), endpointA2, endpointB);
        }
        
        [Fact]
        public async Task Replication_endpoint_recovery_must_repair_inconsistencies_of_endpoint_that_has_lost_all_events()
        {
            var locationA = Location("A", customConfig: SpecConfig);
            var locationB = Location("B", customConfig: SpecConfig);
            var locationC = Location("C", customConfig: SpecConfig);
            Location newLocationD() => Location("D", customConfig: SpecConfig, customPort: CustomPort);
            var locationD1 = newLocationD();

            var endpointA = locationA.Endpoint(Set("L1"), Set(ReplicationConnection(locationD1.Port)), activate: false);
            var endpointB = locationB.Endpoint(Set("L1"), Set(ReplicationConnection(locationD1.Port)), activate: false);
            var endpointC = locationC.Endpoint(Set("L1"), Set(ReplicationConnection(locationD1.Port)), activate: false);
            ReplicationEndpoint newEndpointD(Location l) => l.Endpoint(Set("L1"), Set(ReplicationConnection(locationA.Port), ReplicationConnection(locationB.Port), ReplicationConnection(locationC.Port)), activate: false);
            var endpointD1 = newEndpointD(locationD1);

            var targetA = endpointA.Target("L1");
            var targetB = endpointB.Target("L1");
            var targetC = endpointC.Target("L1");
            var targetD1 = endpointD1.Target("L1");

            var logDirD = await LogDirectory(targetD1);

            Write(targetA, new List<string> {"a"});
            Replicate(targetA, targetD1);
            Replicate(targetD1, targetA);

            Write(targetB, new List<string> {"b"});
            Write(targetC, new List<string> {"c"});
            Replicate(targetB, targetD1);
            Replicate(targetC, targetD1);
            Replicate(targetD1, targetB);
            Replicate(targetD1, targetC);

            Write(targetD1, new List<string> {"d"});
            Replicate(targetD1, targetC);

            // what a disaster ...
            await locationD1.System.Terminate();
            Directory.Delete(logDirD);

            endpointA.Activate();
            endpointB.Activate();
            endpointC.Activate();

            // start node D again (no backup available)
            var locationD2 = newLocationD();
            var endpointD2 = newEndpointD(locationD2);

            await endpointD2.Recover();
            // disclose bug #152 (writing new events is allowed after successful recovery)
            Write(endpointD2.Target("L1"), new List<string> {"d1"});

            AssertConvergence(ImmutableSortedSet.Create("a", "b", "c", "d", "d1"), endpointA, endpointB, endpointC, endpointD2);
        }
        
        [Fact]
        public async Task Replication_endpoint_recovery_must_repair_inconsistencies_of_an_endpoint_that_has_lost_all_events_but_has_been_partially_recovered_from_a_storage_backup()
        {
          var locationA = Location("A", customConfig: SpecConfig);
          var locationB = Location("B", customConfig: SpecConfig);
          var locationC = Location("C", customConfig: SpecConfig);
          Location newLocationD() => Location("D", customConfig: SpecConfig, customPort: CustomPort);
          var locationD1 = newLocationD();

          var endpointA = locationA.Endpoint(Set("L1"), Set(ReplicationConnection(locationD1.Port)), activate: false);
          var endpointB = locationB.Endpoint(Set("L1"), Set(ReplicationConnection(locationD1.Port)), activate: false);
          var endpointC = locationC.Endpoint(Set("L1"), Set(ReplicationConnection(locationD1.Port)), activate: false);
          ReplicationEndpoint newEndpointD(Location l) => l.Endpoint(Set("L1"), Set(ReplicationConnection(locationA.Port), ReplicationConnection(locationB.Port), ReplicationConnection(locationC.Port)), activate: false);
          var endpointD1 = newEndpointD(locationD1);

          var targetA = endpointA.Target("L1");
          var targetB = endpointB.Target("L1");
          var targetC = endpointC.Target("L1");
          var targetD1 = endpointD1.Target("L1");

          var rootDirD = RootDirectory(targetD1);
          var logDirD = await LogDirectory(targetD1);
          var bckDirD = Path.Combine(rootDirD, "backup");

          Write(targetA, new List<string> {"a"});
          Replicate(targetA, targetD1);
          Replicate(targetD1, targetA);

          Write(targetB, new List<string> {"b"});
          Write(targetC, new List<string> {"c"});
          Replicate(targetB, targetD1);

          await locationD1.System.Terminate();
          Directory.Copy(logDirD, bckDirD);

          var locationD2 = newLocationD();
          var endpointD2 = newEndpointD(locationD2);
          var targetD2 = endpointD2.Target("L1");

          Replicate(targetC, targetD2);
          Replicate(targetD2, targetB);
          Replicate(targetD2, targetC);

          Write(targetD2, new List<string> {"d"});
          Replicate(targetD2, targetC);

          // what a disaster ...
          await locationD2.System.Terminate();
          Directory.Delete(logDirD);

          // install a backup
          Directory.Copy(bckDirD, logDirD);

          endpointA.Activate();
          endpointB.Activate();
          endpointC.Activate();

          // start node D again (with backup available)
          var locationD3 = newLocationD();
          var endpointD3 = newEndpointD(locationD3);

          await endpointD3.Recover();

          AssertConvergence(ImmutableSortedSet.Create("a", "b", "c", "d"), endpointA, endpointB, endpointC, endpointD3);
        }
        
        [Fact]
        public async Task Replication_endpoint_recovery_must_repair_inconsistencies_if_recovery_was_stopped_during_event_recovery_and_restarted()
        {
            var config = ConfigurationFactory.ParseString("eventuate.log.write-batch-size = 1").WithFallback(SpecConfig);

            var locationB = Location("B", customConfig: config);
            Location newLocationA() => Location("A", customConfig: config, customPort: CustomPort);
            var locationA1 = newLocationA();

            var endpointB = locationB.Endpoint(Set("L1"), Set(ReplicationConnection(locationA1.Port)), activate: true);
            ReplicationEndpoint newEndpointA(Location l) => l.Endpoint(Set("L1"), Set(ReplicationConnection(locationB.Port)), activate: false);
            var endpointA1 = newEndpointA(locationA1);

            var targetA = endpointA1.Target("L1");
            var logDirA = await LogDirectory(targetA);
            var targetB = endpointB.Target("L1");

            var aS = Enumerable.Range(0, 5).Select(x => "A" + x);
            var bS = Enumerable.Range(0, 5).Select(x => "B" + x);
            var all = aS.ToImmutableSortedSet().Union(bS);

            endpointA1.Activate();

            Write(targetA, aS);
            Write(targetB, bS);
            AssertConvergence(all, endpointA1, endpointB);

            locationA1.System.Terminate();
            Directory.Delete(logDirA);

            var locationA2 = newLocationA();
            var endpointA2 = newEndpointA(locationA2);

            endpointA2.Recover();
            locationA2.Listener(endpointA2.Logs["L1"]).FishForMessage(o => o == "A1");
            await locationA2.System.Terminate();

            var locationA3 = newLocationA();
            var endpointA3 = newEndpointA(locationA3);

            await endpointA3.Recover();

            AssertConvergence(all, endpointA3, endpointB);
        }
        
        [Fact]
        public async Task Replication_endpoint_recovery_must_not_deadlock_if_two_endpoints_are_recovered_simultaneously()
        {
            var locationB = Location("B", customConfig: SpecConfig);
            var locationA = Location("A", customConfig: SpecConfig);

            var endpointB = locationB.Endpoint(Set("L1"), Set(ReplicationConnection(locationA.Port)), activate: false);
            var endpointA = locationA.Endpoint(Set("L1"), Set(ReplicationConnection(locationB.Port)), activate: false);

            var recoverB = endpointB.Recover();
            var recoverA = endpointA.Recover();

            await recoverA;
            await recoverB;
        }
        
        [Fact]
        public async Task Replication_endpoint_recovery_must_repair_inconsistencies_if_the_recovered_endpoint_upgraded_to_new_version()
        {
            var oldVersion = new ApplicationVersion(1, 0);
            var newVersion = new ApplicationVersion(2, 0);
            var locationB = Location("B", customConfig: SpecConfig);
            Location newLocationA() => Location("A", customConfig: SpecConfig);
            var locationA1 = newLocationA();

            var endpointB = locationB.Endpoint(Set("L1"), Set(ReplicationConnection(locationA1.Port)), applicationVersion: oldVersion);
            ReplicationEndpoint newEndpointA(Location l, ApplicationVersion version, bool activate) =>
                l.Endpoint(Set("L1"), Set(ReplicationConnection(locationB.Port)), applicationVersion: version, activate: activate);
            var endpointA1 = newEndpointA(locationA1, oldVersion, activate: true);

            var targetA = endpointA1.Target("L1");
            var logDirA = await LogDirectory(targetA);
            var targetB = endpointB.Target("L1");

            Write(targetA, new List<string>{"A"});
            Write(targetB, new List<string>{"B"});
            AssertConvergence(ImmutableSortedSet.Create("A", "B"), endpointA1, endpointB);

            await locationA1.System.Terminate();
            Directory.Delete(logDirA);

            var locationA2 = newLocationA();
            var endpointA2 = newEndpointA(locationA2, newVersion, activate: false);

            await endpointA2.Recover();
            AssertConvergence(ImmutableSortedSet.Create("A", "B"), endpointA2, endpointB);
        }
        
        [Fact]
        public async Task Replication_endpoint_recovery_must_repair_inconsistencies_if_the_recovered_endpoint_is_connected_through_filtered_and_unfiltered_connection_to_multiple_endpoints()
        {
            var locationA = Location("A", customConfig: SpecConfig);
            var locationB = Location("B", customConfig: SpecConfig);
            Location newLocationC() => Location("C", customConfig: SpecConfig, customPort: CustomPort);
            var locationC1 = newLocationC();

            var endpointA = locationA.Endpoint(Set("L1"), Set(ReplicationConnection(locationC1.Port)));
            var endpointB = locationB.Endpoint(Set("L1"), Set(ReplicationConnection(locationC1.Port)));
            ReplicationEndpoint newEndpointC(Location l, bool activate = true) => l.Endpoint(
                Set("L1"),
                Set(ReplicationConnection(locationA.Port), ReplicationConnection(locationB.Port)),
                TargetFilters(ImmutableDictionary<string, ReplicationFilter>.Empty.SetItem(endpointA.LogId("L1"), new PrefixesFilter("a", "b"))), // A does not receive cs
            activate: activate);

            var endpointC1 = newEndpointC(locationC1);

            var logDirC = await LogDirectory(endpointC1.Target("L1"));

            var cs = Enumerable.Range(1, 5).Select(x => "c" + x);
            await Writer(endpointC1).Write(cs);
            AssertConvergence(cs.ToImmutableSortedSet(), endpointB, endpointC1);
            await Writer(endpointB).Write(new []{"b"});
            AssertConvergence(ImmutableSortedSet.Create("b"), endpointA);

            // disaster on C
            await locationC1.System.Terminate();
            Directory.Delete(logDirC);

            await Writer(endpointA).Write(new[] {"a"});

            var locationC2 = newLocationC();
            var endpointC2 = newEndpointC(locationC2, activate: false);

            await endpointC2.Recover();
            AssertConvergence(cs.ToImmutableSortedSet().Add("b").Add("a"), endpointC2);
        }
        
        [Fact]
        public async Task Replication_endpoint_recovery_must_allow_to_write_more_events_after_partial_recovery_over_a_filtered_connection()
        {
            var locationA = Location("A", customConfig: SpecConfig);
            Location newLocationB() => Location("B", customConfig: SpecConfig, customPort: CustomPort);
            var locationB1 = newLocationB();

            var endpointA = locationA.Endpoint(Set("L1"), Set(ReplicationConnection(locationB1.Port)));
            ReplicationEndpoint newEndpointB(Location l, bool activate = true) => l.Endpoint(
                Set("L1"),
                Set(ReplicationConnection(locationA.Port)),
                TargetFilters(ImmutableDictionary<string, ReplicationFilter>.Empty.SetItem(endpointA.LogId("L1"), new PrefixesFilter("a"))), // A does not receive bs
                activate: activate);

            var endpointB1 = newEndpointB(locationB1);

            var logDirB = await LogDirectory(endpointB1.Target("L1"));

            await Writer(endpointB1).Write(new []{"b", "a1"});
            AssertConvergence(ImmutableSortedSet.Create("a1"), endpointA);

            // disaster on B
            await locationB1.System.Terminate();
            Directory.Delete(logDirB);

            var locationB2 = newLocationB();
            var endpointB2 = newEndpointB(locationB2, activate: false);

            await endpointB2.Recover();
            await Writer(endpointB2).Write(new []{"a2"});
            AssertConvergence(ImmutableSortedSet.Create("a1", "a2"), endpointA);
        }
        
        [Fact]
        public async Task Replication_endpoint_recovery_must_allow_to_write_more_events_after_partial_recovery_over_a_filtered_connection_and_a_restart()
        {   
            var locationA = Location("A", customConfig: SpecConfig);
            Location newLocationB() => Location("B", customConfig: SpecConfig, customPort: CustomPort);
            var locationB1 = newLocationB();

            var endpointA = locationA.Endpoint(Set("L1"), Set(ReplicationConnection(locationB1.Port)))

            ReplicationEndpoint newEndpointB(Location l, bool activate = true) => l.Endpoint(
                Set("L1"),
                Set(ReplicationConnection(locationA.Port)),
                TargetFilters(
                    ImmutableDictionary<string, ReplicationFilter>.Empty.SetItem(endpointA.LogId("L1"),
                        new PrefixesFilter("a"))), // A does not receive bs
                activate: activate);

            var endpointB1 = newEndpointB(locationB1);

            var logDirB = await LogDirectory(endpointB1.Target("L1"));

            await Writer(endpointB1).Write(new[] {"b", "a1"});
            AssertConvergence(ImmutableSortedSet.Create("a1"), endpointA);

            // disaster on B
            await locationB1.System.Terminate();
            Directory.Delete(logDirB);

            var locationB2 = newLocationB();
            var endpointB2 = newEndpointB(locationB2, activate: false);

            await endpointB2.Recover();

            await locationB2.System.Terminate();

            var locationB3 = newLocationB();
            var endpointB3 = newEndpointB(locationB3, activate: true);

            await Writer(endpointB3).Write(new[] {"a2"});
            AssertConvergence(ImmutableSortedSet.Create("a1", "a2"), endpointA);
        }

        private ReplicationEndpoint CreateEndpoint() =>
            new Location("A", LogFactory, customConfig: SpecConfig)
                .Endpoint(ImmutableHashSet.Create("L1"), ImmutableHashSet.Create(new ReplicationConnection("127.0.0.1", CustomPort)), activate: false);
        
        [Fact]
        public async Task Replication_endpoint_must_not_allow_concurrent_retries()
        {
            var endpoint = CreateEndpoint();
            var task = endpoint.Recover();
            await Xunit.Assert.ThrowsAsync<IllegalStateException>(endpoint.Recover);
        }
        
        [Fact]
        public async Task Replication_endpoint_must_not_allow_concurrent_recovery_and_activation()
        {
            var endpoint = CreateEndpoint();
            var task = endpoint.Recover();
            Xunit.Assert.Throws<IllegalStateException>(endpoint.Activate);   
        }
        
        [Fact]
        public async Task Replication_endpoint_must_not_allow_activated_endpoints_to_be_recovered()
        {
            var endpoint = CreateEndpoint();
            endpoint.Activate();
            await Xunit.Assert.ThrowsAsync<IllegalStateException>(endpoint.Recover);
        }
        
        [Fact]
        public async Task Replication_endpoint_must_not_allow_multiple_activations()
        {
            var endpoint = CreateEndpoint();
            endpoint.Activate();
            Xunit.Assert.Throws<IllegalStateException>(endpoint.Activate);
        }
    }
}