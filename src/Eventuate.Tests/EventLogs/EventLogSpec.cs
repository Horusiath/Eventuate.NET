#region copyright
// -----------------------------------------------------------------------
//  <copyright file="EventLogSpec.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.TestKit;
using Akka.TestKit.Xunit2;
using Eventuate.EventsourcingProtocol;
using Eventuate.ReplicationProtocol;
using FluentAssertions;
using FluentAssertions.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace Eventuate.Tests.EventLogs
{
    public abstract class EventLogSpec : TestKit
    {
        #region internal classes

        sealed class ProcessIdFilter : ReplicationFilter
        {
            private readonly string processId;

            public ProcessIdFilter(string processId)
            {
                this.processId = processId;
            }

            public override bool Invoke(DurableEvent e) => e.ProcessId == processId;
        }
        
        sealed class AllFilter : ReplicationFilter
        {
            public static readonly ReplicationFilter Instance = new AllFilter();
            private AllFilter(){}
            public override bool Invoke(DurableEvent durableEvent) => false;
        }
        
        #endregion

        private static readonly DateTime UndefinedTimestamp = default;
        private static readonly DateTime SystemTimestamp = new DateTime(123);
        private static int instanceIdCounter = 0;
        
        public const string EmitterIdA = "A";
        public const string EmitterIdB = "B";
        public const string RemoteLogId = "R1";
        
        private const long ErrorSequenceNr = -1L;
        private const long IgnoreDeletedSequenceNr = -2L;

        public static readonly TimeSpan Timeout = 20.Seconds();
        
        private static readonly string DefaultConfig = @"
            akka.loglevel = ERROR
            akka.test.single-expect-default = 20s";

        protected readonly int instanceId;
        protected readonly TestProbe replyToProbe;
        protected readonly TestProbe replicatorProbe;
        protected readonly TestProbe notificationProbe;
        
        protected ImmutableArray<DurableEvent> generatedEmittedEvents = ImmutableArray<DurableEvent>.Empty;
        protected ImmutableArray<DurableEvent> generatedReplicatedEvents = ImmutableArray<DurableEvent>.Empty;
        
        protected EventLogSpec(ITestOutputHelper output) : base(DefaultConfig, output)
        {
            this.instanceId = Interlocked.Increment(ref instanceIdCounter);
            
            this.replyToProbe = this.CreateTestProbe("reply-to-probe");
            this.replicatorProbe = this.CreateTestProbe("replication-probe");
            this.notificationProbe = this.CreateTestProbe("notification-probe");
        }
        
        protected IActorRef Log { get; }
        protected string LogId => instanceId.ToString();

        protected static DurableEvent Event(object payload) => Event(payload, VectorTime.Zero, EmitterIdA);
        
        protected static DurableEvent Event(object payload, string emitterAggregateId) => Event(payload, VectorTime.Zero, EmitterIdA, emitterAggregateId);

        protected static DurableEvent Event(object payload, string emitterAggregateId, ImmutableHashSet<string> customDestinationAggregateIds) => 
            Event(payload, VectorTime.Zero, EmitterIdA, emitterAggregateId, customDestinationAggregateIds);
        
        private static DurableEvent Event(object payload, VectorTime vectorTime, string emitterId, string emitterAggregateId = null, ImmutableHashSet<string> customDestinationAggregateIds = null)
        {
            return new DurableEvent(
                payload: payload,
                emitterId: emitterId,
                emitterAggregateId: emitterAggregateId,
                customDestinationAggregateIds: customDestinationAggregateIds ?? ImmutableHashSet<string>.Empty,
                systemTimestamp: default,
                vectorTimestamp: vectorTime,
                processId: DurableEvent.UndefinedLogId,
                localLogId: DurableEvent.UndefinedLogId,
                localSequenceNr: DurableEvent.UndefinedSequenceNr);
        }

        protected VectorTime Timestamp(long a, long b)
        {
            if (a == 0)
                return b == 0 ? VectorTime.Zero : new VectorTime((RemoteLogId, b));
            else if (b == 0)
                return new VectorTime((LogId, a));
            else 
                return new VectorTime((LogId, a), (RemoteLogId, b)); 
        }

        protected long CurrentSequenceNr
        {
            get
            {
                Log.Tell(new GetEventLogClock(), replyToProbe.Ref);
                return replyToProbe.ExpectMsg<GetEventLogClockSuccess>().Clock.SequenceNr;
            }
        }

        protected IEnumerable<DurableEvent> ExpectedEmittedEvents(DurableEvent[] events, long offset = 0L)
        {
            for (int i = 0; i < events.Length; i++)
            {
                yield return events[i].Copy(vectorTimestamp: Timestamp(offset + i, 0), processId: LogId, localLogId: LogId, localSequenceNr: offset + i);
            }
        }
        
        protected IEnumerable<DurableEvent> ExpectedReplicatedEvents(DurableEvent[] events, long offset = 0L)
        {
            for (int i = 0; i < events.Length; i++)
            {
                yield return events[i].Copy(processId: RemoteLogId, localLogId: LogId, localSequenceNr: offset + i);
            }
        }

        protected IEnumerable<DurableEvent> WriteEmittedEvents(DurableEvent[] events, IActorRef logRef = null)
        {
            logRef = logRef ?? Log;
            var offset = CurrentSequenceNr + 1;
            var expected = ExpectedEmittedEvents(events, offset);
            logRef.Tell(new Write(events, Sys.DeadLetters, replyToProbe.Ref, 0, 0));
            replyToProbe.ExpectMsg(new WriteSuccess(expected, 0, 0));
            return expected;
        }
        
        protected IEnumerable<DurableEvent> WriteReplicatedEvents(DurableEvent[] events, long replicationProgress, string remoteLogId = null)
        {
            remoteLogId = remoteLogId ?? RemoteLogId;
            var offset = CurrentSequenceNr + 1;
            var expected = ExpectedReplicatedEvents(events, offset);
            Log.Tell(new ReplicationWrite(events, ImmutableDictionary<string, ReplicationMetadata>.Empty.Add(remoteLogId, new ReplicationMetadata(replicationProgress, VectorTime.Zero))), replicatorProbe.Ref);
            replicatorProbe.ExpectMsg<ReplicationWriteSuccess>().Metadata[remoteLogId].ReplicationProgress.Should().Be(replicationProgress);
            return expected;
        }

        protected void WriteReplicationProgress(long replicationProgress, long expectedStoredReplicationProgress,
            string remoteLogId = null)
        {
            remoteLogId = remoteLogId ?? RemoteLogId;
            Log.Tell(new ReplicationWrite(Array.Empty<DurableEvent>(), ImmutableDictionary<string, ReplicationMetadata>.Empty.Add(remoteLogId, new ReplicationMetadata(replicationProgress, VectorTime.Zero))), replicatorProbe.Ref);
            replicatorProbe.ExpectMsg<ReplicationWriteSuccess>().Metadata[remoteLogId].ReplicationProgress.Should().Be(replicationProgress);
        }

        protected TestProbe RegisterCollaborator(string aggregateId = null, TestProbe collaborator = null)
        {
            collaborator = collaborator ?? CreateTestProbe();
            Log.Tell(new Replay(collaborator.Ref, 0, 1L, 0, aggregateId), collaborator.Ref);
            collaborator.ExpectMsg(new ReplaySuccess(Array.Empty<DurableEvent>(), 0L, 0));
            return collaborator;
        }

        protected void GenerateEmittedEvents(int num = 3, string emitterAggregateId = null,
            ImmutableHashSet<string> customDestinationAggregateIds = null)
        {
            for (int i = 1; i <= num; i++)
            {
                var e = new DurableEvent("a-" + i, EmitterIdA, emitterAggregateId, customDestinationAggregateIds);
                generatedEmittedEvents = generatedEmittedEvents.Add(e);
            }
        }
        
        protected void GenerateReplicatedEvents(int num = 3, string emitterAggregateId = null,
            ImmutableHashSet<string> customDestinationAggregateIds = null)
        {
            for (int i = 1; i <= num; i++)
            {
                var e = new DurableEvent("b-" + i, EmitterIdB, emitterAggregateId, customDestinationAggregateIds, default, Timestamp(0, i + 6), RemoteLogId, RemoteLogId, i + 6);
                generatedReplicatedEvents = generatedReplicatedEvents.Add(e);
            }
        }
        
        protected virtual DateTime CurrentTimestamp => SystemTimestamp;

        private static ImmutableDictionary<T1, T2> Map<T1, T2>(params (T1, T2)[] entries)
        {
            var builder = ImmutableDictionary.CreateBuilder<T1, T2>();
            foreach (var (k, v) in entries)
            {
                builder[k] = v;
            }

            return builder.ToImmutable();
        }

        #region EventLogSystemTimeSpec tests

        [Fact]
        public void EventLog_must_update_an_events_system_timestamp()
        {
            var events = new[] {Event("a").Copy(systemTimestamp: new DateTime(3L))};
            Log.Tell(new Write(events, Sys.DeadLetters, replyToProbe.Ref, 0, 0));
            replyToProbe.ExpectMsg<WriteSuccess>().Events.First().SystemTimestamp.Should().Be(CurrentTimestamp);
        }
        
        [Fact]
        public void EventLog_must_update_replicated_events_system_timestamp_if_not_defined()
        {
            var e = new DurableEvent("a", EmitterIdA, processId: LogId, vectorTimestamp: new VectorTime((RemoteLogId, 1L)));
            var collaborator = RegisterCollaborator();
            Log.Tell(new ReplicationWrite(new []{e}, ImmutableDictionary<string, ReplicationMetadata>.Empty.SetItem(RemoteLogId, new ReplicationMetadata(5, VectorTime.Zero))));
            collaborator.ExpectMsg<Written>().Event.SystemTimestamp.Should().NotBe(UndefinedTimestamp);
        }
        
        [Fact]
        public void EventLog_must_not_update_replicated_events_system_timestamp_if_defined()
        {
            var e = new DurableEvent("a", EmitterIdA, processId: LogId, vectorTimestamp: new VectorTime((RemoteLogId, 1L)), systemTimestamp: SystemTimestamp);
            var collaborator = RegisterCollaborator();
            Log.Tell(new ReplicationWrite(new []{e}, ImmutableDictionary<string, ReplicationMetadata>.Empty.SetItem(RemoteLogId, new ReplicationMetadata(5, VectorTime.Zero))));
            collaborator.ExpectMsg<Written>().Event.SystemTimestamp.Should().NotBe(SystemTimestamp);
        }

        #endregion

        [Fact]
        public void EventLog_must_write_local_events()
        {
            GenerateEmittedEvents();
            GenerateEmittedEvents();
        }
        
        [Fact]
        public void EventLog_must_write_local_events_with_undefined_defaultRoutingDestination_and_undefined_customRoutingDestinations_and_route_them_to_collaborators_with_undefined_aggregateId()
        {
            var collaborator = RegisterCollaborator(aggregateId: null);
            GenerateEmittedEvents(emitterAggregateId: null, customDestinationAggregateIds: ImmutableHashSet<string>.Empty);
            collaborator.ExpectMsg(new Written(generatedEmittedEvents[0]));
            collaborator.ExpectMsg(new Written(generatedEmittedEvents[1]));
            collaborator.ExpectMsg(new Written(generatedEmittedEvents[2]));
        }
        
        [Fact]
        public void EventLog_must_write_local_events_with_undefined_defaultRoutingDestination_and_defined_customRoutingDestinations_and_route_them_to_collaborators_with_undefined_aggregateId()
        {
            var collaborator = RegisterCollaborator(aggregateId: null);
            GenerateEmittedEvents(emitterAggregateId: null, customDestinationAggregateIds: ImmutableHashSet<string>.Empty.Add("a1"));
            collaborator.ExpectMsg(new Written(generatedEmittedEvents[0]));
            collaborator.ExpectMsg(new Written(generatedEmittedEvents[1]));
            collaborator.ExpectMsg(new Written(generatedEmittedEvents[2]));
        }
        
        [Fact]
        public void EventLog_must_write_local_events_with_defined_defaultRoutingDestination_and_undefined_customRoutingDestinations_and_route_them_to_collaborators_with_undefined_aggregateId()
        {
            var collaborator = RegisterCollaborator(aggregateId: null);
            GenerateEmittedEvents(emitterAggregateId: "a1", customDestinationAggregateIds: ImmutableHashSet<string>.Empty);
            collaborator.ExpectMsg(new Written(generatedEmittedEvents[0]));
            collaborator.ExpectMsg(new Written(generatedEmittedEvents[1]));
            collaborator.ExpectMsg(new Written(generatedEmittedEvents[2]));
        }
        
        [Fact]
        public void EventLog_must_write_local_events_with_defined_defaultRoutingDestination_and_defined_customRoutingDestinations_and_route_them_to_collaborators_with_undefined_aggregateId()
        {
            var collaborator = RegisterCollaborator(aggregateId: null);
            GenerateEmittedEvents(emitterAggregateId: "a1", customDestinationAggregateIds: ImmutableHashSet<string>.Empty.Add("a2"));
            collaborator.ExpectMsg(new Written(generatedEmittedEvents[0]));
            collaborator.ExpectMsg(new Written(generatedEmittedEvents[1]));
            collaborator.ExpectMsg(new Written(generatedEmittedEvents[2]));
        }
        
        [Fact]
        public void EventLog_must_write_local_events_with_undefined_defaultRoutingDestination_and_undefined_customRoutingDestinations_and_not_route_them_to_collaborators_with_defined_aggregateId()
        {
            var collaborator = CreateTestProbe();
            RegisterCollaborator("a1", collaborator);
            RegisterCollaborator(null, collaborator);
            GenerateEmittedEvents(emitterAggregateId: null, customDestinationAggregateIds: ImmutableHashSet<string>.Empty);
            collaborator.ExpectMsg(new Written(generatedEmittedEvents[0]));
            collaborator.ExpectMsg(new Written(generatedEmittedEvents[1]));
            collaborator.ExpectMsg(new Written(generatedEmittedEvents[2]));
        }
        
        [Fact]
        public void EventLog_must_write_local_events_with_undefined_defaultRoutingDestination_and_defined_customRoutingDestinations_and_route_them_to_collaborators_with_defined_aggregateId()
        {
            var collaborator = CreateTestProbe();
            RegisterCollaborator("a1", collaborator);
            RegisterCollaborator(null, collaborator);
            GenerateEmittedEvents(emitterAggregateId: null, customDestinationAggregateIds: ImmutableHashSet<string>.Empty.Add("a1"));
            collaborator.ExpectMsg(new Written(generatedEmittedEvents[0]));
            collaborator.ExpectMsg(new Written(generatedEmittedEvents[0]));
            collaborator.ExpectMsg(new Written(generatedEmittedEvents[1]));
            collaborator.ExpectMsg(new Written(generatedEmittedEvents[1]));
            collaborator.ExpectMsg(new Written(generatedEmittedEvents[2]));
            collaborator.ExpectMsg(new Written(generatedEmittedEvents[2]));
        }
        
        [Fact]
        public void EventLog_must_write_local_events_with_defined_defaultRoutingDestination_and_undefined_customRoutingDestinations_and_route_them_to_collaborators_with_defined_aggregateId()
        {
            var collaborator = CreateTestProbe();
            RegisterCollaborator("a1", collaborator);
            RegisterCollaborator(null, collaborator);
            GenerateEmittedEvents(emitterAggregateId: "a1", customDestinationAggregateIds: ImmutableHashSet<string>.Empty);
            collaborator.ExpectMsg(new Written(generatedEmittedEvents[0]));
            collaborator.ExpectMsg(new Written(generatedEmittedEvents[0]));
            collaborator.ExpectMsg(new Written(generatedEmittedEvents[1]));
            collaborator.ExpectMsg(new Written(generatedEmittedEvents[1]));
            collaborator.ExpectMsg(new Written(generatedEmittedEvents[2]));
            collaborator.ExpectMsg(new Written(generatedEmittedEvents[2]));
        }
        
        [Fact]
        public void EventLog_must_write_local_events_with_defined_defaultRoutingDestination_and_defined_customRoutingDestinations_and_route_them_to_collaborators_with_defined_aggregateId()
        {
            var collaborator = CreateTestProbe();
            RegisterCollaborator("a1", collaborator);
            RegisterCollaborator("a2", collaborator);
            RegisterCollaborator(null, collaborator);
            GenerateEmittedEvents(emitterAggregateId: "a1", customDestinationAggregateIds: ImmutableHashSet<string>.Empty.Add("a2"));
            collaborator.ExpectMsg(new Written(generatedEmittedEvents[0]));
            collaborator.ExpectMsg(new Written(generatedEmittedEvents[0]));
            collaborator.ExpectMsg(new Written(generatedEmittedEvents[0]));
            collaborator.ExpectMsg(new Written(generatedEmittedEvents[1]));
            collaborator.ExpectMsg(new Written(generatedEmittedEvents[1]));
            collaborator.ExpectMsg(new Written(generatedEmittedEvents[1]));
            collaborator.ExpectMsg(new Written(generatedEmittedEvents[2]));
            collaborator.ExpectMsg(new Written(generatedEmittedEvents[2]));
            collaborator.ExpectMsg(new Written(generatedEmittedEvents[2]));
        }
        
        [Fact]
        public void EventLog_must_reply_with_a_failure_message_if_write_fails()
        {
            var events = new[]
            {
                new DurableEvent("boom", EmitterIdA),
                new DurableEvent("okay", EmitterIdA)
            };
            Log.Tell(new Write(events, Sys.DeadLetters, replyToProbe.Ref, 0, 0));
            replyToProbe.ExpectMsg(new WriteFailure(events, new Exception("boom"), 0, 0));
        }
        
        [Fact]
        public void EventLog_must_write_replicated_events()
        {
            GenerateReplicatedEvents();
        }
        
        [Fact]
        public void EventLog_must_write_replicated_events_with_undefined_defaultRoutingDestination_and_undefined_customRoutingDestinations_and_route_them_to_collaborators_with_undefined_aggregateId()
        {
            var collaborator = RegisterCollaborator(aggregateId: null);
            GenerateReplicatedEvents(emitterAggregateId: null, customDestinationAggregateIds: ImmutableHashSet<string>.Empty);
            collaborator.ExpectMsg(new Written(generatedReplicatedEvents[0]));
            collaborator.ExpectMsg(new Written(generatedReplicatedEvents[1]));
            collaborator.ExpectMsg(new Written(generatedReplicatedEvents[2]));
        }
        
        [Fact]
        public void EventLog_must_write_replicated_events_with_undefined_defaultRoutingDestination_and_defined_customRoutingDestinations_and_route_them_to_collaborators_with_undefined_aggregateId()
        {
            var collaborator = RegisterCollaborator(aggregateId: null);
            GenerateReplicatedEvents(emitterAggregateId: null, customDestinationAggregateIds: ImmutableHashSet<string>.Empty.Add("a1"));
            collaborator.ExpectMsg(new Written(generatedReplicatedEvents[0]));
            collaborator.ExpectMsg(new Written(generatedReplicatedEvents[1]));
            collaborator.ExpectMsg(new Written(generatedReplicatedEvents[2]));
        }
        
        [Fact]
        public void EventLog_must_write_replicated_events_with_defined_defaultRoutingDestination_and_undefined_customRoutingDestinations_and_route_them_to_collaborators_with_undefined_aggregateId()
        {
            var collaborator = RegisterCollaborator(aggregateId: null);
            GenerateReplicatedEvents(emitterAggregateId: "a1", customDestinationAggregateIds: ImmutableHashSet<string>.Empty);
            collaborator.ExpectMsg(new Written(generatedReplicatedEvents[0]));
            collaborator.ExpectMsg(new Written(generatedReplicatedEvents[1]));
            collaborator.ExpectMsg(new Written(generatedReplicatedEvents[2]));
        }
        
        [Fact]
        public void EventLog_must_write_replicated_events_with_defined_defaultRoutingDestination_and_defined_customRoutingDestinations_and_route_them_to_collaborators_with_undefined_aggregateId()
        {
            var collaborator = RegisterCollaborator(aggregateId: null);
            GenerateReplicatedEvents(emitterAggregateId: "a1", customDestinationAggregateIds: ImmutableHashSet<string>.Empty.Add("a2"));
            collaborator.ExpectMsg(new Written(generatedReplicatedEvents[0]));
            collaborator.ExpectMsg(new Written(generatedReplicatedEvents[1]));
            collaborator.ExpectMsg(new Written(generatedReplicatedEvents[2]));
        }
        
        [Fact]
        public void EventLog_must_write_replicated_events_with_undefined_defaultRoutingDestination_and_undefined_customRoutingDestinations_and_not_route_them_to_collaborators_with_defined_aggregateId()
        {
            var collaborator = CreateTestProbe(); 
            RegisterCollaborator(aggregateId: "a1", collaborator);
            RegisterCollaborator(aggregateId: null, collaborator);
            GenerateReplicatedEvents(emitterAggregateId: null, customDestinationAggregateIds: ImmutableHashSet<string>.Empty);
            collaborator.ExpectMsg(new Written(generatedReplicatedEvents[0]));
            collaborator.ExpectMsg(new Written(generatedReplicatedEvents[1]));
            collaborator.ExpectMsg(new Written(generatedReplicatedEvents[2]));
        }
        
        [Fact]
        public void EventLog_must_write_replicated_events_with_undefined_defaultRoutingDestination_and_defined_customRoutingDestinations_and_route_them_to_collaborators_with_defined_aggregateId()
        {
            var collaborator = CreateTestProbe(); 
            RegisterCollaborator(aggregateId: "a1", collaborator);
            RegisterCollaborator(aggregateId: null, collaborator);
            GenerateReplicatedEvents(emitterAggregateId: null, customDestinationAggregateIds: ImmutableHashSet<string>.Empty.Add("a1"));
            collaborator.ExpectMsg(new Written(generatedReplicatedEvents[0]));
            collaborator.ExpectMsg(new Written(generatedReplicatedEvents[0]));
            collaborator.ExpectMsg(new Written(generatedReplicatedEvents[1]));
            collaborator.ExpectMsg(new Written(generatedReplicatedEvents[1]));
            collaborator.ExpectMsg(new Written(generatedReplicatedEvents[2]));
            collaborator.ExpectMsg(new Written(generatedReplicatedEvents[2]));
        }
        
        [Fact]
        public void EventLog_must_write_replicated_events_with_defined_defaultRoutingDestination_and_undefined_customRoutingDestinations_and_route_them_to_collaborators_with_defined_aggregateId()
        {
            var collaborator = CreateTestProbe(); 
            RegisterCollaborator(aggregateId: "a1", collaborator);
            RegisterCollaborator(aggregateId: null, collaborator);
            GenerateReplicatedEvents(emitterAggregateId: "a1", customDestinationAggregateIds: ImmutableHashSet<string>.Empty);
            collaborator.ExpectMsg(new Written(generatedReplicatedEvents[0]));
            collaborator.ExpectMsg(new Written(generatedReplicatedEvents[0]));
            collaborator.ExpectMsg(new Written(generatedReplicatedEvents[1]));
            collaborator.ExpectMsg(new Written(generatedReplicatedEvents[1]));
            collaborator.ExpectMsg(new Written(generatedReplicatedEvents[2]));
            collaborator.ExpectMsg(new Written(generatedReplicatedEvents[2]));
        }
        
        [Fact]
        public void EventLog_must_write_replicated_events_with_defined_defaultRoutingDestination_and_defined_customRoutingDestinations_and_route_them_to_collaborators_with_defined_aggregateId()
        {
            var collaborator = CreateTestProbe(); 
            RegisterCollaborator(aggregateId: "a1", collaborator);
            RegisterCollaborator(aggregateId: "a2", collaborator);
            RegisterCollaborator(aggregateId: null, collaborator);
            GenerateReplicatedEvents(emitterAggregateId: "a1", customDestinationAggregateIds: ImmutableHashSet<string>.Empty.Add("a2"));
            collaborator.ExpectMsg(new Written(generatedReplicatedEvents[0]));
            collaborator.ExpectMsg(new Written(generatedReplicatedEvents[0]));
            collaborator.ExpectMsg(new Written(generatedReplicatedEvents[0]));
            collaborator.ExpectMsg(new Written(generatedReplicatedEvents[1]));
            collaborator.ExpectMsg(new Written(generatedReplicatedEvents[1]));
            collaborator.ExpectMsg(new Written(generatedReplicatedEvents[1]));
            collaborator.ExpectMsg(new Written(generatedReplicatedEvents[2]));
            collaborator.ExpectMsg(new Written(generatedReplicatedEvents[2]));
            collaborator.ExpectMsg(new Written(generatedReplicatedEvents[2]));
        }
        
        [Fact]
        public void EventLog_must_write_replicated_events_and_update_the_replication_progress_map()
        {
            Log.Tell(new GetReplicationProgresses(), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new GetReplicationProgressesSuccess(ImmutableDictionary<string, long>.Empty));
            GenerateReplicatedEvents();
            Log.Tell(new GetReplicationProgresses(), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new GetReplicationProgressesSuccess(Map((RemoteLogId, 17L))));
        }
        
        [Fact]
        public void EventLog_must_write_replicated_events_and_update_the_replication_progress_map_with_multiple_progress_values()
        {
            const string srcLog1 = "L1";
            const string srcLog2 = "L2";

            var events = new[]
            {
                new DurableEvent("a", EmitterIdB, vectorTimestamp: new VectorTime((srcLog1, 3L)), processId: srcLog1, localLogId: srcLog1, localSequenceNr: 7),
                new DurableEvent("b", EmitterIdB, vectorTimestamp: new VectorTime((srcLog2, 4L)), processId: srcLog2, localLogId: srcLog2, localSequenceNr: 7),
            };

            var metadata = Map((srcLog1, new ReplicationMetadata(4, VectorTime.Zero)),
                (srcLog1, new ReplicationMetadata(4, VectorTime.Zero)));
            
            Log.Tell(new ReplicationWrite(events, metadata), replicatorProbe.Ref);
            replicatorProbe.ExpectMsg<ReplicationWriteSuccess>();
            Log.Tell(new GetReplicationProgresses(), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new GetReplicationProgressesSuccess(metadata.ToImmutableDictionary(p => p.Key, p => p.Value.ReplicationProgress)));
        }
        
        [Fact]
        public void EventLog_must_reply_with_a_failure_message_if_replication_fails()
        {
            var events = new[]
            {
                new DurableEvent("a", EmitterIdB, vectorTimestamp: Timestamp(0, 7), processId: RemoteLogId, localLogId: RemoteLogId, localSequenceNr: 7),
                new DurableEvent("b", EmitterIdB, vectorTimestamp: Timestamp(0, 8), processId: RemoteLogId, localLogId: RemoteLogId, localSequenceNr: 8),
            };
            
            Log.Tell(new ReplicationWrite(events, Map((RemoteLogId, new ReplicationMetadata(8, VectorTime.Zero)))), replicatorProbe.Ref);
            replicatorProbe.ExpectMsg(new ReplicationWriteFailure(new Exception("boom")));
        }
        
        [Fact]
        public void EventLog_must_reply_with_a_failure_message_if_replication_fails_and_not_update_the_replication_progress_map()
        {
            Log.Tell(new GetReplicationProgresses(), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new GetReplicationProgressesSuccess(ImmutableDictionary<string, long>.Empty));
            
            var events = new[]
            {
                new DurableEvent("boom", EmitterIdB, vectorTimestamp: Timestamp(0, 7), processId: RemoteLogId, localLogId: RemoteLogId, localSequenceNr: 7),
                new DurableEvent("okay", EmitterIdB, vectorTimestamp: Timestamp(0, 8), processId: RemoteLogId, localLogId: RemoteLogId, localSequenceNr: 8),
            };

            Log.Tell(new ReplicationWrite(events, Map((RemoteLogId, new ReplicationMetadata(8, VectorTime.Zero)))), replicatorProbe.Ref);
            replicatorProbe.ExpectMsg(new ReplicationWriteFailure(IntegrationTestException.Instance));
            Log.Tell(new GetReplicationProgresses(), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new GetReplicationProgressesSuccess(ImmutableDictionary<string, long>.Empty));
        }
        
        [Fact]
        public void EventLog_must_replay_events_from_scratch()
        {
            GenerateEmittedEvents();
            Log.Tell(new Replay(null, 0, 1L), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new ReplaySuccess(generatedEmittedEvents,
                generatedEmittedEvents.Last().LocalSequenceNr, 0));
        }
        
        [Fact]
        public void EventLog_must_replay_events_in_batches()
        {
            GenerateEmittedEvents();
            GenerateEmittedEvents();
            Log.Tell(new Replay(null, 0, 1, 2), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new ReplaySuccess(generatedEmittedEvents.Take(2), generatedEmittedEvents[1].LocalSequenceNr, 0));
            Log.Tell(new Replay(null, 0, generatedEmittedEvents[1].LocalSequenceNr + 1L, 2), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new ReplaySuccess(generatedEmittedEvents.Skip(2).Take(2), generatedEmittedEvents[3].LocalSequenceNr, 0));
            Log.Tell(new Replay(null, 0, generatedEmittedEvents[3].LocalSequenceNr + 1L, 2), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new ReplaySuccess(generatedEmittedEvents.Skip(4).Take(2), generatedEmittedEvents[5].LocalSequenceNr, 0));
        }
        
        [Fact]
        public void EventLog_must_replay_events_from_a_custom_position()
        {
            GenerateEmittedEvents();
            Log.Tell(new Replay(null, 0, 3), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new ReplaySuccess(generatedEmittedEvents.Skip(2).Take(1), generatedEmittedEvents[2].LocalSequenceNr, 0));
            // custom position > last sequence number
            Log.Tell(new Replay(null, 0, 5), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new ReplaySuccess(Array.Empty<DurableEvent>(), 4, 0));
        }
        
        [Fact]
        public void EventLog_must_replay_events_from_the_default_log_if_request_aggregateId_is_not_defined()
        {
            GenerateEmittedEvents(customDestinationAggregateIds: ImmutableHashSet.Create("a1"));
            Log.Tell(new Replay(null, 0, 1L), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new ReplaySuccess(generatedEmittedEvents, generatedEmittedEvents.Last().LocalSequenceNr, 0));
        }
        
        [Fact]
        public void EventLog_must_replay_events_from_the_index_if_request_aggregateId_is_defined()
        {
            GenerateEmittedEvents(customDestinationAggregateIds: ImmutableHashSet.Create("a1"));
            Log.Tell(new Replay(null, 0, 1L, aggregateId: "a1"), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new ReplaySuccess(generatedEmittedEvents, generatedEmittedEvents.Last().LocalSequenceNr, 0));
        }
        
        [Fact]
        public void EventLog_must_replay_events_from_the_index_with_proper_isolation()
        {
            GenerateEmittedEvents(customDestinationAggregateIds: ImmutableHashSet.Create("a1"));
            GenerateEmittedEvents(customDestinationAggregateIds: ImmutableHashSet.Create("a2"));
            Log.Tell(new Replay(null, 0, 1L, aggregateId: "a1"), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new ReplaySuccess(generatedEmittedEvents.Take(3), generatedEmittedEvents[2].LocalSequenceNr, 0));
        }
        
        [Fact]
        public void EventLog_must_replay_events_from_the_index_and_from_a_custom_position()
        {
            GenerateEmittedEvents(customDestinationAggregateIds: ImmutableHashSet.Create("a1"));
            GenerateEmittedEvents(customDestinationAggregateIds: ImmutableHashSet.Create("a2"));
            Log.Tell(new Replay(null, 0, 2L, aggregateId: "a1"), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new ReplaySuccess(generatedEmittedEvents.Skip(1).Take(3), generatedEmittedEvents[2].LocalSequenceNr, 0));
            Log.Tell(new Replay(null, 0, 5L, aggregateId: "a1"), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new ReplaySuccess(Array.Empty<DurableEvent>(), 4, 0));
            Log.Tell(new Replay(null, 0, 2L, aggregateId: "a2"), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new ReplaySuccess(generatedEmittedEvents.Skip(3).Take(3), generatedEmittedEvents[5].LocalSequenceNr, 0));
            Log.Tell(new Replay(null, 0, 5L, aggregateId: "a2"), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new ReplaySuccess(generatedEmittedEvents.Skip(4).Take(2), 4, 0));
        }
        
        [Fact]
        public void EventLog_must_not_replay_events_with_non_matching_aggregateId_if_request_aggregateId_is_defined()
        {
            GenerateEmittedEvents(customDestinationAggregateIds: ImmutableHashSet.Create("a1"));
            Log.Tell(new Replay(null, 0, 1L, aggregateId: "a2"), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new ReplaySuccess(Array.Empty<DurableEvent>(), 0, 0));
        }
        
        [Fact]
        public void EventLog_must_reply_with_a_failure_message_if_replay_fails()
        {
            Log.Tell(new Replay(null, 0, ErrorSequenceNr), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new ReplayFailure(IntegrationTestException.Instance, ErrorSequenceNr, 0));
        }
        
        [Fact]
        public void EventLog_must_replication_read_local_events()
        {
            GenerateEmittedEvents();
            Log.Tell(new ReplicationRead(1, int.MaxValue, int.MaxValue, NoFilter.Instance, DurableEvent.UndefinedLogId, Sys.DeadLetters, VectorTime.Zero), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new ReplicationReadSuccess(generatedEmittedEvents, 1, 3, DurableEvent.UndefinedLogId, new VectorTime((LogId, 3L))));
        }
        
        [Fact]
        public void EventLog_must_replication_read_local_and_replicated_events()
        {
            GenerateEmittedEvents();
            GenerateReplicatedEvents();
            Log.Tell(new ReplicationRead(1, int.MaxValue, int.MaxValue, NoFilter.Instance, DurableEvent.UndefinedLogId, Sys.DeadLetters, VectorTime.Zero), replyToProbe.Ref);
            var events = generatedEmittedEvents.Union(generatedReplicatedEvents).ToImmutableArray();
            replyToProbe.ExpectMsg(new ReplicationReadSuccess(events, 1, 6, DurableEvent.UndefinedLogId, new VectorTime((LogId, 3L), (RemoteLogId, 9L))));
        }
        
        [Fact]
        public void EventLog_must_replication_read_events_with_batch_size_limit()
        {
            GenerateEmittedEvents();
            Log.Tell(new ReplicationRead(1, 2, int.MaxValue, NoFilter.Instance, DurableEvent.UndefinedLogId, Sys.DeadLetters, VectorTime.Zero), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new ReplicationReadSuccess(generatedEmittedEvents.Take(2).ToArray(), 1, 2, DurableEvent.UndefinedLogId, new VectorTime((LogId, 3L))));
            Log.Tell(new ReplicationRead(1, 0, int.MaxValue, NoFilter.Instance, DurableEvent.UndefinedLogId, Sys.DeadLetters, VectorTime.Zero), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new ReplicationReadSuccess(Array.Empty<DurableEvent>(), 1, 0, DurableEvent.UndefinedLogId, new VectorTime((LogId, 3L))));    
        }
        
        [Fact]
        public void EventLog_must_replication_read_events_with_scan_limit()
        {
            GenerateEmittedEvents();
            Log.Tell(new ReplicationRead(1, int.MaxValue, 2, NoFilter.Instance, DurableEvent.UndefinedLogId, Sys.DeadLetters, VectorTime.Zero), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new ReplicationReadSuccess(generatedEmittedEvents.Take(2).ToArray(), 1, 2, DurableEvent.UndefinedLogId, new VectorTime((LogId, 3L))));
            Log.Tell(new ReplicationRead(1, int.MaxValue, 0, NoFilter.Instance, DurableEvent.UndefinedLogId, Sys.DeadLetters, VectorTime.Zero), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new ReplicationReadSuccess(Array.Empty<DurableEvent>(), 1, 0, DurableEvent.UndefinedLogId, new VectorTime((LogId, 3L))));
        }
        
        [Fact]
        public void EventLog_must_replication_read_events_from_custom_position()
        {
            GenerateEmittedEvents();
            Log.Tell(new ReplicationRead(2, int.MaxValue, int.MaxValue, NoFilter.Instance, DurableEvent.UndefinedLogId, Sys.DeadLetters, VectorTime.Zero), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new ReplicationReadSuccess(generatedEmittedEvents.Skip(1).ToArray(), 2, 2, DurableEvent.UndefinedLogId, new VectorTime((LogId, 3L))));
        }
        
        [Fact]
        public void EventLog_must_replication_read_events_from_custom_position_with_batch_size_limit()
        {
            GenerateEmittedEvents();
            Log.Tell(new ReplicationRead(2, 1, int.MaxValue, NoFilter.Instance, DurableEvent.UndefinedLogId, Sys.DeadLetters, VectorTime.Zero), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new ReplicationReadSuccess(generatedEmittedEvents.Skip(1).Take(1).ToArray(), 2, 2, DurableEvent.UndefinedLogId, new VectorTime((LogId, 3L))));
        }
        
        [Fact]
        public void EventLog_must_replication_read_events_from_custom_position_with_scan_limit()
        {
            GenerateEmittedEvents();
            Log.Tell(new ReplicationRead(2, int.MaxValue, 1, NoFilter.Instance, DurableEvent.UndefinedLogId, Sys.DeadLetters, VectorTime.Zero), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new ReplicationReadSuccess(generatedEmittedEvents.Skip(1).Take(1).ToArray(), 2, 2, DurableEvent.UndefinedLogId, new VectorTime((LogId, 3L))));
        }
        
        [Fact]
        public void EventLog_must_replication_read_events_with_custom_filter()
        {
            GenerateEmittedEvents();
            GenerateReplicatedEvents();
            Log.Tell(new ReplicationRead(1, int.MaxValue, int.MaxValue, new ProcessIdFilter(RemoteLogId), DurableEvent.UndefinedLogId, Sys.DeadLetters, VectorTime.Zero), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new ReplicationReadSuccess(generatedReplicatedEvents, 1, 6, DurableEvent.UndefinedLogId, new VectorTime((LogId, 3L), (RemoteLogId, 9L))));

            Log.Tell(new ReplicationRead(1, int.MaxValue, int.MaxValue, new ProcessIdFilter(LogId), DurableEvent.UndefinedLogId, Sys.DeadLetters, VectorTime.Zero), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new ReplicationReadSuccess(generatedEmittedEvents, 1, 6, DurableEvent.UndefinedLogId, new VectorTime((LogId, 3L), (RemoteLogId, 9L))));
        }
        
        [Fact]
        public void EventLog_must_limit_replication_read_progress_to_given_scan_limit()
        {
            GenerateEmittedEvents();
            Log.Tell(new ReplicationRead(1, int.MaxValue, 1, new ProcessIdFilter(RemoteLogId), DurableEvent.UndefinedLogId, Sys.DeadLetters, VectorTime.Zero), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new ReplicationReadSuccess(generatedReplicatedEvents, 1, 1, DurableEvent.UndefinedLogId, new VectorTime((LogId, 3L))));
        }
        
        [Fact]
        public void EventLog_must_not_replication_read_events_from_index()
        {
            GenerateEmittedEvents(customDestinationAggregateIds: ImmutableHashSet.Create("a1"));
            GenerateEmittedEvents(customDestinationAggregateIds: ImmutableHashSet<string>.Empty);
            Log.Tell(new ReplicationRead(1, int.MaxValue, int.MaxValue, NoFilter.Instance, DurableEvent.UndefinedLogId, Sys.DeadLetters, VectorTime.Zero), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new ReplicationReadSuccess(generatedEmittedEvents, 1, 6, DurableEvent.UndefinedLogId, new VectorTime((LogId, 6L))));
        }
        
        [Fact]
        public void EventLog_must_reply_with_a_failure_message_if_replication_read_fails()
        {
            Log.Tell(new ReplicationRead(ErrorSequenceNr, int.MaxValue, int.MaxValue, NoFilter.Instance, DurableEvent.UndefinedLogId, Sys.DeadLetters, VectorTime.Zero), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new ReplicationReadFailure(new ReplicationReadSourceException(IntegrationTestException.Instance.Message), DurableEvent.UndefinedLogId));
        }
        
        [Fact]
        public void EventLog_must_recover_the_current_sequence_number_on_restart()
        {
            GenerateEmittedEvents();
            Log.Tell(new GetEventLogClock(), replyToProbe.Ref);
            replyToProbe.ExpectMsg<GetEventLogClockSuccess>().Clock.SequenceNr.Should().Be(3L);
            Log.Tell("boom");
            Log.Tell(new GetEventLogClock(), replyToProbe.Ref);
            replyToProbe.ExpectMsg<GetEventLogClockSuccess>().Clock.SequenceNr.Should().Be(3L);
        }
        
        [Fact]
        public async Task EventLog_must_recover_an_adjusted_sequence_number_on_restart()
        {
            var e = new DurableEvent("a", EmitterIdA, processId: LogId, vectorTimestamp: new VectorTime((LogId, 5L)), localLogId: LogId, localSequenceNr: 1);
            RegisterCollaborator(aggregateId: null, collaborator: replyToProbe);
            Log.Tell(new ReplicationWrite(new []{e}, Map((RemoteLogId, new ReplicationMetadata(1, VectorTime.Zero)))));
            replyToProbe.ExpectMsg<Written>();
            await Log.Ask(new AdjustEventLogClock(), Timeout);
            Log.Tell("boom");
            (await Log.Ask<GetEventLogClockSuccess>(new GetEventLogClock(), Timeout)).Clock.SequenceNr.Should().Be(5L);
        }
        
        [Fact]
        public void EventLog_must_recover_the_replication_progress_on_restart()
        {
            Log.Tell(new SetReplicationProgress("x", 17), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new SetReplicationProgressSuccess("x", 17));
            Log.Tell(new SetReplicationProgress("y", 19), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new SetReplicationProgressSuccess("y", 19));
            Log.Tell(new GetReplicationProgresses(), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new GetReplicationProgressesSuccess(Map(("x", 17L), ("y", 19L))));
            Log.Tell("boom");
            Log.Tell(new GetReplicationProgresses(), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new GetReplicationProgressesSuccess(Map(("x", 17L), ("y", 19L))));
        }
        
        [Fact]
        public void EventLog_must_update_the_replication_progress_if_last_read_sequence_nr_greater_than_last_replicated_sequence_nr()
        {
            Log.Tell(new GetReplicationProgresses(), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new GetReplicationProgressesSuccess(ImmutableDictionary<string, long>.Empty));
            WriteReplicationProgress(19, 19);
            Log.Tell(new GetReplicationProgresses(), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new GetReplicationProgressesSuccess(Map((RemoteLogId, 19L))));
        }
        
        [Fact]
        public void EventLog_must_update_an_emitted_events_process_id_and_vector_timestamp_during_if_the_process_id_is_not_defined()
        {
            var evt = new DurableEvent("a", EmitterIdA, processId: DurableEvent.UndefinedLogId);
            var exp = new DurableEvent("a", EmitterIdA, processId: LogId, vectorTimestamp: new VectorTime((LogId, 1L)), localLogId: LogId, localSequenceNr: 1);
            Log.Tell(new Write(new []{evt}, Sys.DeadLetters, replyToProbe.Ref, 0, 0));
            replyToProbe.ExpectMsg<WriteSuccess>().Events.First().Should().Be(exp);
        }
        
        [Fact]
        public void EventLog_must_not_update_an_emitted_events_process_id_and_vector_timestamp_during_if_the_process_id_is_defined()
        {
            var evt = new DurableEvent("a", EmitterIdA, processId: DurableEvent.UndefinedLogId, vectorTimestamp: new VectorTime((LogId, 1L)));
            var exp = new DurableEvent("a", EmitterIdA, processId: LogId, vectorTimestamp: new VectorTime((LogId, 1L)), localLogId: LogId, localSequenceNr: 1);
            Log.Tell(new Write(new []{evt}, Sys.DeadLetters, replyToProbe.Ref, 0, 0));
            replyToProbe.ExpectMsg<WriteSuccess>().Events.First().Should().Be(exp);
        }
        
        [Fact]
        public void EventLog_must_update_a_replicated_events_process_id_and_vector_timestamp_during_if_the_process_id_is_not_defined()
        {
            var evt = new DurableEvent("a", EmitterIdA, processId: DurableEvent.UndefinedLogId, vectorTimestamp: new VectorTime((RemoteLogId, 1L)));
            var exp = new DurableEvent("a", EmitterIdA, processId: LogId, vectorTimestamp: new VectorTime((RemoteLogId, 1L), (LogId, 1L)), localLogId: LogId, localSequenceNr: 1);
            RegisterCollaborator(null, replyToProbe);
            Log.Tell(new ReplicationWrite(new []{evt}, Map((RemoteLogId, new ReplicationMetadata(5, VectorTime.Zero)))));
            replyToProbe.ExpectMsg<Written>().Event.Should().Be(exp);
        }
        
        [Fact]
        public void EventLog_must_not_update_a_replicated_events_process_id_and_vector_timestamp_during_if_the_process_id_is_defined()
        {
            var evt = new DurableEvent("a", EmitterIdA, processId: DurableEvent.UndefinedLogId, vectorTimestamp: new VectorTime((EmitterIdA, 1L)));
            var exp = new DurableEvent("a", EmitterIdA, processId: LogId, vectorTimestamp: new VectorTime((EmitterIdA, 1L)), localLogId: LogId, localSequenceNr: 1);
            RegisterCollaborator(null, replyToProbe);
            Log.Tell(new ReplicationWrite(new []{evt}, Map((RemoteLogId, new ReplicationMetadata(5, VectorTime.Zero)))));
            replyToProbe.ExpectMsg<Written>().Event.Should().Be(exp);
        }
        
        [Fact]
        public void EventLog_must_not_write_events_to_the_target_log_that_are_in_causal_past_of_the_target_log()
        {
            var evt1 = new DurableEvent("i", EmitterIdB, vectorTimestamp: Timestamp(0, 7), processId: RemoteLogId);
            var evt2 = new DurableEvent("j", EmitterIdB, vectorTimestamp: Timestamp(0, 8), processId: RemoteLogId);
            var evt3 = new DurableEvent("k", EmitterIdB, vectorTimestamp: Timestamp(0, 9), processId: RemoteLogId);
            
            RegisterCollaborator(null, replyToProbe);
            Log.Tell(new ReplicationWrite(new []{evt1, evt2}, Map((RemoteLogId, new ReplicationMetadata(5, VectorTime.Zero)))));
            Log.Tell(new ReplicationWrite(new []{evt2, evt3}, Map((RemoteLogId, new ReplicationMetadata(6, VectorTime.Zero)))));
            replyToProbe.ExpectMsg<Written>().Event.Payload.Should().Be("i");
            replyToProbe.ExpectMsg<Written>().Event.Payload.Should().Be("j");
            replyToProbe.ExpectMsg<Written>().Event.Payload.Should().Be("k");
        }
        
        [Fact]
        public void EventLog_must_not_read_events_from_the_source_log_that_are_in_causal_past_of_the_target_log_using_the_target_time_from_the_request()
        {
            GenerateEmittedEvents();
            Log.Tell(new ReplicationRead(1, int.MaxValue, int.MaxValue, NoFilter.Instance, RemoteLogId, Sys.DeadLetters, Timestamp(1, 0)), replyToProbe.Ref);
            replyToProbe.ExpectMsg<ReplicationReadSuccess>().Events
                .Select(x => x.Payload)
                .Should().BeEquivalentTo("a-2", "a-3");
        }
        
        [Fact]
        public void EventLog_must_not_read_events_from_the_source_log_that_are_in_causal_past_of_the_target_log__using_the_target_time_from_the_cache()
        {
            GenerateEmittedEvents();
            Log.Tell(new ReplicationWrite(Array.Empty<DurableEvent>(), Map((RemoteLogId, new ReplicationMetadata(5, Timestamp(2, 0))))));
            Log.Tell(new ReplicationRead(1, int.MaxValue, int.MaxValue, NoFilter.Instance, RemoteLogId, Sys.DeadLetters, Timestamp(1, 0)), replyToProbe.Ref);
            replyToProbe.ExpectMsg<ReplicationReadSuccess>().Events
                .Select(x => x.Payload)
                .Should().BeEquivalentTo("a-3");
        }
        
        [Fact]
        public async Task EventLog_must_delete_all_events_when_requested_sequence_nr_is_higher_than_current()
        {
            GenerateEmittedEvents();
            await Log.Ask(new Delete(generatedEmittedEvents[2].LocalSequenceNr + 1));
            Log.Tell(new Replay(null, 0, 1L), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new ReplaySuccess(Array.Empty<DurableEvent>(), 3L, 0));
        }
        
        [Fact]
        public async Task EventLog_must_not_replay_deleted_events()
        {
            GenerateEmittedEvents();
            await Log.Ask(new Delete(generatedEmittedEvents[1].LocalSequenceNr));
            Log.Tell(new Replay(null, 0, 1L), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new ReplaySuccess(generatedEmittedEvents.Skip(2).Take(1), generatedEmittedEvents[2].LocalSequenceNr, 0));
        }
        
        [Fact]
        public async Task EventLog_must_not_replay_deleted_events_from_an_index()
        {
            GenerateEmittedEvents(customDestinationAggregateIds: ImmutableHashSet.Create("a"));
            await Log.Ask(new Delete(generatedEmittedEvents[1].LocalSequenceNr));
            Log.Tell(new Replay(null, 0, 1L, aggregateId: "a"), replyToProbe.Ref);
            replyToProbe.ExpectMsg(new ReplaySuccess(generatedEmittedEvents.Skip(2).Take(1), generatedEmittedEvents[2].LocalSequenceNr, 0));
        }
        
        [Fact]
        public async Task EventLog_must_not_delete_future_events_when_requested_sequence_nr_is_higher_than_current()
        {
            await Log.Ask(new Delete(10), Timeout);
            GenerateEmittedEvents();
            Log.Tell(new Replay(replyToProbe.Ref, 0, 1L));
            replyToProbe.ExpectMsg(new ReplaySuccess(generatedEmittedEvents,
                generatedEmittedEvents.Last().LocalSequenceNr, 0));
        }
        
        [Fact]
        public async Task EventLog_must_not_mark_already_deleted_events_as_not_deleted()
        {
            GenerateEmittedEvents();
            Log.Tell(new Delete(generatedEmittedEvents[2].LocalSequenceNr));
            await Log.Ask(new Delete(generatedEmittedEvents[1].LocalSequenceNr), Timeout);
            Log.Tell(new Replay(replyToProbe.Ref, 0, 1L, 0, null));
            replyToProbe.ExpectMsg(new ReplaySuccess(Array.Empty<DurableEvent>(), 3L, 0));
        }
    }

    public class IntegrationTestException : Exception
    {
        public static readonly IntegrationTestException Instance = new IntegrationTestException();
        private IntegrationTestException() : base("boom")
        {
        }
    }
}