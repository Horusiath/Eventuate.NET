#region copyright
// -----------------------------------------------------------------------
//  <copyright file="CassandraEventLog.cs" company="Akka.NET Project">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Dispatch;
using Akka.Event;
using Eventuate.EventLogs;
using Eventuate.Snapshots;

namespace Eventuate.Cassandra
{
    public sealed class CassandraEventLogState : IEventLogState
    {
        public CassandraEventLogState(EventLogClock eventLogClock, EventLogClock eventLogClockSnapshot, DeletionMetadata deletionMetadata)
        {
            EventLogClock = eventLogClock;
            EventLogClockSnapshot = eventLogClockSnapshot;
            DeletionMetadata = deletionMetadata;
        }

        public EventLogClock EventLogClock { get; }
        public EventLogClock EventLogClockSnapshot { get; }
        public DeletionMetadata DeletionMetadata { get; }
    }
    
    
    /// <summary>
    /// An event log actor with [[http://cassandra.apache.org/ Apache Cassandra]] as storage backend. It uses
    /// the <see cref="Cassandra"/> extension to connect to a Cassandra cluster. Applications should create an instance
    /// of this actor using the <see cref="Props"/> method.
    /// 
    /// <code>
    ///   ActorRefFactory factory = ... // ActorSystem or ActorContext
    ///   string logId = "example"      // Unique log id
    /// 
    ///   IActorRef log = factory.ActorOf(CassandraEventLog.Props(logId))
    /// </code>
    /// 
    /// Each event log actor creates two tables in the configured keyspace (see also <see cref="Cassandra"/>). Assuming
    /// the following table prefix
    /// 
    /// <code>
    ///   eventuate.log.cassandra.table-prefix = "log"
    /// </code>
    /// 
    /// and a log `id` with value `example`, the names of these two tables are
    /// 
    ///  - `log_example` which represents the local event log.
    ///  - `log_example_agg` which is an index of the local event log for those events that have non-empty
    ///    <see cref="DurableEvent.DestinationAggregateIds"/> set. It is used for fast recovery
    ///    of event-sourced actors, views, stateful writers and processors that have an
    ///    <see cref="EventsourcedView.AggregateId"/> defined.
    /// </summary>
    /// <seealso cref="Cassandra"/>
    /// <seealso cref="DurableEvent"/>
    public sealed class CassandraEventLog : EventLog<CassandraEventLogSettings, CassandraEventLogState>
    {
        private static readonly Regex validCassandraIdentifier = new Regex("^[a-zA-Z0-9_]+$", RegexOptions.Compiled);

        /// <summary>
        /// Check whether the specified <paramref name="logId"/> is valid for Cassandra
        /// table, column and/or keyspace name usage.
        /// </summary>
        private static bool IsValidEventLogId(string logId) => validCassandraIdentifier.IsMatch(logId);
        
        /// <summary>
        /// Creates a <see cref="CassandraEventLog"/> configuration object.
        /// </summary>
        /// <param name="logId">unique log id</param>
        /// <param name="batching">`true` if write-batching shall be enabled (recommended)</param>
        /// <param name="aggregateIndexing">
        /// `true` if aggregates should be indexed (recommended)
        /// Turn this off only if you don't use aggregate IDs on this event log!
        /// </param>
        public static Akka.Actor.Props Props(string logId, bool batching = true, bool aggregateIndexing = true)
        {
            var logProps = Akka.Actor.Props.Create(() => new CassandraEventLog(logId, aggregateIndexing))
                .WithDispatcher("eventuate.log.dispatchers.write-dispatcher");
            return Akka.Actor.Props.Create(() => new CircuitBreaker(logProps, batching));
        }
        
        private Cassandra cassandra;
        private CassandraReplicationProgressStore progressStore;
        private CassandraDeletedToStore deletedToStore;
        private CassandraEventLogStore eventLogStore;
        private CassandraIndexStore indexStore;

        private IActorRef index = null;
        private long indexSeqNr = 0L;
        private long updateCount = 0L;

        /// <summary>
        /// Creates a new instance of <see cref="CassandraEventLog"/>.
        /// </summary>
        /// <param name="id">unique log id</param>
        /// <param name="aggregateIndexing">
        /// `true` if the event log shall process aggregate indexing (recommended).
        /// Turn this off only if you don't use aggregate IDs on this event log!
        /// </param>
        public CassandraEventLog(string id, bool aggregateIndexing) : base(id)
        {
            if (!IsValidEventLogId(id))
                throw new ArgumentException($"invalid id '{id}' specified - Cassandra allows alphanumeric and underscore characters only");

            this.cassandra = Cassandra.Get(Context.System);
            this.Settings = this.cassandra.Settings;

        }

        protected override void PreStart()
        {
            ActorTaskScheduler.RunTask(async () =>
            {
                await cassandra.Initialized;

                await cassandra.CreateEventTable(Id);
                await cassandra.CreateAggregateEventTable(Id);

                this.progressStore = CreateReplicationProgressStore(cassandra, Id);
                this.deletedToStore = CreateDeletedToStore(cassandra, Id);
                this.eventLogStore = CreateEventLogStore(cassandra, Id);
                this.indexStore = CreateIndexStore(cassandra, Id);
            });
            
            base.PreStart();
        }

        private static CassandraIndexStore CreateIndexStore(Cassandra c, string id) => new CassandraIndexStore(c, id);
        private static CassandraEventLogStore CreateEventLogStore(Cassandra c, string id) => new CassandraEventLogStore(c, id);
        private static CassandraDeletedToStore CreateDeletedToStore(Cassandra c, string id) => new CassandraDeletedToStore(c, id);
        private static CassandraReplicationProgressStore CreateReplicationProgressStore(Cassandra c, string id) => new CassandraReplicationProgressStore(c, id);

        protected override ISnapshotStore SnapshotStore { get; }
        
        public override async Task WriteReplicationProgresses(ImmutableDictionary<string, long> progresses)
        {
            throw new NotImplementedException();
        }

        public override async Task<BatchReadResult> Read(long fromSequenceNr, long toSequenceNr, int max)
        {
            throw new NotImplementedException();
        }

        public override async Task<BatchReadResult> Read(long fromSequenceNr, long toSequenceNr, int max, string aggregateId)
        {
            throw new NotImplementedException();
        }

        public override async Task<BatchReadResult> ReplicationRead(long fromSequenceNr, long toSequenceNr, int max, int scanLimit, Func<DurableEvent, bool> filter)
        {
            throw new NotImplementedException();
        }

        public override async Task Write(IEnumerable<DurableEvent> events, long partition, EventLogClock clock)
        {
            throw new NotImplementedException();
        }

        public override async Task WriteDeletionMetadata(DeletionMetadata metadata)
        {
            throw new NotImplementedException();
        }

        public override async Task WriteEventLogClockSnapshot(EventLogClock clock)
        {
            throw new NotImplementedException();
        }

        public override async Task<long> ReadReplicationProgress(string logId)
        {
            throw new NotImplementedException();
        }

        public override async Task<ImmutableDictionary<string, long>> ReadReplicationProgresses()
        {
            throw new NotImplementedException();
        }

        public override async Task<CassandraEventLogState> RecoverState()
        {
            var dmtask = deletedToStore.ReadDeletedTo();
            var sctask = indexStore.ReadEventLogClockSnapshot();

            await Task.WhenAll(dmtask, sctask);

            var rc = RecoverEventLogClock(sctask.Result);
            
            return new CassandraEventLogState(rc, sctask.Result, new DeletionMetadata(dmtask.Result, ImmutableHashSet<string>.Empty));
        }

        private EventLogClock RecoverEventLogClock(EventLogClock clock)
        {
            throw new NotImplementedException();
        }
    }
}