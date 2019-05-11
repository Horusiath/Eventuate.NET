using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Serialization;
using Eventuate.EventLogs;
using Eventuate.Snapshots;
using Spreads.LMDB;
using Config = Akka.Configuration.Config;

namespace Eventuate.Lmdb
{
    public sealed class LmdbEventLogSettings : IEventLogSettings
    {
        public LmdbEventLogSettings(Config config)
        {
            this.ReadTimeout = config.GetTimeSpan("eventuate.log.read-timeout");

            var lmdb = config.GetConfig("eventuate.log.lmdb");
            this.RootDir = lmdb.GetString("dir");
            this.DbName = lmdb.GetString("db-name");
            this.StateSnapshotLimit = lmdb.GetInt("state-snapshot-limit");
            this.DeletionBatchSize = lmdb.GetInt("deletion-batch-size");
            this.DeletionRetryDelay = lmdb.GetTimeSpan("deletion-retry-delay");
        }

        public TimeSpan ReadTimeout { get; }
        public string RootDir { get; }
        public int StateSnapshotLimit { get; }
        public int DeletionBatchSize { get; }
        public TimeSpan DeletionRetryDelay { get; }
        public long PartitionSize { get; } = long.MaxValue;
        public int InitRetryMax { get; } = 0;
        public TimeSpan InitRetryDelay { get; } = TimeSpan.Zero;
        public string DbName { get;  }
    }

    public sealed class LmdbEventLogState : IEventLogState
    {
        public LmdbEventLogState(EventLogClock eventLogClock, DeletionMetadata deletionMetadata)
        {
            EventLogClock = eventLogClock;
            DeletionMetadata = deletionMetadata;
        }

        public EventLogClock EventLogClock { get; }
        public DeletionMetadata DeletionMetadata { get; }
    }
    
    public sealed class LmdbEventLog : EventLog<LmdbEventLogSettings, LmdbEventLogState>
    {
        public static Props Props(string logId, string prefix = "log", bool batching = true, ISnapshotStore snapshotStore = null)
        {
            var logProps = Akka.Actor.Props.Create(() => new LmdbEventLog(logId, prefix, snapshotStore)).WithDispatcher("eventuate.log.dispatchers.write-dispatcher");
            return batching ? Akka.Actor.Props.Create(() => new BatchingLayer(logProps)) : logProps;
        }

        private readonly string id;
        private readonly string prefix;
        private readonly LMDBEnvironment env;
        private readonly Database db;
        private readonly Serialization serialization;

        public LmdbEventLog(string id, string prefix, ISnapshotStore snapshotStore = null) : base(id)
        {
            this.id = id;
            this.prefix = prefix;
            this.serialization = Context.System.Serialization;
            this.Settings = new LmdbEventLogSettings(Context.System.Settings.Config);
            this.env = LMDBEnvironment.Create(this.Settings.RootDir);
            this.db = this.env.OpenDatabase(this.Settings.DbName, new DatabaseConfig(DbFlags.Create));
            this.SnapshotStore = snapshotStore ?? new LmdbSnapshotStore(this.serialization, this.env, this.db);
        }

        protected override ISnapshotStore SnapshotStore { get; }
        public override Task WriteReplicationProgresses(ImmutableDictionary<string, long> progresses)
        {
            throw new NotImplementedException();
        }

        public override Task<BatchReadResult> Read(long fromSequenceNr, long toSequenceNr, int max)
        {
            throw new NotImplementedException();
        }

        public override Task<BatchReadResult> Read(long fromSequenceNr, long toSequenceNr, int max, string aggregateId)
        {
            throw new NotImplementedException();
        }

        public override Task<BatchReadResult> ReplicationRead(long fromSequenceNr, long toSequenceNr, int max, int scanLimit, Func<DurableEvent, bool> filter)
        {
            throw new NotImplementedException();
        }

        public override Task Write(IEnumerable<DurableEvent> events, long partition, EventLogClock clock)
        {
            throw new NotImplementedException();
        }

        public override Task WriteDeletionMetadata(DeletionMetadata metadata)
        {
            throw new NotImplementedException();
        }

        public override Task WriteEventLogClockSnapshot(EventLogClock clock)
        {
            throw new NotImplementedException();
        }

        public override Task<long> ReadReplicationProgress(string logId)
        {
            throw new NotImplementedException();
        }

        public override Task<ImmutableDictionary<string, long>> ReadReplicationProgresses()
        {
            throw new NotImplementedException();
        }

        public override Task<LmdbEventLogState> RecoverState()
        {
            throw new NotImplementedException();
        }
    }
}