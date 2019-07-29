#region copyright
// -----------------------------------------------------------------------
//  <copyright file="SqlEventLog.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading.Tasks;
using Eventuate.EventLogs;
using Eventuate.Snapshots;

namespace Eventuate.Sql
{
    public class SqlEventLog : EventLog<SqlEventLogSettings, SqlEventLogState>
    {
        public SqlEventLog(string id, ISnapshotStore snapshotStore) : base(id)
        {
            SnapshotStore = snapshotStore;
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

        public override Task<SqlEventLogState> RecoverState()
        {
            throw new NotImplementedException();
        }
    }

    public class SqlEventLogState : IEventLogState
    {
        public EventLogClock EventLogClock { get; set; }
        public DeletionMetadata DeletionMetadata { get; set; }
    }

    public class SqlEventLogSettings : IEventLogSettings
    {
        public long PartitionSize { get; }
        public int InitRetryMax { get; }
        public TimeSpan InitRetryDelay { get; }
        public TimeSpan DeletionRetryDelay { get; }

        public SqlEventLogSettings(long partitionSize, int initRetryMax, TimeSpan initRetryDelay, TimeSpan deletionRetryDelay)
        {
            PartitionSize = partitionSize;
            InitRetryMax = initRetryMax;
            InitRetryDelay = initRetryDelay;
            DeletionRetryDelay = deletionRetryDelay;
        }
    }
}