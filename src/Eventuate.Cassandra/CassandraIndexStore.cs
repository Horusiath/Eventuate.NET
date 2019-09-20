#region copyright
// -----------------------------------------------------------------------
//  <copyright file="CassandraIndexStore.cs">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;
using Eventuate.EventLogs;

namespace Eventuate.Cassandra
{
    public class CassandraIndexStore
    {
        
        private readonly Cassandra cassandra;
        private readonly string logId;
        private Task<PreparedStatement> preparedReadAggregateEventStatement;
        private Task<PreparedStatement> preparedWriteAggregateEventStatement;

        public CassandraIndexStore(Cassandra cassandra, string logId)
        {
            this.cassandra = cassandra;
            this.logId = logId;
            this.preparedReadAggregateEventStatement = cassandra.PrepareReadAggregateEvents(logId);
            this.preparedWriteAggregateEventStatement = cassandra.PrepareWriteAggregateEvent(logId);
        }

        public async Task<EventLogClock> ReadEventLogClockSnapshot()
        {
            var stmt = await preparedReadAggregateEventStatement;
            var resultSet = await cassandra.Session.ExecuteAsync(stmt.Bind(logId));
            return resultSet.IsExhausted() 
                ? new EventLogClock() 
                : cassandra.ClockFromBytes(resultSet.First().GetValue<byte[]>("clock"));
        }
        
        public async Task<EventLogClock> WriteEventLogClockSnapshot(EventLogClock clock)
        {
            var stmt = await preparedWriteAggregateEventStatement;
            await cassandra.Session.ExecuteAsync(stmt.Bind(logId, cassandra.ClockToBytes(clock)));
            return clock;
        }

        public async Task<EventLogClock> Write(CassandraIndex.AggregateEvents aggregateEvents, EventLogClock clock)
        {
            await WriteAggregateEventsAsync(aggregateEvents);
            return await WriteEventLogClockSnapshot(clock); // must be after other writes
        }

        public async Task WriteAggregateEventsAsync(CassandraIndex.AggregateEvents aggregateEvents)
        {
            foreach (var entry in aggregateEvents.Events)
            {
                await WriteAggregateEventsAsync(entry.Key, entry.Value);
            }
        }

        public async Task WriteAggregateEventsAsync(string aggregateId, ImmutableArray<DurableEvent> events)
        {
            var stmt = await preparedWriteAggregateEventStatement;
            var batch = new BatchStatement();
            foreach (var e in events)
            {
                batch.Add(stmt.Bind(aggregateId, e.LocalSequenceNr, cassandra.EventToBytes(e)));
            }

            await cassandra.Session.ExecuteAsync(batch);
        }

        public IAsyncEnumerator<DurableEvent> AggregateEventIterator(string aggregateId, long fromSequenceNr, long toSequenceNr, int fetchSize) =>
            new AggregateEventEnumerator(cassandra, preparedReadAggregateEventStatement, aggregateId, fromSequenceNr, toSequenceNr, fetchSize);

        internal sealed class AggregateEventEnumerator : IAsyncEnumerator<DurableEvent>
        {
            private readonly Cassandra cassandra;
            private readonly Task<PreparedStatement> task;
            private readonly string aggregateId;
            private readonly long toSequenceNr;
            private readonly int fetchSize;

            private long currentSequenceNr;
            private PreparedStatement statement;
            private IEnumerator<Row> currentEnumerator;
            private long rowCount = 0;

            public AggregateEventEnumerator(Cassandra cassandra, Task<PreparedStatement> task, string aggregateId, long fromSequenceNr, long toSequenceNr, int fetchSize)
            {
                this.cassandra = cassandra;
                this.task = task;
                this.aggregateId = aggregateId;
                this.currentSequenceNr = fromSequenceNr;
                this.toSequenceNr = toSequenceNr;
                this.fetchSize = fetchSize;
            }

            public async ValueTask<bool> MoveNextAsync()
            {
                if (this.statement is null)
                {
                    this.statement = await task;
                    this.currentEnumerator = await NewEnumerator();
                }

                while (true)
                {
                    if (currentEnumerator.MoveNext())
                    {
                        var row = currentEnumerator.Current;
                        currentSequenceNr = row.GetValue<long>("sequence_nr");
                        rowCount++;
                        Current = this.cassandra.EventFromBytes(row.GetValue<byte[]>("event"));
                        return true;
                    }
                    else if (rowCount < cassandra.Settings.PartitionSize)
                    {
                        // all events consumed
                        return false;
                    }
                    else
                    {
                        // max result set size reached, fetch again
                        currentSequenceNr++;
                        currentEnumerator = await NewEnumerator();
                        rowCount = 0;
                    }
                }
            }

            private async ValueTask<IEnumerator<Row>> NewEnumerator()
            {
                if (currentSequenceNr > toSequenceNr)
                    return Enumerable.Empty<Row>().GetEnumerator();
                else
                {
                    return (await Read()).GetEnumerator();
                }
            }

            private async Task<RowSet> Read()
            {
                return await cassandra.Session.ExecuteAsync(statement.Bind(aggregateId, currentSequenceNr, toSequenceNr)
                    .SetPageSize(fetchSize));
            }

            public DurableEvent Current { get; private set; } = null;
        }
    }

    public interface IAsyncEnumerator<out T>
    {
        ValueTask<bool> MoveNextAsync();
        T Current { get; }
    }
}