#region copyright
// -----------------------------------------------------------------------
//  <copyright file="CassandraEventLogStore.cs" company="Akka.NET Project">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Cassandra;
using Eventuate.EventLogs;

namespace Eventuate.Cassandra
{
    internal class CassandraEventLogStore
    {
        private readonly Cassandra cassandra;
        private readonly string logId;
        private readonly Task<PreparedStatement> preparedWriteEventStatement;
        private readonly Task<PreparedStatement> preparedReadEventsStatement;

        public CassandraEventLogStore(Cassandra cassandra, string logId)
        {
            this.cassandra = cassandra;
            this.logId = logId;
            this.preparedReadEventsStatement = cassandra.PrepareReadEvents(logId);
            this.preparedWriteEventStatement = cassandra.PrepareWriteEvent(logId);
        }

        public async Task<BatchReadResult> ReadAsync(long fromSequenceNr, long toSequenceNr, int max, int fetchSize) => 
            await ReadAsync(fromSequenceNr, toSequenceNr, max, fetchSize, int.MaxValue, _ => true);

        public async Task<BatchReadResult> ReadAsync(long fromSequenceNr, long toSequenceNr, int max, int fetchSize, int scanLimit, Func<DurableEvent, bool> filter)
        {
            var enumerator = EventIterator(fromSequenceNr, toSequenceNr, fetchSize);
            var events = new List<DurableEvent>();
            var lastSequenceNr = fromSequenceNr - 1L;
            var scanned = 0;
            var filtered = 0;

            while (filtered < max && scanned < scanLimit && await enumerator.MoveNextAsync())
            {
                var durableEvent = enumerator.Current;
                if (filter(durableEvent))
                {
                    events.Add(durableEvent);
                    filtered++;
                }

                scanned++;
                lastSequenceNr = durableEvent.LocalSequenceNr;
            }
            
            return new BatchReadResult(events, lastSequenceNr);
        }

        public async Task WriteAsync(IEnumerable<DurableEvent> events, long partition)
        {
            var stmt = await preparedWriteEventStatement;
            var batch = new BatchStatement();

            foreach (var durableEvent in events)
            {
                batch.Add(stmt.Bind(partition, durableEvent.LocalSequenceNr, cassandra.EventToBytes(durableEvent)));
            }

            await cassandra.Session.ExecuteAsync(batch);
        }
        
        internal EventEnumerator EventIterator(long fromSequenceNr, long toSequenceNr, int fetchSize) =>
            new EventEnumerator(cassandra, preparedReadEventsStatement, fromSequenceNr, toSequenceNr, fetchSize);

        internal sealed class EventEnumerator : IAsyncEnumerator<DurableEvent>
        {
            private readonly Cassandra cassandra;
            private readonly Task<PreparedStatement> preparedReadEventsStatement;
            private readonly long toSequenceNr;
            private readonly long partitionSize;

            private long currentSequenceNr;
            private long currentPartition;
            private IEnumerator<Row> currentEnumerator = null;
            private readonly int fetchSize;
            private bool shouldRead;

            public EventEnumerator(Cassandra cassandra, Task<PreparedStatement> preparedReadEventsStatement, long fromSequenceNr, long toSequenceNr, int fetchSize)
            {
                this.cassandra = cassandra;
                this.preparedReadEventsStatement = preparedReadEventsStatement;
                this.currentSequenceNr = Math.Max(fromSequenceNr, 1L);
                this.toSequenceNr = toSequenceNr;
                this.fetchSize = fetchSize;
                this.partitionSize = cassandra.Settings.PartitionSize;
                this.currentPartition = PartitionOf(this.currentSequenceNr, partitionSize);
                this.shouldRead = currentSequenceNr != FirstSequenceNr(currentPartition, partitionSize);
            }

            public async ValueTask<bool> MoveNextAsync()
            {
                if (currentEnumerator is null)
                {
                    currentEnumerator = await NewEnumerator();
                }

                while (true)
                {
                    if (currentEnumerator.MoveNext())
                    {
                        var row = currentEnumerator.Current;
                        currentSequenceNr = row.GetValue<long>("sequence_nr");
                        shouldRead = true;
                        Current = cassandra.EventFromBytes(row.GetValue<byte[]>("event"));
                        return true;
                    }
                    else if (shouldRead)
                    {
                        // some events read from current partition, try next partition
                        currentPartition++;
                        currentSequenceNr = FirstSequenceNr(currentPartition, partitionSize);
                        currentEnumerator = await NewEnumerator();
                        shouldRead = false;
                    }
                    else return false; // no events read from current partition, we're done
                }
            }

            public DurableEvent Current { get; private set; }

            private async ValueTask<IEnumerator<Row>> NewEnumerator()
            {
                if (currentSequenceNr > toSequenceNr)
                    return Enumerable.Empty<Row>().GetEnumerator();
                else
                {
                    var result = await ReadAsync(Math.Min(toSequenceNr, LastSequenceNr(currentPartition, partitionSize)));
                    return result.GetEnumerator();
                }
            }

            private async Task<RowSet> ReadAsync(long upperSequenceNr)
            {
                var stmt = await preparedReadEventsStatement;
                return await cassandra.Session.ExecuteAsync(stmt
                    .Bind(currentPartition, currentSequenceNr, upperSequenceNr)
                    .SetPageSize(fetchSize));
            }

            /// <summary>
            /// First sequence number of given <paramref name="partition"/>.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static long FirstSequenceNr(long partition, long partitionSizeMax) => partition * partitionSizeMax + 1L;

            /// <summary>
            /// Partition number for given <paramref name="sequenceNr"/>.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static long PartitionOf(long sequenceNr, long partitionSizeMax) => (sequenceNr == 0L) ? -1L : (sequenceNr - 1L) / partitionSizeMax;
            
            /// <summary>
            /// Last sequence number of given <paramref name="partition"/>.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static long LastSequenceNr(long partition, long partitionSizeMax) => (partition + 1L) * partitionSizeMax;
            
        }
    }
}