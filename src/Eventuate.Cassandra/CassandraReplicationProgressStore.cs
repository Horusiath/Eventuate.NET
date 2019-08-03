#region copyright
// -----------------------------------------------------------------------
//  <copyright file="CassandraReplicationProgressStore.cs" company="Akka.NET Project">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;

namespace Eventuate.Cassandra
{
    internal sealed class CassandraReplicationProgressStore
    {
        private readonly Cassandra cassandra;
        private readonly string logId;

        public CassandraReplicationProgressStore(Cassandra cassandra, string logId)
        {
            this.cassandra = cassandra;
            this.logId = logId;
        }

        public async Task<ImmutableDictionary<string, long>> ReadReplicationProgressesAsync()
        {
            var stmt = await cassandra.PreparedReadReplicationProgressesStatement();
            var resultSet = await cassandra.Session.ExecuteAsync(stmt.Bind(logId));

            var builder = ImmutableDictionary.CreateBuilder<string, long>();
            foreach (var row in resultSet)
            {
                builder[row.GetValue<string>("source_log_id")] = row.GetValue<long>("source_log_read_pos");
            }

            return builder.ToImmutable();
        }
        
        public async Task<long> ReadReplicationProgressAsync(string sourceLogId)
        {
            var stmt = await cassandra.PreparedReadReplicationProgressStatement();
            var resultSet = await cassandra.Session.ExecuteAsync(stmt.Bind(logId, sourceLogId));
            if (resultSet.IsExhausted())
                return 0L;
            else
                return resultSet.First().GetValue<long>("source_log_read_pos");
        }
        
        
        public async Task WriteReplicationProgressesAsync(ImmutableDictionary<string, long> progresses)
        {
            var stmt = await cassandra.PreparedWriteReplicationProgressStatement();
            var batch = new BatchStatement();
            foreach (var entry in progresses)
            {
                batch.Add(stmt.Bind(logId, entry.Key, entry.Value));
            }

            await cassandra.Session.ExecuteAsync(batch);
        }
    }
}