#region copyright
// -----------------------------------------------------------------------
//  <copyright file="CassandraDeletedToStore.cs">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System.Linq;
using System.Threading.Tasks;

namespace Eventuate.Cassandra
{
    internal readonly struct CassandraDeletedToStore
    {
        private readonly Cassandra cassandra;
        private readonly string logId;

        public CassandraDeletedToStore(Cassandra cassandra, string logId)
        {
            this.cassandra = cassandra;
            this.logId = logId;
        }

        public async Task WriteDeletedTo(long deletedTo)
        {
            var stmt = await cassandra.PreparedWriteDeletedToStatement();
            await cassandra.Session.ExecuteAsync(stmt.Bind(logId, deletedTo));
        }

        public async Task<long> ReadDeletedTo()
        {
            var stmt = await cassandra.PreparedReadDeletedToStatement();
            var resultSet = await cassandra.Session.ExecuteAsync(stmt.Bind(logId));
            if (resultSet.IsExhausted())
                return 0L;
            else
                return resultSet.First().GetValue<long>("deleted_to");
        }
    }
}