#region copyright
// -----------------------------------------------------------------------
//  <copyright file="CassandraEventLogStore.cs" company="Akka.NET Project">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System.Threading.Tasks;
using Eventuate.EventLogs;

namespace Eventuate.Cassandra
{
    internal class CassandraEventLogStore
    {
        public CassandraEventLogStore(Cassandra cassandra, string id)
        {
            throw new System.NotImplementedException();
        }

        public async Task<BatchReadResult> ReadAsync(long fromSequenceNr, long toSequenceNr, int max, int fetchSize)
        {
            throw new System.NotImplementedException();
        }
    }
}