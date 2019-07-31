#region copyright
// -----------------------------------------------------------------------
//  <copyright file="CassandraIndexStore.cs" company="Akka.NET Project">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System.Threading.Tasks;
using Eventuate.EventLogs;

namespace Eventuate.Cassandra
{
    internal class CassandraIndexStore
    {
        public CassandraIndexStore(Cassandra cassandra, string logId)
        {
            throw new System.NotImplementedException();
        }

        public async Task<EventLogClock> ReadEventLogClockSnapshot()
        {
            throw new System.NotImplementedException();
        }
    }
}