#region copyright
// -----------------------------------------------------------------------
//  <copyright file="CassandraIndexStore.cs" company="Akka.NET Project">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Threading.Tasks;
using Cassandra;
using Eventuate.EventLogs;

namespace Eventuate.Cassandra
{
    
    internal class CassandraIndexStore
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
            throw new System.NotImplementedException();
        }
        
        public async Task WriteEventLogClockSnapshot(EventLogClock clock)
        {
            throw new System.NotImplementedException();
        }

        public async Task<EventLogClock> Write(CassandraIndex.AggregateEvents aggregateEvents, EventLogClock clock)
        {
            throw new NotImplementedException();   
        }
    }
}