#region copyright
// -----------------------------------------------------------------------
//  <copyright file="CassandraStatements.cs" company="Akka.NET Project">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System.Threading.Tasks;

namespace Eventuate.Cassandra
{
    internal sealed class CassandraStatements
    {
        private readonly CassandraEventLogSettings settings;

        public CassandraStatements(CassandraEventLogSettings settings)
        {
            this.settings = settings;
            
            this.CreateKeySpaceStatement = $@"
                CREATE KEYSPACE IF NOT EXISTS {settings.Keyspace}
                WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : {settings.ReplicationFactor} }}";

            var eventLogClockTable = Table("elc");
            this.CreateEventLogClockTableStatement = $@"
                CREATE TABLE IF NOT EXISTS {eventLogClockTable} (
                  log_id text,
                  clock blob,
                  PRIMARY KEY (log_id))";
            this.WriteEventLogClockStatement = $"INSERT INTO {eventLogClockTable} (log_id, clock) VALUES (?, ?)";
            this.ReadEventLogClockStatement = $"SELECT * FROM {eventLogClockTable} WHERE log_id = ?";

            var replicationProgressTable = Table("rp");
            this.CreateReplicationProgressTableStatement = $@"
                CREATE TABLE IF NOT EXISTS {replicationProgressTable} (
                  log_id text,
                  source_log_id text,
                  source_log_read_pos bigint,
                  PRIMARY KEY (log_id, source_log_id))";
            this.WriteReplicationProgressStatement =
                $"INSERT INTO {replicationProgressTable} (log_id, source_log_id, source_log_read_pos) VALUES (?, ?, ?)";
            this.ReadReplicationProgressStatement =
                $"SELECT * FROM {replicationProgressTable} WHERE log_id = ? AND source_log_id = ?";
            this.ReadReplicationProgressesStatement = $"SELECT * FROM {replicationProgressTable} WHERE log_id = ?";

            var deletedToTable = Table("del");
            this.CreateDeletedToTableStatement = $@"
                CREATE TABLE IF NOT EXISTS {deletedToTable} (
                  log_id text,
                  deleted_to bigint,
                  PRIMARY KEY (log_id))";
            this.WriteDeletedToStatement = $"INSERT INTO {deletedToTable} (log_id, deleted_to) VALUES (?, ?)";
            this.ReadDeletedToStatement = $"SELECT deleted_to FROM {deletedToTable} WHERE log_id = ?";
        }

        private string Table(string suffix) => $"{settings.Keyspace}.{settings.TablePrefix}_{suffix}";

        public string CreateKeySpaceStatement { get; }
        public string CreateEventLogClockTableStatement { get; }
        public string WriteEventLogClockStatement { get; }
        public string ReadEventLogClockStatement { get; }
        public string CreateReplicationProgressTableStatement { get; }
        public string WriteReplicationProgressStatement { get; }
        public string ReadReplicationProgressStatement { get; }
        public string ReadReplicationProgressesStatement { get; }
        public string CreateDeletedToTableStatement { get; }
        public string WriteDeletedToStatement { get; }
        public string ReadDeletedToStatement { get; }

        public string CreateEventTableStatement(string logId) => $@"
            CREATE TABLE IF NOT EXISTS {Table(logId)} (
                partition_nr bigint,
                sequence_nr bigint,
                event blob,
                PRIMARY KEY (partition_nr, sequence_nr))
            WITH COMPACT STORAGE
            ";

        public string WriteEventStatement(string logId) => $@"
            INSERT INTO {Table(logId)} (partition_nr, sequence_nr, event)
            VALUES (?, ?, ?)";
        
        public string ReadEventStatement(string logId) => $@"
            SELECT * FROM {Table(logId)} WHERE
              partition_nr = ? AND
              sequence_nr >= ? AND
              sequence_nr <= ?
            LIMIT {settings.PartitionSize}";

        public string CreateAggregateEventTableStatement(string logId) => $@"
            CREATE TABLE IF NOT EXISTS {Table(logId)}_agg (
              aggregate_id text,
              sequence_nr bigint,
              event blob,
              PRIMARY KEY (aggregate_id, sequence_nr))
              WITH COMPACT STORAGE";
        
        public string WriteAggregateEventStatement(string logId) => $@"
            INSERT INTO {Table(logId)}_agg (aggregate_id, sequence_nr, event)
            VALUES (?, ?, ?)";
        
        public string ReadAggregateEventStatement(string logId) => $@"
            SELECT * FROM {Table(logId)}_agg WHERE
              aggregate_id = ? AND
              sequence_nr >= ? AND
              sequence_nr <= ?
            LIMIT {settings.PartitionSize}";
        
    }
}