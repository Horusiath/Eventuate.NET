#region copyright
// -----------------------------------------------------------------------
//  <copyright file="CassandraEventLogSettings.cs">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using Akka.Configuration;
using Cassandra;
using Eventuate.EventLogs;

namespace Eventuate.Cassandra
{
    public sealed class CassandraEventLogSettings : IEventLogSettings
    {
        public CassandraEventLogSettings(Config config) : this(
            writeTimeout: config.GetTimeSpan("write-timeout"), 
            writeBatchSize: config.GetInt("write-batch-size"), 
            keyspace: config.GetString("cassandra.keyspace"), 
            keyspaceAutoCreate: config.GetBoolean("cassandra.keyspace-autocreate"), 
            replicationFactor: config.GetInt("cassandra.replication-factor"), 
            tablePrefix: config.GetString("cassandra.table-prefix"), 
            readConsistency: (ConsistencyLevel)Enum.Parse(typeof(ConsistencyLevel), config.GetString("cassandra.read-consistency"), ignoreCase: true), 
            writeConsistency: (ConsistencyLevel)Enum.Parse(typeof(ConsistencyLevel), config.GetString("cassandra.write-consistency"), ignoreCase: true), 
            writeRetryMax: config.GetInt("cassandra.write-retry-max"), 
            defaultPort: config.GetInt("cassandra.default-port"), 
            contactPoints: GetContactPoints(config.GetStringList("cassandra.contact-points"), config.GetInt("cassandra.default-port")).ToArray(), 
            partitionSize: config.GetInt("cassandra.partition-size"), 
            indexUpdateLimit: config.GetInt("cassandra.index-update-limit"), 
            initRetryMax: config.GetInt("cassandra.init-retry-max"), 
            initRetryDelay: config.GetTimeSpan("cassandra.init-retry-delay"), 
            deletionRetryDelay: config.GetTimeSpan("cassandra.deletion-retry-delay"), 
            connectionRetryMax: config.GetInt("cassandra.connect-retry-max"), 
            connectionRetryDelay: config.GetTimeSpan("cassandra.connect-retry-delay"), 
            clusterBuilder: new Builder().WithCredentials(
                username: config.GetString("cassandra.username"),
                password: config.GetString("cassandra.password")))
        {
        }

        public CassandraEventLogSettings(
            TimeSpan writeTimeout, 
            int writeBatchSize, 
            string keyspace, 
            bool keyspaceAutoCreate, 
            int replicationFactor, 
            string tablePrefix, 
            ConsistencyLevel readConsistency, 
            ConsistencyLevel writeConsistency, 
            int writeRetryMax, 
            int defaultPort, 
            IReadOnlyList<IPEndPoint> contactPoints, 
            long partitionSize, 
            int indexUpdateLimit, 
            int initRetryMax, 
            TimeSpan initRetryDelay, 
            TimeSpan deletionRetryDelay, 
            int connectionRetryMax, 
            TimeSpan connectionRetryDelay, 
            Builder clusterBuilder)
        {
            if (partitionSize <= writeBatchSize)
            {
                throw new ArgumentException($"eventuate.log.cassandra.partition-size must be greater than eventuate.log.write-batch-size (${writeBatchSize})", nameof(partitionSize));
            }
            
            WriteTimeout = writeTimeout;
            WriteBatchSize = writeBatchSize;
            Keyspace = keyspace;
            KeyspaceAutoCreate = keyspaceAutoCreate;
            ReplicationFactor = replicationFactor;
            TablePrefix = tablePrefix;
            ReadConsistency = readConsistency;
            WriteConsistency = writeConsistency;
            WriteRetryMax = writeRetryMax;
            DefaultPort = defaultPort;
            ContactPoints = contactPoints;
            PartitionSize = partitionSize;
            IndexUpdateLimit = indexUpdateLimit;
            InitRetryMax = initRetryMax;
            InitRetryDelay = initRetryDelay;
            DeletionRetryDelay = deletionRetryDelay;
            ConnectionRetryMax = connectionRetryMax;
            ConnectionRetryDelay = connectionRetryDelay;
            ClusterBuilder = clusterBuilder.AddContactPoints(contactPoints);
        }
        
        public CassandraEventLogSettings Copy(
            TimeSpan? writeTimeout = null, 
            int? writeBatchSize = null, 
            string keyspace = null, 
            bool? keyspaceAutoCreate = null, 
            int? replicationFactor = null, 
            string tablePrefix = null, 
            ConsistencyLevel? readConsistency = null, 
            ConsistencyLevel? writeConsistency = null, 
            int? writeRetryMax = null, 
            int? defaultPort = null, 
            IReadOnlyList<IPEndPoint> contactPoints = null, 
            long? partitionSize = null, 
            int? indexUpdateLimit = null, 
            int? initRetryMax = null, 
            TimeSpan? initRetryDelay = null, 
            TimeSpan? deletionRetryDelay = null, 
            int? connectionRetryMax = null, 
            TimeSpan? connectionRetryDelay = null, 
            Builder clusterBuilder = null) =>
        new CassandraEventLogSettings(
            writeTimeout: writeTimeout ?? this.WriteTimeout, 
            writeBatchSize: writeBatchSize ?? this.WriteBatchSize, 
            keyspace: keyspace ?? this.Keyspace, 
            keyspaceAutoCreate: keyspaceAutoCreate ?? this.KeyspaceAutoCreate, 
            replicationFactor: replicationFactor ?? this.ReplicationFactor, 
            tablePrefix: tablePrefix ?? this.TablePrefix, 
            readConsistency: readConsistency ?? this.ReadConsistency, 
            writeConsistency: writeConsistency ?? this.WriteConsistency, 
            writeRetryMax: writeRetryMax ?? this.WriteRetryMax, 
            defaultPort: defaultPort ?? this.DefaultPort, 
            contactPoints: contactPoints ?? this.ContactPoints, 
            partitionSize: partitionSize ?? this.PartitionSize, 
            indexUpdateLimit: indexUpdateLimit ?? this.IndexUpdateLimit, 
            initRetryMax: initRetryMax ?? this.InitRetryMax, 
            initRetryDelay: initRetryDelay ?? this.InitRetryDelay, 
            deletionRetryDelay: deletionRetryDelay ?? this.DeletionRetryDelay, 
            connectionRetryMax: connectionRetryMax ?? this.ConnectionRetryMax, 
            connectionRetryDelay: connectionRetryDelay ?? this.ConnectionRetryDelay, 
            clusterBuilder: clusterBuilder ?? this.ClusterBuilder);

        public TimeSpan WriteTimeout { get; }
        public int WriteBatchSize { get; }
        
        /// <summary>
        /// Name of the keyspace created/used by Eventuate.
        /// Default: "eventuate".
        /// </summary>
        public string Keyspace { get; }
        
        /// <summary>
        /// Whether or not to auto-create the keyspace.
        /// Default: true.
        /// </summary>
        public bool KeyspaceAutoCreate { get; }
        
        /// <summary>
        /// Replication factor to use when creating the keyspace.
        /// Default: 1.
        /// </summary>
        public int ReplicationFactor { get; }
        
        /// <summary>
        /// Prefix of all tables created/used by Eventuate.
        /// Default: "log".
        /// </summary>
        public string TablePrefix { get; }
        
        /// <summary>
        /// Read consistency level.
        /// Default: "QUORUM".
        /// </summary>
        public ConsistencyLevel ReadConsistency { get; }
        
        /// <summary>
        /// Write consistency level.
        /// Default: "QUORUM".
        /// </summary>
        public ConsistencyLevel WriteConsistency { get; }
        
        /// <summary>
        /// Maximum number of times a failed write should be retried until the event
        /// log actor gives up and stops itself. The delay between write retries can
        /// be configured with eventuate.log.write-timeout.
        /// Default: 65536
        /// </summary>
        public int WriteRetryMax { get; }
        
        /// <summary>
        /// Default port of contact points in the cluster. Ports defined in contact-points override this setting.
        /// Default: 9042.
        /// </summary>
        public int DefaultPort { get; }
        
        /// <summary>
        /// Comma-separated list of contact points in the cluster (comma-separated ip or ip:port list).
        /// Default: ["127.0.0.1:9042"].
        /// </summary>
        public IReadOnlyList<IPEndPoint> ContactPoints { get; }
        
        /// <summary>
        /// Maximum number of events stored per event log partition. Must be greater
        /// than eventuate.log.write-batch-size.
        /// Default: 131072.
        /// </summary>
        public long PartitionSize { get; }
        
        /// <summary>
        /// Minimum number of new events that must have been written before another
        /// index update is triggered.
        /// Default: 64.
        /// </summary>
        public int IndexUpdateLimit { get; }
        
        /// <summary>
        /// Maximum number event log initialization retries. Initialization includes
        /// recovery of the current sequence number and version vector as well as
        /// indexing of not yet indexed log entries.
        /// Default: 3.
        /// </summary>
        public int InitRetryMax { get; }
        
        /// <summary>
        /// Delay between event log initialization retries. Initialization includes
        /// recovery of the current sequence number and version vector as well as
        /// indexing of not yet indexed log entries.
        /// Default: 5 sec.
        /// </summary>
        public TimeSpan InitRetryDelay { get; }
        public TimeSpan DeletionRetryDelay { get; }
        
        /// <summary>
        /// Maximum number of initial connection retries to a Cassandra cluster.
        /// Default: 3.
        /// </summary>
        public int ConnectionRetryMax { get; }
        
        /// <summary>
        /// Delay between initial connection retries to a Cassandra cluster.
        /// Default: 5 sec.
        /// </summary>
        public TimeSpan ConnectionRetryDelay { get; }
        
        public global::Cassandra.Builder ClusterBuilder { get; }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static IEnumerable<IPEndPoint> GetContactPoints(IList<string> contactPoints, int defaultPort)
        {
            if (contactPoints is null || contactPoints.Count == 0)
            {
                throw new ArgumentException($"A cassandra node contact point list cannot be empty.", nameof(contactPoints));
            }

            foreach (var contactPoint in contactPoints)
            {
                var ipWithPort = contactPoint.Split(':');
                switch (ipWithPort.Length)
                {
                    case 2: 
                        if (!IPAddress.TryParse(ipWithPort[0], out var ipAddress))
                            throw new ArgumentException($"Value '{ipWithPort[0]}' is not a valid IP address");
                        if (!ushort.TryParse(ipWithPort[1], out var port))
                            throw new ArgumentException($"Value '{ipWithPort[1]}' is not a valid socket port.");
                        yield return new IPEndPoint(ipAddress, port);
                        break;
                        
                    case 1:
                        if (!IPAddress.TryParse(ipWithPort[0], out ipAddress))
                            throw new ArgumentException($"Value '{ipWithPort[0]}' is not a valid IP address");
                        yield return new IPEndPoint(ipAddress, defaultPort);
                        break;
                        
                    default:
                        throw new ArgumentException($"A contact point must follow either [ip:port] or [ip] pattern but was: " + contactPoint);
                }
            }
        }
    }
}