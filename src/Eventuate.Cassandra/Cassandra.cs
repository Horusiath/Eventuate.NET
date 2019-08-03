#region copyright
// -----------------------------------------------------------------------
//  <copyright file="Cassandra.cs" company="Akka.NET Project">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Event;
using Akka.IO;
using Akka.Serialization;
using Cassandra;
using Eventuate.EventLogs;

namespace Eventuate.Cassandra
{
    public class CassandraProvider : ExtensionIdProvider<Cassandra>
    {
        public override Cassandra CreateExtension(ExtendedActorSystem system)
        {
            return new Cassandra(system);
        }
    }

    /// <summary>
    /// An Akka extension for using [[http://cassandra.apache.org/ Apache Cassandra]] as event log storage backend.
    /// The extension connects to the configured Cassandra cluster and creates the configured keyspace if it doesn't
    /// exist yet. Keyspace auto-creation can be turned off by setting
    /// 
    /// <code>
    ///   eventuate.log.cassandra.keyspace-autocreate = false
    /// </code>
    /// 
    /// The name of the keyspace defaults to `eventuate` and can be configured with
    /// 
    /// <code>
    ///   eventuate.log.cassandra.keyspace = "eventuate"
    /// </code>
    /// 
    /// The Cassandra cluster contact points can be configured with
    /// 
    /// <code>
    ///   eventuate.log.cassandra.contact-points = [host1[:port1], host2[:port2], ...]
    /// </code>
    /// 
    /// Ports are optional and default to `9042` according to
    /// 
    /// <code>
    ///   eventuate.log.cassandra.default-port = 9042
    /// </code>
    /// 
    /// This extension also creates two index tables for storing replication progress data and event log indexing
    /// progress data. The names of these tables have a prefix defined by
    /// 
    /// <code>
    ///   eventuate.log.cassandra.table-prefix = "log"
    /// </code>
    /// 
    /// Assuming a `log` prefix
    /// 
    ///  - the replication progress table name is `log_rp` and
    ///  - the log indexing progress table name is `log_snr`.
    /// 
    /// If two instances of this extensions are created concurrently by two different actor systems, index table
    /// creation can fail (see [[https://issues.apache.org/jira/browse/CASSANDRA-8387 CASSANDRA-8387]]). It is
    /// therefore recommended to initialize a clean Cassandra cluster with a separate administrative application
    /// that only creates an instance of this Akka extension before creating [[CassandraEventLog]] actors. This
    /// must be done only once. Alternatively, different actor systems can be configured with different
    /// `eventuate.log.cassandra.keyspace` names. In this case they won't share a keyspace and index tables and
    /// concurrent initialization is not an issue.
    /// 
    /// </summary>
    /// <seealso cref="CassandraEventLog"/>
    public class Cassandra : IExtension
    {
        public static Cassandra Get(ActorSystem system) => system.WithExtension<Cassandra, CassandraProvider>();

        private readonly ExtendedActorSystem system;
        private readonly ILoggingAdapter logger;
        private readonly CassandraStatements statements;
        private ISession session;
        private ICluster cluster;

        public Task Initialized { get; }
        public ISession Session
        {
            [MethodImpl(MethodImplOptions.NoInlining)]
            get
            {
                if (session is null)
                    throw new InvalidOperationException("Session is not initialized yet. Await for `Initialized` property and try again afterwards.");
                
                return session;
            }
        }

        public ICluster Cluster
        {
            [MethodImpl(MethodImplOptions.NoInlining)]
            get
            {
                if (cluster is null)
                    throw new InvalidOperationException("Cluster is not initialized yet. Await for `Initialized` property and try again afterwards.");
                
                return cluster;
            }
        }

        public Cassandra(ExtendedActorSystem system)
        {
            this.system = system;
            Settings = new CassandraEventLogSettings(system.Settings.Config.GetConfig("eventuate.log"));
            this.ClockSerializer = system.Serialization.FindSerializerForType(typeof(EventLogClock));
            this.EventSerializer = system.Serialization.FindSerializerForType(typeof(DurableEvent));
            this.logger = system.Log;
            this.statements = new CassandraStatements(Settings);
            system.RegisterOnTermination(() =>
            {
                session?.Dispose();
                cluster?.Dispose();
            });
            this.Initialized = Task.Run(this.InitializeAsync);
        }

        private async Task InitializeAsync()
        {
            this.session = await ConnectAsync();
            this.cluster = session.Cluster;
            try
            {
                if (Settings.KeyspaceAutoCreate)
                    await session.ExecuteAsync(new SimpleStatement(statements.CreateKeySpaceStatement));

                var stmt = new BatchStatement()
                    .Add(new SimpleStatement(statements.CreateEventLogClockTableStatement))
                    .Add(new SimpleStatement(statements.CreateReplicationProgressTableStatement))
                    .Add(new SimpleStatement(statements.CreateDeletedToTableStatement));

                await session.ExecuteAsync(stmt);
            }
            catch (Exception e)
            {
                logger.Error(e, "Failed to initialize cassandra extension");
                await system.Terminate();
                throw;
            }
        }

        

        private async Task<ISession> ConnectAsync(int retries = 0)
        {
            try
            {
                return this.Settings.ClusterBuilder.Build().Connect();
            }
            catch (Exception e) when (retries < Settings.ConnectionRetryMax)
            {
                this.logger.Error(e, "Cannot connect to cluster (attempt {0}/{1} ...)", retries + 1, Settings.ConnectionRetryMax + 1);
                await Task.Delay(Settings.ConnectionRetryDelay);
                return await ConnectAsync(retries + 1);
            }
        }

        /// <summary>
        /// Settings used by the Cassandra storage backend. Closed when the `ActorSystem` of
        /// this extension terminates.
        /// </summary>
        public CassandraEventLogSettings Settings { get; }
        
        internal Serializer ClockSerializer { get; }
        internal Serializer EventSerializer { get; }
        
        public async Task CreateEventTable(string logId) => 
            await session.ExecuteAsync(new SimpleStatement(statements.CreateEventTableStatement(logId)));

        public async Task CreateAggregateEventTable(string logId) => 
            await session.ExecuteAsync(new SimpleStatement(statements.CreateAggregateEventTableStatement(logId)));

        public async Task<PreparedStatement> PrepareWriteEvent(string logId)
        {
            var stmt = await session.PrepareAsync(statements.WriteEventStatement(logId));
            return stmt.SetConsistencyLevel(Settings.WriteConsistency);
        }
        
        public async Task<PreparedStatement> PrepareReadEvents(string logId)
        {
            var stmt = await session.PrepareAsync(statements.ReadEventStatement(logId));
            return stmt.SetConsistencyLevel(Settings.ReadConsistency);
        }
        
        public async Task<PreparedStatement> PrepareWriteAggregateEvent(string logId)
        {
            var stmt = await session.PrepareAsync(statements.WriteAggregateEventStatement(logId));
            return stmt.SetConsistencyLevel(Settings.WriteConsistency);
        }
        
        public async Task<PreparedStatement> PrepareReadAggregateEvents(string logId)
        {
            var stmt = await session.PrepareAsync(statements.ReadAggregateEventStatement(logId));
            return stmt.SetConsistencyLevel(Settings.ReadConsistency);
        }

        public async Task<PreparedStatement> PreparedWriteEventLogClockStatement()
        {
            var stmt = await session.PrepareAsync(statements.WriteEventLogClockStatement);
            return stmt.SetConsistencyLevel(Settings.WriteConsistency);
        }

        public async Task<PreparedStatement> PreparedReadEventLogClockStatement()
        {
            var stmt = await session.PrepareAsync(statements.ReadEventLogClockStatement);
            return stmt.SetConsistencyLevel(Settings.ReadConsistency);
        }
        
        public async Task<PreparedStatement> PreparedWriteReplicationProgressStatement()
        {
            var stmt = await session.PrepareAsync(statements.WriteReplicationProgressStatement);
            return stmt.SetConsistencyLevel(Settings.WriteConsistency);
        }
        
        public async Task<PreparedStatement> PreparedReadReplicationProgressesStatement()
        {
            var stmt = await session.PrepareAsync(statements.ReadReplicationProgressesStatement);
            return stmt.SetConsistencyLevel(Settings.ReadConsistency);
        }
        
        public async Task<PreparedStatement> PreparedReadReplicationProgressStatement()
        {
            var stmt = await session.PrepareAsync(statements.ReadReplicationProgressStatement);
            return stmt.SetConsistencyLevel(Settings.ReadConsistency);
        }
        
        public async Task<PreparedStatement> PreparedWriteDeletedToStatement()
        {
            var stmt = await session.PrepareAsync(statements.WriteDeletedToStatement);
            return stmt.SetConsistencyLevel(Settings.WriteConsistency);
        }

        public async Task<PreparedStatement> PreparedReadDeletedToStatement()
        {
            var stmt = await session.PrepareAsync(statements.ReadDeletedToStatement);
            return stmt.SetConsistencyLevel(Settings.ReadConsistency);
        }

        public EventLogClock ClockFromBytes(byte[] payload) => 
            (EventLogClock) ClockSerializer.FromBinary(payload, typeof(EventLogClock));

        public DurableEvent EventFromBytes(byte[] payload) => 
            (DurableEvent) EventSerializer.FromBinary(payload, typeof(DurableEvent));

        public byte[] ClockToBytes(EventLogClock clock) => ClockSerializer.ToBinary(clock);

        public byte[] EventToBytes(DurableEvent e) => EventSerializer.ToBinary(e);
    }
}