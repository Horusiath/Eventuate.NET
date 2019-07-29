#region copyright
// -----------------------------------------------------------------------
//  <copyright file="Cassandra.cs" company="Akka.NET Project">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using Akka.Actor;

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
        public Cassandra(ExtendedActorSystem system)
        {
            
        }
    }
}