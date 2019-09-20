#region copyright
// -----------------------------------------------------------------------
//  <copyright file="SqlSnapshotStore.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Data.Common;
using Akka.Configuration;

namespace Eventuate.Sql
{
    /// <summary>
    /// Type used to configure <see cref="SqlSnapshotStore{TSnapshot}"/>.
    /// </summary>
    public sealed class SqlSnapshotStoreSettings<TSnapshot> where TSnapshot: class
    {
        public SqlSnapshotStoreSettings(Config config)
        {
            this.OperationTimeout = config.HasPath("operation-timeout") ? config.GetTimeSpan("operation-timeout") : default(TimeSpan?);
            this.ConnectionString = config.GetString("connection-string");
            this.ProviderType = DbConnectionFactory.GetProviderType(config.GetString("provider-name"));
            var converterType = Type.GetType(config.GetString("converter-class"), throwOnError: true);
            this.Converter = (ISnapshotConverter<TSnapshot>) Activator.CreateInstance(converterType);
            
            if (String.IsNullOrEmpty(ConnectionString)) throw new ConfigurationException($"'connection-string' HOCON config was not provided");
            if (ProviderType is null) throw new ConfigurationException($"'connection-string' HOCON config was not provided");
            if (Converter is null) throw new ConfigurationException($"'connection-string' HOCON config was not provided");
            
            if (!typeof(DbConnection).IsAssignableFrom(ProviderType))
                throw new ConfigurationException($"provider-name='{ProviderType}' does not inherit from DbConnection");
            
            if (!Converter.SelectStatement.Contains("@emitterId"))
                throw new ConfigurationException($"converter-class='{Converter.GetType().FullName}' SELECT statement definition doesn't contain a required @emitterId parameter");

            if (!Converter.DeleteToStatement.Contains("@sequenceNr"))
                throw new ConfigurationException($"converter-class='{Converter.GetType().FullName}' DELETE statement definition doesn't contain a required @sequenceNr parameter");

        }
        
        public SqlSnapshotStoreSettings(string connectionString, Type providerType, ISnapshotConverter<TSnapshot> converter, TimeSpan? operationTimeout)
        {
            if (String.IsNullOrEmpty(connectionString)) throw new ArgumentNullException(nameof(connectionString));
            if (providerType is null) throw new ArgumentNullException(nameof(providerType));
            if (converter is null) throw new ArgumentNullException(nameof(converter));
            
            if (!typeof(DbConnection).IsAssignableFrom(providerType))
                throw new ArgumentException($"'{providerType}' does not inherit from DbConnection", nameof(providerType));
            
            if (!converter.SelectStatement.Contains("@emitterId"))
                throw new ArgumentException($"'{converter.GetType().FullName}' SELECT statement definition doesn't contain a required @emitterId parameter", nameof(converter));
            
            if (!converter.DeleteToStatement.Contains("@sequenceNr"))
                throw new ArgumentException($"'{converter.GetType().FullName}' DELETE statement definition doesn't contain a required @sequenceNr parameter", nameof(converter));

            ConnectionString = connectionString;
            ProviderType = providerType;
            Converter = converter;
            OperationTimeout = operationTimeout;
        }

        /// <summary>
        /// Connection string to an SQL database of choice.
        /// </summary>
        public string ConnectionString { get; }
        
        /// <summary>
        /// Provider name describing current SQL driver in use. Supported:
        /// 1. `System.Data.SqlClient.SqlConnection` (SqlServer)
        /// 2. `Npgsql.NpgsqlConnection`
        /// 3. `MySql.Data.MySqlClient.MySqlConnection` (MySql)
        /// 4. `System.Data.SQLite.SqliteConnection` (Sqlite)
        /// That type must have public constructor accepting connection string as a parameter.
        /// </summary>
        public Type ProviderType { get; }
        
        /// <summary>
        /// An implementation of<see cref="ISnapshotConverter{TSnapshot}"/> interface, which will be used to convert
        /// between <see cref="Snapshot"/> data and its database record representation.
        /// </summary>
        public ISnapshotConverter<TSnapshot> Converter { get; }
        
        /// <summary>
        /// A time given for a connection to complete database request before timeout is called.
        /// </summary>
        public TimeSpan? OperationTimeout { get; }
        
        /// <summary>
        /// Specifies a <paramref name="connectionString"/> used by <see cref="SqlSnapshotStore"/> configured by
        /// this settings object.
        /// </summary>
        public SqlSnapshotStoreSettings<TSnapshot> WithConnectionString(string connectionString) =>
            new SqlSnapshotStoreSettings<TSnapshot>(connectionString, this.ProviderType, this.Converter, this.OperationTimeout);
        
        /// <summary>
        /// Specifies a type of DB connection driver used to communicate to a database. Supported:
        /// 1. `System.Data.SqlClient.SqlConnection` (SqlServer)
        /// 2. `Npgsql.NpgsqlConnection`
        /// 3. `MySql.Data.MySqlClient.MySqlConnection` (MySql)
        /// 4. `System.Data.SQLite.SqliteConnection` (Sqlite)
        /// That type must have public constructor accepting connection string as a parameter.
        /// </summary>
        public SqlSnapshotStoreSettings<TSnapshot> WithProviderType<TProvider>() where TProvider: DbConnection => 
            new SqlSnapshotStoreSettings<TSnapshot>(this.ConnectionString, typeof(TProvider), this.Converter, this.OperationTimeout);
        
        /// <summary>
        /// Specifies a type of DB connection driver used to communicate to a database. Supported:
        /// 1. `System.Data.SqlClient.SqlConnection` (SqlServer)
        /// 2. `Npgsql.NpgsqlConnection`
        /// 3. `MySql.Data.MySqlClient.MySqlConnection` (MySql)
        /// 4. `System.Data.SQLite.SqliteConnection` (Sqlite)
        /// That type must have public constructor accepting connection string as a parameter.
        /// </summary>
        public SqlSnapshotStoreSettings<TSnapshot> WithProviderType(Type providerType) => 
            new SqlSnapshotStoreSettings<TSnapshot>(this.ConnectionString, providerType, this.Converter, this.OperationTimeout);
        
        /// <summary>
        /// Specifies a <see cref="ISnapshotConverter{TSnapshot}"/> used to map between database records and user defined snapshots.
        /// </summary>
        public SqlSnapshotStoreSettings<TSnapshot> WithConverter(ISnapshotConverter<TSnapshot> converter) => 
            new SqlSnapshotStoreSettings<TSnapshot>(this.ConnectionString, this.ProviderType, converter, this.OperationTimeout);
        
        /// <summary>
        /// Specifies a timeout given for a DB connection to complete the request.
        /// </summary>
        public SqlSnapshotStoreSettings<TSnapshot> WithOperationTimeout(TimeSpan? timeout) => 
            new SqlSnapshotStoreSettings<TSnapshot>(this.ConnectionString, this.ProviderType, this.Converter, timeout);
    }
}