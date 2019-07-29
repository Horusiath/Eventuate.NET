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
using System.Threading;
using System.Threading.Tasks;
using Eventuate.Snapshots;

namespace Eventuate.Sql
{
    public sealed class SqlSnapshotStore : ISnapshotStore
    {
        private readonly SqlSnapshotStoreSettings settings;
        private readonly Func<DbConnection> dbFactory;
        private readonly ISnapshotConverter converter;

        public SqlSnapshotStore(SqlSnapshotStoreSettings settings)
        {
            this.settings = settings;
            this.dbFactory = DbConnectionFactory.Create(settings.ConnectionString, settings.ProviderName);
            this.converter = (ISnapshotConverter) Activator.CreateInstance(Type.GetType(settings.ConverterType, throwOnError: true));
        }

        public Task Delete(long lowerSequenceNr)
        {
            throw new System.NotImplementedException();
        }

        public Task Save(Snapshot snapshot)
        {
            throw new System.NotImplementedException();
        }

        public Task<Snapshot> Load(string emitterId)
        {
            throw new System.NotImplementedException();
        }

        private async Task<DbConnection> OpenConnection(CancellationToken cancellationToken)
        {
            var connection = dbFactory();
            await connection.OpenAsync(cancellationToken);
            return connection;
        }
    }
    
    /// <summary>
    /// Type used to configure <see cref="SqlSnapshotStore"/>.
    /// </summary>
    public sealed class SqlSnapshotStoreSettings
    {
        public SqlSnapshotStoreSettings(string connectionString, string providerName, string converterType)
        {
            ConnectionString = connectionString;
            ProviderName = providerName;
            ConverterType = converterType;
        }

        /// <summary>
        /// Connection string to an SQL database of choice.
        /// </summary>
        public string ConnectionString { get; }
        
        /// <summary>
        /// Provider name describing current SQL driver in use. Supported:
        /// 1. `System.Data.SqlClient` (SqlServer)
        /// 2. `Npgsql`
        /// 3. `MySql.Data.MySqlClient` (MySql)
        /// 4. `System.Data.SQLite` (Sqlite)
        /// </summary>
        public string ProviderName { get; }
        
        /// <summary>
        /// A fully qualified name targetting a type implementing <see cref="ISnapshotConverter"/> interface,
        /// which will be used to convert between <see cref="Snapshot"/> data and its database record representation.
        /// </summary>
        public string ConverterType { get; }
    }
}