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
using Dapper;
using Dapper.Contrib.Extensions;
using Eventuate.Snapshots;

namespace Eventuate.Sql
{
    public sealed class SqlSnapshotStore<TSnapshot> : ISnapshotStore where TSnapshot: class
    {
        private readonly SqlSnapshotStoreSettings<TSnapshot> settings;
        private readonly Func<DbConnection> dbFactory;
        private readonly ISnapshotConverter<TSnapshot> converter;

        public SqlSnapshotStore(SqlSnapshotStoreSettings<TSnapshot> settings)
        {
            this.settings = settings;
            this.dbFactory = DbConnectionFactory.Create(settings.ConnectionString, settings.ProviderType);
            this.converter = settings.Converter;
        }

        public async Task Delete(long lowerSequenceNr)
        {
            await this.UsingConnection(connection =>
            {
                if (settings.OperationTimeout.HasValue)
                    return connection.ExecuteAsync(converter.DeleteToStatement, new { sequenceNr = lowerSequenceNr}, commandTimeout: (int) settings.OperationTimeout.Value.TotalMilliseconds);
                else
                    return connection.ExecuteAsync(converter.DeleteToStatement, new { sequenceNr = lowerSequenceNr});
            });
        }

        public async Task Save(Snapshot snapshot)
        {
            var record = converter.FromSnapshot(snapshot);
            await this.UsingConnection(connection =>
            {
                if (settings.OperationTimeout.HasValue)
                    return connection.InsertAsync(record, commandTimeout: (int) settings.OperationTimeout.Value.TotalMilliseconds);
                else
                    return connection.InsertAsync(record);
            });
        }

        public async Task<Snapshot> Load(string emitterId)
        {
            return await this.UsingConnection(async connection =>
            {
                var record = settings.OperationTimeout.HasValue
                    ? connection.QueryFirstAsync<TSnapshot>(converter.SelectStatement, new { emitterId }, commandTimeout: (int) settings.OperationTimeout.Value.TotalMilliseconds)
                    : connection.QueryFirstAsync<TSnapshot>(converter.SelectStatement, new { emitterId });
                
                return converter.ToSnapshot(await record);
            });
        }

        private async Task<T> UsingConnection<T>(Func<DbConnection, Task<T>> operation)
        {
            if (settings.OperationTimeout.HasValue)
            {
                using (var cts = new CancellationTokenSource(settings.OperationTimeout.Value))
                using (var connection = this.dbFactory())
                {
                    await connection.OpenAsync(cts.Token);
                    return await operation(connection);
                }
            }
            else
            {
                using (var connection = this.dbFactory())
                {
                    await connection.OpenAsync();
                    return await operation(connection);
                }
            }
        }
    }
}