#region copyright

// -----------------------------------------------------------------------
//  <copyright file="DbConnectionFactory.cs" company="Akka.NET Project">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------

#endregion

using System;
using System.Data.Common;
using System.Linq;
using BindingFlags = System.Reflection.BindingFlags;

namespace Eventuate.Sql
{
    internal static class DbConnectionFactory
    {
        public static Func<DbConnection> Create(string connectionString, string providerName)
        {
            string typeName = "";
            switch (providerName.ToLowerInvariant())
            {
                case "system.data.sqlclient":
                case "sqlserver":
                    typeName = "System.Data.SqlClient.SqlConnection, System.Data.SqlClient";
                    break;
                case "npgsql":
                    typeName = "Npgsql.NpgsqlConnection, Npgsql";
                    break;
                case "mysql.data.mysqlclient":
                case "mysql":
                    typeName = "MySql.Data.MySqlClient.MySqlConnection, MySql.Data";
                    break;
                case "system.data.sqlite":
                case "sqlite":
                    typeName = "System.Data.SQLite.SQLiteConnection , System.Data.SQLite";
                    break;
                default:
                    typeName = providerName;
                    break;
            }

            var connectionType = Type.GetType(typeName, throwOnError: true);
            if (!typeof(DbConnection).IsAssignableFrom(connectionType))
            {
                throw new ArgumentException(
                    $"A configured provider name '{providerName}' refers to type '{connectionType.FullName}' which is not a DbConnection.");
            }

            var ctor = connectionType.GetConstructors(BindingFlags.Public)
                .FirstOrDefault(c =>
                {
                    var parameters = c.GetParameters();
                    return parameters.Length == 1 && parameters[0].ParameterType == typeof(string);
                });

            if (ctor is null)
            {
                throw new ArgumentException(
                    $"Cannot build an SQL connection factory for type '{connectionType.FullName}', as it lacks a public constructor with connection string as its only parameter.");
            }

            var args = new object[] {connectionString};
            return () => (DbConnection) ctor.Invoke(args);
        }
    }
}