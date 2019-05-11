#region copyright
// -----------------------------------------------------------------------
//  <copyright file="ReplicationConnection.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Collections.Generic;
using System.Text;

namespace Eventuate
{
    /// <summary>
    /// A replication connection descriptor.
    /// </summary>
    public readonly struct ReplicationConnection
    {
        /// <summary>
        /// Default name of the remote actor system to connect to.
        /// </summary>
        public const string DefaultRemoteSystemName = "location";

        public ReplicationConnection(string host, int port, string name = null)
        {
            Host = host;
            Port = port;
            Name = name ?? DefaultRemoteSystemName;
        }

        /// <summary>
        /// Host of the remote actor system that runs a <see cref="ReplicationEndpoint"/>.
        /// </summary>
        public string Host { get; }

        /// <summary>
        /// Port of the remote actor system that runs a <see cref="ReplicationEndpoint"/>.
        /// </summary>
        public int Port { get; }

        /// <summary>
        /// Name of the remote actor system that runs a <see cref="ReplicationEndpoint"/>.
        /// </summary>
        public string Name { get; }
    }
}
