#region copyright
// -----------------------------------------------------------------------
//  <copyright file="MultiNodeReplicationEndpoint.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System.Collections.Immutable;
using Akka.Actor;
using Akka.Remote.TestKit;

namespace Eventuate.Tests.MultiNode
{
    public interface IMultiNodeReplicationEndpoint
    {
        Props LogProps(string logId);
    }
        
    public static class MultiNodeReplicationEndpoint
    {
        public static string LogName(this MultiNodeSpec self)
        {
            var s = self.GetType().ToString();
            return s.Substring(0, s.IndexOf("Spec"));
        }

        public static ReplicationEndpoint CreateEndpoint<T>(this T self, string endpointId, ImmutableHashSet<ReplicationConnection> connections)
            where T: MultiNodeSpec, IMultiNodeReplicationEndpoint =>
            CreateEndpoint(self, endpointId, ImmutableHashSet<string>.Empty.Add(self.LogName()), connections);

        public static ReplicationEndpoint CreateEndpoint<T>(this T self, string endpointId, ImmutableHashSet<string> logNames,
            ImmutableHashSet<ReplicationConnection> connections, bool activate = true)
            where T: MultiNodeSpec, IMultiNodeReplicationEndpoint 
        {
            var endpoint = new ReplicationEndpoint(self.Sys, endpointId, logNames, self.LogProps, connections);
            if (activate) endpoint.Activate();
            return endpoint;
        }

        public static ReplicationConnection ToReplicationConnection(this Address address) => 
            new ReplicationConnection(address.Host, address.Port.Value, address.System);
    }
}