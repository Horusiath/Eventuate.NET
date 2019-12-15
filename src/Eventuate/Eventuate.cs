#region copyright
// -----------------------------------------------------------------------
//  <copyright file="Eventuate.cs" company="Akka.NET Project">
//      Copyright (C) 2009-2019 Lightbend Inc. <http://www.lightbend.com>
//      Copyright (C) 2013-2019 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System.IO;
using System.Reflection;
using Akka.Configuration;

namespace Eventuate
{
    public static class EventuateConfig
    {
        /// <summary>
        /// Default configuration for Eventuate.
        /// </summary>
        public static Config Default { get; } = ConfigurationFactory.FromResource("Eventuate.reference.conf", typeof(EventuateConfig).Assembly);
    }
}