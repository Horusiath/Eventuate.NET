#region copyright

// -----------------------------------------------------------------------
//  <copyright file="SqlEventLogSettings.cs" company="Akka.NET Project">
//      Copyright (C) 2009-2019 Lightbend Inc. <http://www.lightbend.com>
//      Copyright (C) 2013-2019 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

#endregion

using System;
using Eventuate.EventLogs;

namespace Eventuate.Sql
{
    public sealed class SqlEventLogSettings<TEvent> : IEventLogSettings where TEvent: class
    {
        public long PartitionSize { get; }
        public int InitRetryMax { get; }
        public TimeSpan InitRetryDelay { get; }
        public TimeSpan DeletionRetryDelay { get; }
        
        public IEventConverter<TEvent> Converter { get; }

        public SqlEventLogSettings(long partitionSize, int initRetryMax, TimeSpan initRetryDelay, TimeSpan deletionRetryDelay, IEventConverter<TEvent> converter)
        {
            PartitionSize = partitionSize;
            InitRetryMax = initRetryMax;
            InitRetryDelay = initRetryDelay;
            DeletionRetryDelay = deletionRetryDelay;
            Converter = converter;
        }
    }
}