#region copyright
// -----------------------------------------------------------------------
//  <copyright file="MultiNodeReplicationConfig.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System.Configuration;
using Akka.Configuration;
using Akka.Remote.TestKit;

namespace Eventuate.Tests.MultiNode
{
    public abstract class MultiNodeReplicationConfig : MultiNodeConfig
    {
        public static Config DefaultConfig = ConfigurationFactory.ParseString(@"
          akka.test.single-expect-default = 20s
          akka.testconductor.barrier-timeout = 60s
          akka.loglevel = ""ERROR""
          
          eventuate.log.write-batch-size = 3
          eventuate.log.replication.retry-delay = 1s
          eventuate.log.replication.failure-detection-limit = 60s
          
          eventuate.snapshot.filesystem.dir = target/test-snapshot
        ");
    }
}