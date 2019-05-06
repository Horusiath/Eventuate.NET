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