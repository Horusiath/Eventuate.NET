using Akka.Configuration;
using System;
using System.Collections.Generic;
using System.Text;

namespace Eventuate.Streams
{
    public sealed class ProgressSourceSettings
    {
        public ProgressSourceSettings(Config config)
        {
            this.ReadTimeout = config.GetTimeSpan("eventuate.log.read-timeout");
        }

        public TimeSpan ReadTimeout { get; }
    }

    public static class ProgressSource
    {
    }
}
