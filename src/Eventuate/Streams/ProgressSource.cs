#region copyright
// -----------------------------------------------------------------------
//  <copyright file="ProgressSource.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

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
