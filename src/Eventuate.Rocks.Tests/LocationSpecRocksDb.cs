#region copyright
// -----------------------------------------------------------------------
//  <copyright file="LocationSpecRocksDb.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using Akka.Actor;
using Eventuate.Snapshots.Filesystem;
using Eventuate.Tests;

namespace Eventuate.Rocks.Tests
{
    public abstract class MultiLocationSpecRocksDb : MultiLocationSpec
    {
        public override Props LogFactory(string id) => RocksDbEventLog.Props(id);
    }
}