#region copyright
// -----------------------------------------------------------------------
//  <copyright file="Records.cs" company="Akka.NET Project">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

namespace Eventuate.Sql
{
    public interface IEventConverter
    {
        DurableEvent ToEvent(object row);

        object FromEvent(DurableEvent durableEvent);
    }

    public interface ISnapshotConverter
    {
        Snapshot ToSnapshot(object row);
        
        object FromSnapshot(Snapshot snapshot);
    }
}