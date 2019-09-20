#region copyright
// -----------------------------------------------------------------------
//  <copyright file="SqlSnapshotStore.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

namespace Eventuate.Sql
{
    public interface IEventConverter<TEvent>
    {
        DurableEvent ToEvent(TEvent row);

        TEvent FromEvent(DurableEvent durableEvent);
    }

    public interface ISnapshotConverter<TSnapshot>
    {
        Snapshot ToSnapshot(TSnapshot row);
        
        TSnapshot FromSnapshot(Snapshot snapshot);
        
        /// <summary>
        /// A SQL select statement used to retrieve snapshot data. It must contain an @emitterId parameter.
        /// </summary>
        string SelectStatement { get; }

        /// <summary>
        /// A SQL select statement used to delete snapshot data lower than provided @sequenceNr parameter.
        /// </summary>
        string DeleteToStatement { get; }
        
        /// <summary>
        /// A SQL select statement used to insert snapshot. It must have parameters for all of the <typeparamref name="TSnapshot"/> fields.
        /// </summary>
        string InsertStatement { get; }
    }
}