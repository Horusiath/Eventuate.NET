﻿#region copyright
// -----------------------------------------------------------------------
//  <copyright file="ISnapshotStore.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Eventuate.Snapshots
{
    /// <summary>
    /// Snapshot store provider interface.
    /// </summary>
    public interface ISnapshotStore
    {
        /// <summary>
        /// Asynchronously deletes all snapshots with a sequence number greater than or equal <paramref name="lowerSequenceNr"/>.
        /// </summary>
        Task Delete(long lowerSequenceNr);

        /// <summary>
        /// Asynchronously saves the given <paramref name="snapshot"/>.
        /// </summary>
        Task Save(Snapshot snapshot);

        /// <summary>
        /// Asynchronously loads the latest snapshot saved by an event-sourced actor, view, writer or processor 
        /// identified by <paramref name="emitterId"/>.
        /// </summary>
        Task<Snapshot> Load(string emitterId);
    }
}
