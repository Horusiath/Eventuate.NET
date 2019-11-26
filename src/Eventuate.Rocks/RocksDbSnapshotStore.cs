#region copyright
// -----------------------------------------------------------------------
//  <copyright file="RocksDbSnapshotStore.cs">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System.Threading.Tasks;
using Eventuate.Snapshots;

namespace Eventuate.Rocks
{
    public class RocksDbSnapshotStore : ISnapshotStore
    {
        public Task Delete(long lowerSequenceNr)
        {
            throw new System.NotImplementedException();
        }

        public Task Save(Snapshot snapshot)
        {
            throw new System.NotImplementedException();
        }

        public Task<Snapshot> Load(string emitterId)
        {
            throw new System.NotImplementedException();
        }
    }
}