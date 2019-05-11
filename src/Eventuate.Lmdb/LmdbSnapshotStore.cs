#region copyright
// -----------------------------------------------------------------------
//  <copyright file="LmdbSnapshotStore.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System.Buffers;
using System.Text;
using System.Threading.Tasks;
using Akka.Serialization;
using Eventuate.EventLogs;
using Eventuate.Snapshots;
using Spreads.Buffers;
using Spreads.LMDB;

namespace Eventuate.Lmdb
{
    public sealed class LmdbSnapshotStore : ISnapshotStore
    {
        private const int MaxBufferSize = 4096 * 2;
        private readonly LMDBEnvironment env;
        private readonly Database db;
        private readonly Serializer snapshotSerializer;

        public LmdbSnapshotStore(Serialization serialization, LMDBEnvironment env, Database db)
        {
            this.env = env;
            this.db = db;
            this.snapshotSerializer = serialization.FindSerializerForType(typeof(Snapshot));
        }

        public async Task Delete(long lowerSequenceNr)
        {
            throw new PhysicalDeletionNotSupportedException();
        }

        public async Task Save(Snapshot snapshot)
        {
            await env.WriteAsync(txn =>
            {
                var key = new DirectBuffer(Encoding.UTF8.GetBytes(snapshot.Metadata.EmitterId));
                var data = new DirectBuffer(snapshotSerializer.ToBinary(snapshot));
                using (var cursor = db.OpenCursor(txn))
                {
                    cursor.Put(ref key, ref data, CursorPutOptions.Current);
                }
            });
        }

        public async Task<Snapshot> Load(string emitterId)
        {
            return env.Read(txn =>
            {
                using (var owner = MemoryPool<byte>.Shared.Rent(MaxBufferSize))
                {
                    var key = new DirectBuffer(Encoding.UTF8.GetBytes(emitterId));
                    var data = new DirectBuffer(owner.Memory.Span);
                    using (var cursor = db.OpenReadOnlyCursor(txn))
                    {
                        if (cursor.TryGet(ref key, ref data, CursorGetOption.Last))
                        {
                            return (Snapshot)snapshotSerializer.FromBinary(owner.Memory.ToArray(), typeof(Snapshot));
                        }
                        else return default(Snapshot);
                    }
                }
            });
        }
    }
}