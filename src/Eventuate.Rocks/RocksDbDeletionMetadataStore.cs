#region copyright
// -----------------------------------------------------------------------
//  <copyright file="RocksDbDeletionMetadataStore.cs">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Buffers.Binary;
using System.Buffers.Text;
using System.Collections.Immutable;
using System.Text;
using Eventuate.EventLogs;
using RocksDbSharp;

namespace Eventuate.Rocks
{
    internal sealed class RocksDbDeletionMetadataStore
    {
        private const int DeletedToSequenceNrKey = 1;
        private const int RemoteLogIdsKey = 2;
        private const char StringSetSeparatorChar = '\u0000';
        
        private readonly RocksDb db;
        private readonly WriteOptions writeOptions;
        private readonly int classifier;

        public RocksDbDeletionMetadataStore(RocksDb db, WriteOptions writeOptions, int classifier)
        {
            this.db = db;
            this.writeOptions = writeOptions;
            this.classifier = classifier;
        }

        public void WriteDeletionMetadata(DeletionMetadata info)
        {
            using (var batch = new WriteBatch())
            {
                batch.Put(IdKeyBytes(DeletedToSequenceNrKey), LongBytes(info.ToSequenceNr));
                batch.Put(IdKeyBytes(RemoteLogIdsKey), StringSetBytes(info.RemoteLogIds));
                
                db.Write(batch, writeOptions);
            }
        }

        private static byte[] StringSetBytes(ImmutableHashSet<string> logIds)
        {
            var str = String.Join(StringSetSeparatorChar.ToString(), logIds);
            return Encoding.UTF8.GetBytes(str);
        }

        private static byte[] LongBytes(long l)
        {
            var bytes = new byte[8];
            BinaryPrimitives.WriteInt64BigEndian(bytes, l);
            return bytes;
        }

        private byte[] IdKeyBytes(int key)
        {
            var bytes = new byte[8];
            BinaryPrimitives.WriteInt32BigEndian(new Span<byte>(bytes, 0, 4), classifier);
            BinaryPrimitives.WriteInt32BigEndian(new Span<byte>(bytes, 4, 4), key);
            return bytes;
        }

        public DeletionMetadata ReadDeletionMetadata()
        {
            var toSequenceNr = LongFromBytes(db.Get(IdKeyBytes(DeletedToSequenceNrKey)));
            var remoteLogIds = StringSetFromBytes(db.Get(IdKeyBytes(RemoteLogIdsKey)));
            return new DeletionMetadata(toSequenceNr, remoteLogIds);
        }

        private static ImmutableHashSet<string> StringSetFromBytes(byte[] buffer)
        {
            if (buffer is null || buffer.Length == 0) return ImmutableHashSet<string>.Empty;
            else
            {
                var str = Encoding.UTF8.GetString(buffer);
                return str.Split(StringSetSeparatorChar).ToImmutableHashSet();
            }
        }

        private long LongFromBytes(byte[] buffer) => buffer is null ? 0L : BinaryPrimitives.ReadInt64BigEndian(buffer);
    }
}