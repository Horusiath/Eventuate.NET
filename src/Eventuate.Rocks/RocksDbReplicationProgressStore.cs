#region copyright
// -----------------------------------------------------------------------
//  <copyright file="RocksDbReplicationProgressStore.cs">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Collections.Immutable;
using Akka.Streams.Util;
using RocksDbSharp;

namespace Eventuate.Rocks
{
    internal sealed class RocksDbReplicationProgressStore
    {
        private const int RpEndKey = int.MaxValue;
        
        private readonly RocksDb db;
        private readonly int classifier;
        private readonly Func<string, int> numericId;
        private readonly Func<int, Option<string>> findId;

        private readonly byte[] rpEndKeyBytes;

        public RocksDbReplicationProgressStore(RocksDb db, int classifier, Func<string, int> numericId, Func<int, Option<string>> findId)
        {
            this.db = db;
            this.classifier = classifier;
            this.numericId = numericId;
            this.findId = findId;
            
            this.rpEndKeyBytes = RpKeyBytes(classifier, RpEndKey);
            db.Put(rpEndKeyBytes, Array.Empty<byte>());
        }

        public void WriteReplicationProgress(string logId, long logSeqNr, WriteBatch batch)
        {
            var nid = numericId(logId);
            var buffer = new byte[8];
            BinaryPrimitives.WriteInt64BigEndian(buffer, logSeqNr);
            batch.Put(RpKeyBytes(classifier, nid), buffer);
        }

        public long ReadReplicationProgress(string logId)
        {
            var nid = numericId(logId);
            var progress = db.Get(RpKeyBytes(classifier, nid));
            if (progress is null)
                return 0L;
            else
                return BinaryPrimitives.ReadInt64BigEndian(progress);
        }

        public ImmutableDictionary<string, long> ReadReplicationProgresses(Iterator iter)
        {
            iter.Seek(RpKeyBytes(classifier, 0));
            var builder = ImmutableDictionary.CreateBuilder<string, long>();
            while (true)
            {
                iter.Next();
                var key = RpKey(iter.Key());
                if (key == RpEndKey)
                    break;

                var value = BinaryPrimitives.ReadInt64BigEndian(iter.Value());
                var id = findId(key);
                if (id.HasValue)
                    builder[id.Value] = value;
            }

            return builder.ToImmutable();
        }
        
        private static byte[] RpKeyBytes(int classifier, int key)
        {
            var buffer = new byte[8];
            BinaryPrimitives.WriteInt32BigEndian(new Span<byte>(buffer, 0, 4), classifier);
            BinaryPrimitives.WriteInt32BigEndian(new Span<byte>(buffer, 4, 4), key);
            return buffer;
        }
        
        private int RpKey(byte[] key) => BinaryPrimitives.ReadInt32BigEndian(new Span<byte>(key, 4, 4));
    }
}