#region copyright
// -----------------------------------------------------------------------
//  <copyright file="RocksDbNumericIdentifierStore.cs">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Text;
using Akka.Streams.Util;
using RocksDbSharp;

namespace Eventuate.Rocks
{
    internal sealed class RocksDbNumericIdentifierStore
    {
        private const int IdEndKey = int.MaxValue;
        
        private readonly byte[] idKeyEndBytes;
        private readonly RocksDb db;
        private readonly int classifier;

        private Dictionary<string, int> idMap;
        
        public RocksDbNumericIdentifierStore(RocksDb db, int classifier)
        {
            this.idKeyEndBytes = IdKeyBytes(classifier, IdEndKey);
            this.db = db;
            this.classifier = classifier;
            this.idMap = new Dictionary<string, int>();
            
            db.Put(idKeyEndBytes, Array.Empty<byte>());
        }

        public int NumericId(string id)
        {
            if (idMap.TryGetValue(id, out var numId)) return numId;
            else return WriteIdMapping(id, this.idMap.Count + 1);
        }

        public Option<string> FindId(int id)
        {
            foreach (var entry in idMap)
            {
                if (entry.Value == id) return new Option<string>(entry.Key);
            }
            
            return Option<string>.None;
        }

        public void ReadIdMap(Iterator iter)
        {
            iter.Seek(IdKeyBytes(classifier, 0));
            while (true)
            {
                iter.Next();
                var intKey = IdKey(iter.Key());
                if (intKey == IdEndKey) 
                    break;

                var stringKey = Encoding.UTF8.GetString(iter.Value());
                this.idMap[stringKey] = intKey;
            }
        }

        private int WriteIdMapping(string stringKey, int intKey)
        {
            db.Put(IdKeyBytes(classifier, intKey), Encoding.UTF8.GetBytes(stringKey));
            idMap[stringKey] = intKey;
            return intKey;
        }

        private static byte[] IdKeyBytes(int classifier, int key)
        {
            var buffer = new byte[8];
            BinaryPrimitives.WriteInt32BigEndian(new Span<byte>(buffer, 0, 4), classifier);
            BinaryPrimitives.WriteInt32BigEndian(new Span<byte>(buffer, 4, 4), key);
            return buffer;
        }

        private static int IdKey(byte[] buffer) => BinaryPrimitives.ReadInt32BigEndian(new Span<byte>(buffer, 4, 4));
    }
}