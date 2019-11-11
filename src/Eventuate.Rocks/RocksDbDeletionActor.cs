#region copyright
// -----------------------------------------------------------------------
//  <copyright file="RocksDbDeletionActor.cs">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;
using Akka.Actor;
using RocksDbSharp;
using static Eventuate.Rocks.RocksDbEventLog;

namespace Eventuate.Rocks
{
    internal sealed class RocksDbDeletionActor : ActorBase
    {
        private sealed class DeleteBatch
        {
            public static readonly DeleteBatch Instance = new DeleteBatch();
            private DeleteBatch() {}
        }
        
        private sealed class EventKeyEnumerator : IEnumerator<EventKey>
        {
            private readonly long toSequenceNr;
            private readonly Iterator iterator;

            public EventKeyEnumerator(RocksDb db, ReadOptions options, long toSequenceNr)
            {
                this.toSequenceNr = toSequenceNr;
                this.iterator = db.NewIterator(readOptions: options);
                iterator.Seek(EventKey.ToBytes(EventKey.DefaultClassifier, 1L));
            }

            public bool MoveNext()
            {
                while (true)
                {
                    var key = EventKey.FromBytes(iterator.Next().Key());
                    if (key != EventKey.End && key.SequenceNr <= toSequenceNr)
                    {
                        this.Current = key;
                        break;
                    }
                    else
                    {
                        iterator.Seek(EventKey.ToBytes(key.Classifier + 1, 1L));
                    }
                }
                
                return true;
            }

            public void Reset() => throw new System.NotImplementedException();

            public EventKey Current { get; private set; }

            object IEnumerator.Current => Current;

            public void Dispose() => iterator.Dispose();
        }
        
        public static Props Props(RocksDb db, ReadOptions readOptions, WriteOptions writeOptions, int batchSize, long toSequenceNr, TaskCompletionSource<long> promise) => 
            Akka.Actor.Props.Create(() => new RocksDbDeletionActor(db, readOptions, writeOptions, batchSize, toSequenceNr, promise));

        private readonly RocksDb db;
        private readonly ReadOptions readOptions;
        private readonly WriteOptions writeOptions;
        private readonly int batchSize;
        private readonly long toSequenceNr;
        private readonly TaskCompletionSource<long> promise;

        private readonly EventKeyEnumerator enumerator;

        public RocksDbDeletionActor(RocksDb db, ReadOptions readOptions, WriteOptions writeOptions, int batchSize, long toSequenceNr, TaskCompletionSource<long> promise)
        {
            this.db = db;
            this.readOptions = readOptions;
            this.writeOptions = writeOptions;
            this.batchSize = batchSize;
            this.toSequenceNr = toSequenceNr;
            this.promise = promise;
            
            this.enumerator = new EventKeyEnumerator(db, readOptions, toSequenceNr);
        }

        protected override void PreStart() => Self.Tell(DeleteBatch.Instance);

        protected override void PostStop()
        {
            enumerator.Dispose();
            base.PostStop();
        }

        protected override bool Receive(object message)
        {
            switch (message)
            {
                case DeleteBatch _:
                {
                    var hasNext = false;
                    using (var batch = new WriteBatch())
                    {
                        var i = batchSize;
                        while (i != 0 && (hasNext = enumerator.MoveNext()))
                        {
                            var key = enumerator.Current;
                            batch.Delete(EventKey.ToBytes(key.Classifier, key.SequenceNr));
                            i--;
                        }
                        
                        db.Write(batch, writeOptions);
                    }

                    if (hasNext)
                    {
                        Self.Tell(DeleteBatch.Instance);
                    }
                    else
                    {
                        promise.TrySetResult(toSequenceNr);
                        Self.Tell(PoisonPill.Instance);
                    }

                    return true;
                }
                default: return false;
            }
        }
    }
}