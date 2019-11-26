#region copyright
// -----------------------------------------------------------------------
//  <copyright file="RocksDbEventLog.cs">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Buffers.Binary;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Eventuate.EventLogs;
using Eventuate.Snapshots;
using Eventuate.Snapshots.Filesystem;
using RocksDbSharp;

namespace Eventuate.Rocks
{
    internal static class Classifiers
    {
        public const int AggregateIdMap = -1;
        public const int EventLogIdMap = -2;
        public const int ReplicationProgressMap = -3;
        public const int DeletionMetadata = -4;
        public const int Snapshot = -5;
    }
    
    public sealed class RocksDbSettings : IEventLogSettings
    {
        public long PartitionSize => long.MaxValue;
        public int InitRetryMax => 1;
        public TimeSpan InitRetryDelay => TimeSpan.Zero;
        public string Dir { get; }
        public TimeSpan DeletionRetryDelay { get; }
        public TimeSpan ReadTimeout { get; }
        public bool ShouldSync { get; }
        public int StateSnapshotLimit { get; }
        public int DeletionBatchSize { get; }

        public RocksDbSettings(Config config)
            : this(
                dir: config.GetString("eventuate.log.leveldb.dir"),
                deletionRetryDelay: config.GetTimeSpan("eventuate.log.leveldb.deletion-retry-delay"),
                readTimeout: config.GetTimeSpan("eventuate.log.read-timeout"),
                shouldSync: config.GetBoolean("eventuate.log.leveldb.fsync"), 
                stateSnapshotLimit: config.GetInt("eventuate.log.leveldb.state-snapshot-limit"),
                deletionBatchSize: config.GetInt("eventuate.log.leveldb.deletion-batch-size"))
        {
        }

        public RocksDbSettings(string dir, TimeSpan deletionRetryDelay, TimeSpan readTimeout, bool shouldSync, int stateSnapshotLimit, int deletionBatchSize)
        {
            Dir = dir;
            DeletionRetryDelay = deletionRetryDelay;
            ReadTimeout = readTimeout;
            ShouldSync = shouldSync;
            StateSnapshotLimit = stateSnapshotLimit;
            DeletionBatchSize = deletionBatchSize;
        }
    }

    public readonly struct RocksDbLogState : IEventLogState
    {
        public EventLogClock EventLogClock { get; }
        public DeletionMetadata DeletionMetadata { get; }

        public RocksDbLogState(EventLogClock eventLogClock, DeletionMetadata deletionMetadata)
        {
            EventLogClock = eventLogClock;
            DeletionMetadata = deletionMetadata;
        }
    }
    
    /// <summary>
    ///An event log actor with LevelDB as storage backend. The directory containing the LevelDB files
    ///for this event log is named after the constructor parameters using the template "`prefix`-`id`"
    ///and stored in a root directory defined by the `log.leveldb.dir` configuration.
    /// 
    ///'''Please note:''' `prefix` and `id` are currently not escaped when creating the directory name.
    ///</summary>
    public sealed class RocksDbEventLog : EventLog<RocksDbSettings, RocksDbLogState>
    {
        #region internal classes

        internal readonly struct EventKey : IEquatable<EventKey>
        {
            public const int DefaultClassifier = 0;
            public static readonly EventKey End = new EventKey(int.MaxValue, long.MaxValue);
            internal static readonly byte[] EndBytes = ToBytes(End.Classifier, End.SequenceNr);
            internal static readonly byte[] ClockKeyBytes = ToBytes(0, 0);
            
            public int Classifier { get; }
            public long SequenceNr { get; }

            public EventKey(int classifier, long sequenceNr)
            {
                Classifier = classifier;
                SequenceNr = sequenceNr;
            }

            public override string ToString() => $"EventKey(classifier: {Classifier}, sequenceNr: {SequenceNr})";
            public bool Equals(EventKey other) => Classifier == other.Classifier && SequenceNr == other.SequenceNr;
            public override bool Equals(object obj) => obj is EventKey other && Equals(other);
            public override int GetHashCode()
            {
                unchecked
                {
                    return (Classifier * 397) ^ SequenceNr.GetHashCode();
                }
            }

            public static bool operator ==(EventKey a, EventKey b) => a.Equals(b);

            public static bool operator !=(EventKey a, EventKey b) => !(a == b);

            internal static byte[] ToBytes(int classifier, long sequenceNr)
            {
                var buffer = new byte[12];
                BinaryPrimitives.WriteInt32BigEndian(new Span<byte>(buffer, 0, 4), classifier);
                BinaryPrimitives.WriteInt64BigEndian(new Span<byte>(buffer, 4, 8), sequenceNr);
                return buffer;
            }

            internal static EventKey FromBytes(byte[] buffer)
            {
                var classifier = BinaryPrimitives.ReadInt32BigEndian(new Span<byte>(buffer, 0, 4));
                var sequenceNr = BinaryPrimitives.ReadInt64BigEndian(new Span<byte>(buffer, 4, 8));
                return new EventKey(classifier, sequenceNr);
            }
        }

        internal sealed class EventEnumerator : IEnumerator<DurableEvent>
        {
            private readonly int classifier;
            private readonly Func<byte[], DurableEvent> toEvent;
            private readonly long from;
            private readonly Iterator iter;

            public EventEnumerator(RocksDb db, long from, int classifier, Func<byte[], DurableEvent> toEvent)
            {
                this.classifier = classifier;
                this.toEvent = toEvent;
                this.from = from;
                this.iter = db.NewIterator();
                iter.Seek(RocksDbEventLog.EventKey.ToBytes(classifier, from));
            }

            public bool MoveNext()
            {
                var key = RocksDbEventLog.EventKey.FromBytes(iter.Next().Key());
                if (key.Classifier != classifier)
                    return false;
                else
                {
                    Current = toEvent(iter.Value());
                    return true;
                }
            }

            public void Reset() => throw new NotImplementedException();

            public DurableEvent Current { get; private set; }

            object IEnumerator.Current => Current;

            public void Dispose() => iter.Dispose();
        }
        
        #endregion

        public static Akka.Actor.Props Props(string logId, RocksDbSettings settings = null, string prefix = "log", bool batching = true)
        {
            var logProps = Akka.Actor.Props.Create(() => new RocksDbEventLog(logId, prefix, settings));
            return batching ? Akka.Actor.Props.Create(() => new BatchingLayer(logProps)) : logProps;
        }
        
        private readonly string prefix;
        private readonly Akka.Serialization.Serialization serialization;
        private readonly string dir;
        private readonly RocksDb db;
        private readonly ReadOptions readOptions;
        private readonly WriteOptions writeOptions;
        private readonly RocksDbNumericIdentifierStore aggregateIdMap;
        private readonly RocksDbNumericIdentifierStore eventLogIdMap;
        private readonly RocksDbReplicationProgressStore replicationProgressMap;
        private readonly RocksDbDeletionMetadataStore deletionMetadataStore;

        private long updateCount = 0L;
        private readonly byte[] clockKeyBytes = new byte[8];
        private readonly Func<byte[], DurableEvent> deserializeEvent;

        public RocksDbEventLog(string id, string prefix, RocksDbSettings settings) : base(id)
        {
            settings ??= new RocksDbSettings(Context.System.Settings.Config);
            this.prefix = prefix;
            this.SnapshotStore = new FilesystemSnapshotStore(Context.System, id);
            this.Settings = settings;
            
            this.serialization = Context.System.Serialization;
            this.dir = Path.Combine(settings.Dir, $"{prefix}-{id}");
            if (!Directory.Exists(dir))
            {
                Directory.CreateDirectory(dir);
            }

            this.db = RocksDb.Open(new DbOptions().SetCreateIfMissing(true), dir);
            this.writeOptions = new WriteOptions().SetSync(settings.ShouldSync);
            this.readOptions = new ReadOptions().SetVerifyChecksums(false);
            this.aggregateIdMap = new RocksDbNumericIdentifierStore(db, Classifiers.AggregateIdMap);
            this.eventLogIdMap = new RocksDbNumericIdentifierStore(db, Classifiers.EventLogIdMap);
            this.replicationProgressMap = new RocksDbReplicationProgressStore(db, Classifiers.ReplicationProgressMap, eventLogIdMap.NumericId, eventLogIdMap.FindId);
            this.deletionMetadataStore = new RocksDbDeletionMetadataStore(db, writeOptions, Classifiers.DeletionMetadata);
            
            var tDurableEvent = typeof(DurableEvent);
            var eventSerializerId = serialization.FindSerializerForType(tDurableEvent).Identifier;
            this.deserializeEvent = bytes => (DurableEvent) serialization.Deserialize(bytes, eventSerializerId, tDurableEvent);
        }

        protected override ISnapshotStore SnapshotStore { get; }
        public override async Task WriteReplicationProgresses(ImmutableDictionary<string, long> progresses)
        {
            using (var batch = new WriteBatch())
            {
                foreach (var entry in progresses)
                {
                    replicationProgressMap.WriteReplicationProgress(entry.Key, entry.Value, batch);
                }
                
                db.Write(batch);
            }
        }

        public override async Task<BatchReadResult> Read(long fromSequenceNr, long toSequenceNr, int max)
        {
            using var cancellation = new CancellationTokenSource(Settings.ReadTimeout);
            return await Task.Run(() => ReadSync(fromSequenceNr, toSequenceNr, EventKey.DefaultClassifier, max, int.MaxValue, _ => true), cancellation.Token);
        }

        public override async Task<BatchReadResult> Read(long fromSequenceNr, long toSequenceNr, int max, string aggregateId)
        {
            using var cancellation = new CancellationTokenSource(Settings.ReadTimeout);
            return await Task.Run(() => ReadSync(fromSequenceNr, toSequenceNr, aggregateIdMap.NumericId(aggregateId), max, int.MaxValue, _ => true), cancellation.Token);
        }

        public override async Task<BatchReadResult> ReplicationRead(long fromSequenceNr, long toSequenceNr, int max, int scanLimit, Func<DurableEvent, bool> filter)
        {
            using var cancellation = new CancellationTokenSource(Settings.ReadTimeout);
            return await Task.Run(() => ReadSync(fromSequenceNr, toSequenceNr, EventKey.DefaultClassifier, max, scanLimit, filter), cancellation.Token);
        }

        private BatchReadResult ReadSync(long fromSequenceNr, long toSequenceNr, int classifier, int max, int scanLimit,
            Func<DurableEvent, bool> filter)
        {
            var events = new List<DurableEvent>();
            var first = Math.Max(1L, fromSequenceNr);
            var last = first - 1L;
            var scanned = 0;
            var filtered = 0;

            using (var enumerator = new EventEnumerator(db, first, classifier, deserializeEvent))
            {
                while (enumerator.MoveNext() && filtered < max && scanned < scanLimit)
                {
                    var e = enumerator.Current;
                    if (filter(e))
                    {
                        events.Add(e);
                        filtered++;
                    }

                    scanned++;
                    last = e.LocalSequenceNr;
                }
            }
                
            return new BatchReadResult(events, last);
        }

        public override async Task Write(IReadOnlyCollection<DurableEvent> events, long partition, EventLogClock clock, IActorContext context)
        {
            using var batch = new WriteBatch();
            foreach (var e in events)
            {
                var sequenceNr = e.LocalSequenceNr;
                var eventBytes = serialization.Serialize(e);
                    
                batch.Put(EventKey.ToBytes(EventKey.DefaultClassifier, sequenceNr), eventBytes);
                foreach (var id in e.CustomDestinationAggregateIds)
                {
                    batch.Put(EventKey.ToBytes(aggregateIdMap.NumericId(id), sequenceNr), eventBytes);
                }
                    
                updateCount++;
            }

            if (updateCount >= Settings.StateSnapshotLimit)
            {
                WriteEventLogClockSnapshot(clock, batch);
                updateCount = 0;
            }

            db.Write(batch);
        }

        private void WriteEventLogClockSnapshot(EventLogClock clock, WriteBatch batch)
        {
            batch.Put(this.clockKeyBytes, serialization.Serialize(clock));
        }

        public override async Task WriteDeletionMetadata(DeletionMetadata metadata)
        {
            deletionMetadataStore.WriteDeletionMetadata(metadata);
        }

        public override async Task WriteEventLogClockSnapshot(EventLogClock clock, IActorContext context)
        {
            using (var batch = new WriteBatch())
            {
                WriteEventLogClockSnapshot(clock, batch);
                db.Write(batch);
            }
        }

        public override Task<long> Delete(long toSequenceNr)
        {
            var adjusted = Math.Min(ReadEventLogClockSnapshot().SequenceNr, toSequenceNr);
            var promise = new TaskCompletionSource<long>();
            Context.ActorOf(RocksDbDeletionActor.Props(db, readOptions, writeOptions, Settings.DeletionBatchSize, adjusted, promise));
            return promise.Task;
        }

        public override async Task<long> ReadReplicationProgress(string logId)
        {
            return replicationProgressMap.ReadReplicationProgress(logId);
        }

        public override async Task<ImmutableDictionary<string, long>> ReadReplicationProgresses()
        {
            using var iter = db.NewIterator();
            return replicationProgressMap.ReadReplicationProgresses(iter);
        }

        public override async Task<RocksDbLogState> RecoverState()
        {
            var clock = ReadEventLogClockSnapshot();
            using var enumerator = new EventEnumerator(db, clock.SequenceNr + 1L, EventKey.DefaultClassifier, deserializeEvent);
            while (enumerator.MoveNext())
            {
                clock = clock.Update(enumerator.Current);
            }

            return new RocksDbLogState(clock, deletionMetadataStore.ReadDeletionMetadata());
        }

        private EventLogClock ReadEventLogClockSnapshot()
        {
            var x = db.Get(clockKeyBytes);
            return (x is null)
                ? EventLogClock.Empty
                : (EventLogClock)serialization.FindSerializerFor(typeof(EventLogClock)).FromBinary(x, typeof(EventLogClock));
        }

        protected override void PreStart()
        {
            using (var iter = db.NewIterator()) aggregateIdMap.ReadIdMap(iter);
            using (var iter = db.NewIterator()) eventLogIdMap.ReadIdMap(iter);
            db.Put(EventKey.EndBytes, Array.Empty<byte>());
            
            base.PreStart();
        }

        protected override void PostStop()
        {
            // Leveldb iterators that are used by threads other that this actor's dispatcher threads
            // are used in child actors of this actor. This ensures that these iterators are closed
            // before this actor closes the leveldb instance (fixing issue #234).
            db.Dispose();
            base.PostStop();
        }
    }
}