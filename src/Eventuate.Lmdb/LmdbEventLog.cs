#region copyright
// -----------------------------------------------------------------------
//  <copyright file="LmdbEventLog.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Serialization;
using Eventuate.EventLogs;
using Eventuate.Snapshots;
using Spreads.Buffers;
using Spreads.LMDB;
using Config = Akka.Configuration.Config;

namespace Eventuate.Lmdb
{
    public sealed class LmdbEventLogSettings : IEventLogSettings
    {
        public LmdbEventLogSettings(Config config)
        {
            this.ReadTimeout = config.GetTimeSpan("eventuate.log.read-timeout");

            var lmdb = config.GetConfig("eventuate.log.lmdb");
            this.RootDir = lmdb.GetString("dir");
            this.DbName = lmdb.GetString("db-name");
            this.StateSnapshotLimit = lmdb.GetInt("state-snapshot-limit");
            this.DeletionBatchSize = lmdb.GetInt("deletion-batch-size");
            this.DeletionRetryDelay = lmdb.GetTimeSpan("deletion-retry-delay");
        }

        public TimeSpan ReadTimeout { get; }
        public string RootDir { get; }
        public int StateSnapshotLimit { get; }
        public int DeletionBatchSize { get; }
        public TimeSpan DeletionRetryDelay { get; }
        public long PartitionSize { get; } = long.MaxValue;
        public int InitRetryMax { get; } = 0;
        public TimeSpan InitRetryDelay { get; } = TimeSpan.Zero;
        public string DbName { get;  }
    }

    public sealed class LmdbEventLogState : IEventLogState
    {
        public LmdbEventLogState(EventLogClock eventLogClock, DeletionMetadata deletionMetadata)
        {
            EventLogClock = eventLogClock;
            DeletionMetadata = deletionMetadata;
        }

        public EventLogClock EventLogClock { get; }
        public DeletionMetadata DeletionMetadata { get; }
    }
    
    public sealed class LmdbEventLog : EventLog<LmdbEventLogSettings, LmdbEventLogState>
    {
        public static Props Props(string logId, string prefix = "log", bool batching = true, ISnapshotStore snapshotStore = null)
        {
            var logProps = Akka.Actor.Props.Create(() => new LmdbEventLog(logId, prefix, snapshotStore)).WithDispatcher("eventuate.log.dispatchers.write-dispatcher");
            return batching ? Akka.Actor.Props.Create(() => new BatchingLayer(logProps)) : logProps;
        }

        private readonly string id;
        private readonly string prefix;
        private readonly LMDBEnvironment env;
        private readonly Database db;
        private readonly Serialization serialization;
        
        private readonly byte[] replicationProgressesKey;
        private readonly byte[] deletionMetadataKey;
        private readonly byte[] clockKey;

        private readonly Serializer clockSerializer;
        private readonly Serializer eventSerializer;
        private readonly Serializer deletionMetadataSerializer;
        private readonly Serializer replicationProgressSerializer;
        

        public LmdbEventLog(string id, string prefix, ISnapshotStore snapshotStore = null) : base(id)
        {
            this.id = id;
            this.prefix = prefix;
            this.serialization = Context.System.Serialization;
            this.Settings = new LmdbEventLogSettings(Context.System.Settings.Config);
            this.env = LMDBEnvironment.Create(this.Settings.RootDir);
            this.db = this.env.OpenDatabase(this.Settings.DbName, new DatabaseConfig(DbFlags.Create));
            this.SnapshotStore = snapshotStore ?? new LmdbSnapshotStore(this.serialization, this.env, this.db);

            this.replicationProgressesKey = Encoding.UTF8.GetBytes(this.Id + "/progresses");
            this.deletionMetadataKey = Encoding.UTF8.GetBytes(this.Id + "/deletion");
            this.clockKey = Encoding.UTF8.GetBytes(this.Id + "/clock");

            this.clockSerializer = serialization.FindSerializerForType(typeof(EventLogClock));
            this.eventSerializer = serialization.FindSerializerForType(typeof(DurableEvent));
            this.deletionMetadataSerializer = serialization.FindSerializerForType(typeof(DeletionMetadata));
            this.replicationProgressSerializer = serialization.FindSerializerForType(typeof(ImmutableDictionary<string, long>));
        }

        protected override ISnapshotStore SnapshotStore { get; }

        public override Task<BatchReadResult> Read(long fromSequenceNr, long toSequenceNr, int max)
        {
            return ReadInternal(fromSequenceNr, toSequenceNr, max, max, e => true);
        }

        public override Task<BatchReadResult> Read(long fromSequenceNr, long toSequenceNr, int max, string aggregateId)
        {
            return ReadInternal(fromSequenceNr, toSequenceNr, max, max, e => e.EmitterAggregateId == aggregateId);
        }

        public override Task<BatchReadResult> ReplicationRead(long fromSequenceNr, long toSequenceNr, int max, int scanLimit, Func<DurableEvent, bool> filter)
        {
            return ReadInternal(fromSequenceNr, toSequenceNr, max, scanLimit, filter);
        }
        
        private Task<BatchReadResult> ReadInternal(long fromSequenceNr, long toSequenceNr, int max, int scanLimit, Func<DurableEvent, bool> predicate)
        {
            var txn = this.env.BeginReadOnlyTransaction();
            try
            {
                long current = fromSequenceNr;
                var events = new List<DurableEvent>(max);
                var count = max;
                Span<byte> keySpan = stackalloc byte[sizeof(long)];
                for (int i = 0; i < scanLimit && current <= toSequenceNr; i++)
                {
                    BinaryPrimitives.WriteInt64BigEndian(keySpan, current);
                    var key = new DirectBuffer(keySpan);

                    if (this.db.TryGet(txn, ref key, out DirectBuffer value))
                    {
                        var bytes = new byte[value.Length];
                        Marshal.Copy(value.IntPtr, bytes, 0, value.Length);
                        var e = (DurableEvent)this.eventSerializer.FromBinary(bytes, typeof(DurableEvent));

                        if (predicate(e))
                        {
                            events.Add(e);
                            if (++count == max) 
                                break;
                        }
                        current++;
                    }
                    else
                    {
                        break;
                    }
                }

                var result = new BatchReadResult(events, current);
                return Task.FromResult(result);
            }
            catch (Exception cause)
            {
                return Task.FromException<BatchReadResult>(cause);
            }
            finally
            {
                txn.Dispose();
            }
        }
        
        public override Task WriteReplicationProgresses(ImmutableDictionary<string, long> progresses)
        {
            var key = new DirectBuffer(this.replicationProgressesKey);
            return WriteInternal(ref key, progresses, this.replicationProgressSerializer);
        }

        public override Task Write(IEnumerable<DurableEvent> events, long partition, EventLogClock clock)
        {
            var txn = this.env.BeginTransaction();
            try
            {
                foreach (var e in events)
                {
                    Span<byte> keySpan = stackalloc byte[sizeof(long)];
                    BinaryPrimitives.WriteInt64BigEndian(keySpan, e.LocalSequenceNr);
                    var key = new DirectBuffer(keySpan);
                    var bytes = this.eventSerializer.ToBinary(e);
                    var payload = new DirectBuffer(bytes);

                    this.db.Put(txn, ref key, ref payload);    
                }

                var ckey = new DirectBuffer(this.clockKey);
                var binary = this.clockSerializer.ToBinary(clock);
                var clockValue = new DirectBuffer(binary);
                this.db.Put(txn, ref ckey, ref clockValue);
                
                txn.Commit();
                
                return Task.CompletedTask;
            }
            catch (Exception cause)
            {
                txn.Abort();
                return Task.FromException(cause);
            }
            finally
            {
                txn.Dispose();
            }
        }

        public override Task WriteDeletionMetadata(DeletionMetadata metadata)
        {
            var key = new DirectBuffer(this.deletionMetadataKey);
            return WriteInternal(ref key, metadata, this.deletionMetadataSerializer);
        }

        public override Task WriteEventLogClockSnapshot(EventLogClock clock)
        {
            var key = new DirectBuffer(this.clockKey);
            return WriteInternal(ref key, clock, this.clockSerializer);
        }

        private Task WriteInternal<T>(ref DirectBuffer key, T value, Serializer serializer)
        {
            var txn = this.env.BeginTransaction();
            try
            {
                var binary = serializer.ToBinary(value);
                var payload = new DirectBuffer(binary);

                this.db.Put(txn, ref key, ref payload);
                
                txn.Commit();
                
                return Task.CompletedTask;
            }
            catch (Exception cause)
            {
                txn.Abort();
                return Task.FromException(cause);
            }
            finally
            {
                txn.Dispose();
            }
        }

        public override async Task<long> ReadReplicationProgress(string logId)
        {
            var progresses = await ReadReplicationProgresses();
            return progresses.GetValueOrDefault(logId, 0L);
        }

        public override Task<ImmutableDictionary<string, long>> ReadReplicationProgresses()
        {
            var txn = this.env.BeginReadOnlyTransaction();
            try
            {
                var key = new DirectBuffer(this.replicationProgressesKey);
                if (this.db.TryGet(txn, ref key, out DirectBuffer value))
                {
                    var bytes = new byte[value.Length];
                    Marshal.Copy(value.IntPtr, bytes, 0, value.Length);
                    var progresses = (ImmutableDictionary<string, long>)this.replicationProgressSerializer.FromBinary(bytes, typeof(ImmutableDictionary<string, long>));
                    return Task.FromResult(progresses);
                }
                else return Task.FromResult(ImmutableDictionary<string, long>.Empty);
            }
            catch (Exception cause)
            {
                return Task.FromException<ImmutableDictionary<string, long>>(cause);
            }
            finally
            {
                txn.Dispose();
            }
        }

        public override Task<LmdbEventLogState> RecoverState()
        {
            var txn = this.env.BeginReadOnlyTransaction();
            try
            {
                var ckey = new DirectBuffer(this.clockKey);
                EventLogClock clock;
                if (this.db.TryGet(txn, ref ckey, out var value))
                {
                    var bytes = new byte[value.Length];
                    Marshal.Copy(value.IntPtr, bytes, 0, value.Length);
                    clock = (EventLogClock)this.clockSerializer.FromBinary(bytes, typeof(EventLogClock));
                }
                else
                {
                    clock = new EventLogClock();
                }
                
                var dmkey = new DirectBuffer(this.deletionMetadataKey);
                DeletionMetadata metadata;
                if (this.db.TryGet(txn, ref dmkey, out value))
                {
                    var bytes = new byte[value.Length];
                    Marshal.Copy(value.IntPtr, bytes, 0, value.Length);
                    metadata = (DeletionMetadata)this.deletionMetadataSerializer.FromBinary(bytes, typeof(DeletionMetadata));
                }
                else
                {
                    metadata = new DeletionMetadata();
                }

                return Task.FromResult(new LmdbEventLogState(clock, metadata));
            }
            catch (Exception cause)
            {
                return Task.FromException<LmdbEventLogState>(cause);
            }
            finally
            {
                txn.Dispose();
            }
        }
    }
}