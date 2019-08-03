#region copyright
// -----------------------------------------------------------------------
//  <copyright file="IEventLog.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Text;
using System.Threading.Tasks;

namespace Eventuate.EventLogs
{
    /// <summary>
    /// Event log settings to be implemented by storage providers.
    /// </summary>
    public interface IEventLogSettings
    {
        /// <summary>
        /// Maximum number of events to store per partition. If a storage provider does not support
        /// partitioned event storage it should return <see cref="long.MaxValue"/>, otherwise the appropriate
        /// partition size. Eventuate internally calculates the target partition for batch writes.
        /// If an event batch doesn't fit into the current partition, it will be written to the next
        /// partition, so that batch writes are always single-partition writes.
        /// </summary>
        long PartitionSize { get; }

        /// <summary>
        /// Maximum number of clock recovery retries.
        /// </summary>
        int InitRetryMax { get; }

        /// <summary>
        /// Delay between clock recovery retries.
        /// </summary>
        TimeSpan InitRetryDelay { get; }

        /// <summary>
        /// Delay between two tries to physically delete all requested events while keeping
        /// those that are not yet replicated.
        /// </summary>
        TimeSpan DeletionRetryDelay { get; }
    }

    /// <summary>
    /// <see cref="EventLog"/> actor state that must be recovered on log actor initialization. Implementations are storage provider-specific.
    /// </summary>
    public interface IEventLogState
    {
        /// <summary>
        /// Recovered event log clock.
        /// </summary>
        EventLogClock EventLogClock { get; }

        /// <summary>
        /// Recovered deletion metadata.
        /// </summary>
        DeletionMetadata DeletionMetadata { get; }
    }

    public readonly struct DeletionMetadata
    {
        public DeletionMetadata(long toSequenceNr, ImmutableHashSet<string> remoteLogIds)
        {
            ToSequenceNr = toSequenceNr;
            RemoteLogIds = remoteLogIds;
        }

        public long ToSequenceNr { get; }
        public ImmutableHashSet<string> RemoteLogIds { get; }
    }

    /// <summary>
    /// Result of an event batch-read operation.
    /// </summary>
    public readonly struct BatchReadResult
    {
        public BatchReadResult(IEnumerable<DurableEvent> events, long to)
        {
            Events = events;
            To = to;
        }

        /// <summary>
        /// Read event batch.
        /// </summary>
        public IEnumerable<DurableEvent> Events { get; }

        /// <summary>
        /// Last read position in the event log.
        /// </summary>
        public long To { get; }
    }

    /// <summary>
    /// Indicates that a storage backend doesn't support physical deletion of events.
    /// </summary>
    public class PhysicalDeletionNotSupportedException : NotSupportedException
    {
    }

    /// <summary>
    /// Event log storage provider interface (SPI).
    /// </summary>
    /// <typeparam name="TState">Event log state type.</typeparam>
    public interface IEventLog<TSettings, TState>
        where TSettings : IEventLogSettings
        where TState : IEventLogState
    {
        /// <summary>
        /// Event log settings.
        /// </summary>
        TSettings Settings { get; }

        /// <summary>
        /// Asynchronously recovers event log state from the storage backend.
        /// </summary>
        Task<TState> RecoverState();

        /// <summary>
        /// Called after successful event log state recovery.
        /// </summary>
        /// <param name="state">Event log state.</param>
        void RecoverStateSuccess(TState state);

        /// <summary>
        /// Called after failed event log state recovery.
        /// </summary>
        void RecoverStateFailure(Exception cause);

        /// <summary>
        /// Asynchronously reads all stored local replication progresses.
        /// </summary>
        /// <seealso cref="ReplicationProtocol.GetReplicationProgresses"/>
        Task<ImmutableDictionary<string, long>> ReadReplicationProgresses();

        /// <summary>
        /// Asynchronously reads the replication progress for given source <paramref name="logId"/>.
        /// </summary>
        /// <seealso cref="ReplicationProtocol.GetReplicationProgress"/>
        Task<long> ReadReplicationProgress(string logId);

        /// <summary>
        /// Asynchronously writes the replication progresses for source log ids given by <paramref name="progresses"/> keys.
        /// </summary>
        Task WriteReplicationProgresses(ImmutableDictionary<string, long> progresses);

        /// <summary>
        /// Asynchronously batch-reads events from the raw event log. At most <paramref name="max"/> events must be returned that are
        /// within the sequence number bounds <paramref name="fromSequenceNr"/> and <paramref name="toSequenceNr"/>.
        /// </summary>
        /// <param name="fromSequenceNr">Sequence number to start reading (inclusive).</param>
        /// <param name="toSequenceNr"> Sequence number to stop reading (inclusive) or earlier if <paramref name="max"/> events have already been read.</param>
        Task<BatchReadResult> Read(long fromSequenceNr, long toSequenceNr, int max);

        /// <summary>
        /// Asynchronously batch-reads events whose `destinationAggregateIds` contains the given <paramref name="aggregateId"/>. At most
        /// <paramref name="max"/> events must be returned that are within the sequence number bounds <paramref name="fromSequenceNr"/> and <paramref name="toSequenceNr"/>.
        /// </summary>
        /// <param name="fromSequenceNr">Sequence number to start reading (inclusive).</param>
        /// <param name="toSequenceNr"> Sequence number to stop reading (inclusive) or earlier if <paramref name="max"/> events have already been read.</param>
        Task<BatchReadResult> Read(long fromSequenceNr, long toSequenceNr, int max, string aggregateId);

        /// <summary>
        /// Asynchronously batch-reads events from the raw event log. At most <paramref name="max"/> events must be returned that are
        /// within the sequence number bounds <paramref name="fromSequenceNr"/> and <paramref name="toSequenceNr"/> and that pass the given <paramref name="filter"/>.
        /// </summary>
        /// <param name="fromSequenceNr">Sequence number to start reading (inclusive).</param>
        /// <param name="toSequenceNr"> Sequence number to stop reading (inclusive) or earlier if <paramref name="max"/> events have already been read.</param>
        Task<BatchReadResult> ReplicationRead(long fromSequenceNr, long toSequenceNr, int max, int scanLimit, Func<DurableEvent, bool> filter);

        /// <summary>
        /// Asynchronously writes <paramref name="events"/> to the given <paramref name="partition"/>. The partition is calculated from the configured
        /// <see cref="IEventLogSettings.PartitionSize"/> and the current sequence number. Asynchronous writes will be supported in future versions.
        /// 
        /// This method may only throw an exception if it can guarantee that <paramref name="events"/> have not been written to the storage
        /// backend. If this is not the case (e.g. after a timeout communicating with a remote storage backend) this method
        /// must retry writing or give up by stopping the actor with <c>Context.Stop(Self)</c>. This is necessary to avoid that
        /// <paramref name="events"/> are erroneously excluded from the event stream sent to event-sourced actors, views, writers and
        /// processors, as they may later re-appear during recovery which would violate ordering/causality guarantees.
        /// 
        /// Implementations that potentially retry a write for a longer time should use a <see cref="CircuitBreaker"/> for protecting
        /// themselves against request overload.
        /// </summary>
        /// <seealso cref="IEventLogSettings"/>
        Task Write(IReadOnlyCollection<DurableEvent> events, long partition, EventLogClock clock);

        /// <summary>
        /// Asynchronously writes metadata for a <see cref="EventsourcingProtocol.Delete"/> request. This marks events up to
        /// <see cref="DeletionMetadata.ToSequenceNr"/> as deleted, i.e. they are not read on replay and indicates which remote logs
        /// must have replicated these events before they are allowed to be physically deleted locally.
        /// </summary>
        Task WriteDeletionMetadata(DeletionMetadata metadata);
        
        /// <summary>
        /// Asynchronously writes the current snapshot of the event log clock.
        /// </summary>
        Task WriteEventLogClockSnapshot(EventLogClock clock);

        /// <summary>
        /// Instructs the log to asynchronously and physically delete events up to <paramref name="toSequenceNr"/>. This operation completes when
        /// physical deletion completed and returns the sequence nr up to which events have been deleted. This can be
        /// smaller then the requested <paramref name="toSequenceNr"/> if a backend has to keep events for internal reasons.
        /// A backend that does not support physical deletion should not override this method.
        /// </summary>
        Task<long> Delete(long toSequenceNr);
    }
}
