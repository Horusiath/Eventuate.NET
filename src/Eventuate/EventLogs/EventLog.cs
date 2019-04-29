using Akka.Actor;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Text;
using System.Threading.Tasks;

namespace Eventuate.EventLogs
{
    public abstract class EventLog<TSettings, TState> : ActorBase, IEventLog<TSettings, TState>, IWithUnboundedStash
        where TSettings : IEventLogSettings
        where TState : IEventLogState
    {
        protected EventLog()
        {
        }

        public IStash Stash { get; set; }

        public TSettings Settings { get; }

        public Task<long> Delete(long toSequenceNr)
        {
            throw new NotImplementedException();
        }

        public Task<BatchReadResult> Read(long fromSequenceNr, long toSequenceNr, int max)
        {
            throw new NotImplementedException();
        }

        public Task<BatchReadResult> Read(long fromSequenceNr, long toSequenceNr, int max, string aggregateId)
        {
            throw new NotImplementedException();
        }

        public Task<long> ReadReplicationProgress(string logId)
        {
            throw new NotImplementedException();
        }

        public Task<ImmutableDictionary<string, long>> ReadReplicationProgresses()
        {
            throw new NotImplementedException();
        }

        public Task<TState> RecoverState()
        {
            throw new NotImplementedException();
        }

        public void RecoverStateFailure(Exception cause)
        {
            throw new NotImplementedException();
        }

        public void RecoverStateSuccess(TState state)
        {
            throw new NotImplementedException();
        }

        public Task<BatchReadResult> ReplicationRead(long fromSequenceNr, long toSequenceNr, int max, int scanLimit, Func<DurableEvent, bool> filter)
        {
            throw new NotImplementedException();
        }

        public Task Write(IEnumerable<DurableEvent> events, long partition, EventLogClock clock)
        {
            throw new NotImplementedException();
        }

        public Task WriteDeletionMetadata(DeletionMetadata metadata)
        {
            throw new NotImplementedException();
        }

        public Task WriteEventLogClockSnapshot(EventLogClock clock)
        {
            throw new NotImplementedException();
        }

        public Task WriteReplicationProgresses(ImmutableDictionary<string, long> progresses)
        {
            throw new NotImplementedException();
        }
    }
}
