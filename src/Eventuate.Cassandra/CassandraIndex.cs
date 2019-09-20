#region copyright
// -----------------------------------------------------------------------
//  <copyright file="CassandraIndex.cs">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Event;
using Eventuate.EventLogs;

namespace Eventuate.Cassandra
{
    public sealed class CassandraIndex : ActorBase
    {
        #region internal classes

        public sealed class UpdateIndex
        {
            public EventLogClock Clock { get; }
            public long ToSequenceNr { get; }
            public TaskCompletionSource<UpdateIndexSuccess> Promise { get; }

            public UpdateIndex(EventLogClock clock, long toSequenceNr, TaskCompletionSource<UpdateIndexSuccess> promise)
            {
                Clock = clock;
                ToSequenceNr = toSequenceNr;
                Promise = promise;
            }
        }
        
        public readonly struct UpdateIndexProgress
        {
            public IndexIncrement Increment { get; }

            public UpdateIndexProgress(IndexIncrement increment)
            {
                Increment = increment;
            }
        }
        
        public sealed class UpdateIndexSuccess
        {
            public EventLogClock Clock { get; }
            public int Steps { get; }

            public UpdateIndexSuccess(EventLogClock clock, int steps = 0)
            {
                Clock = clock;
                Steps = steps;
            }
        }
        
        public sealed class UpdateIndexFailure
        {
            public Exception Cause { get; }

            public UpdateIndexFailure(Exception cause)
            {
                Cause = cause;
            }
        }
        
        public readonly struct AggregateEvents
        {
            public ImmutableDictionary<string, ImmutableArray<DurableEvent>> Events { get; }

            public AggregateEvents(ImmutableDictionary<string, ImmutableArray<DurableEvent>> events)
            {
                Events = events ?? ImmutableDictionary<string, ImmutableArray<DurableEvent>>.Empty;
            }

            public AggregateEvents Update(DurableEvent e)
            {
                if (e.DestinationAggregateIds.IsEmpty) return this;
                else
                {
                    var builder = this.Events.ToBuilder();
                    foreach (var aggregateId in e.DestinationAggregateIds)
                    {
                        if (!builder.TryGetValue(aggregateId, out var events))
                            builder[aggregateId] = ImmutableArray.Create(e);
                        else
                            builder[aggregateId] = events.Add(e);
                    }
                    return new AggregateEvents(builder.ToImmutable());
                }
            }
        }
        
        public readonly struct IndexIncrement
        {
            public AggregateEvents AggregateEvents { get; }
            public EventLogClock Clock { get; }

            public IndexIncrement(AggregateEvents aggregateEvents, EventLogClock clock)
            {
                AggregateEvents = aggregateEvents;
                Clock = clock;
            }

            public IndexIncrement Update(IEnumerable<DurableEvent> events)
            {
                var index = this;
                foreach (var durableEvent in events)
                {
                    index = index.Update(durableEvent);
                }

                return index;
            }
            
            public IndexIncrement Update(DurableEvent e) => new IndexIncrement(AggregateEvents.Update(e), Clock.Update(e));

            public IndexIncrement Clear() => new IndexIncrement(new AggregateEvents(ImmutableDictionary<string, ImmutableArray<DurableEvent>>.Empty), Clock);
        }

        #endregion

        /// <summary>
        /// Contains the sequence number of the last event in event log that
        /// has been successfully processed and written to the index.
        /// </summary>
        private EventLogClock clock;
        
        private readonly IActorRef indexUpdater;
        private readonly Cassandra cassandra;
        private readonly CassandraEventLogStore logStore;
        private readonly CassandraIndexStore indexStore;
        private readonly string logId;
        
        public CassandraIndex(Cassandra cassandra, EventLogClock clock, CassandraEventLogStore logStore, CassandraIndexStore indexStore, string logId)
        {
            this.indexUpdater = Context.ActorOf(Akka.Actor.Props.Create(() => new CassandraIndexUpdater(cassandra, logStore, indexStore)));
            this.cassandra = cassandra;
            this.clock = clock;
            this.logStore = logStore;
            this.indexStore = indexStore;
            this.logId = logId;
        }

        protected override bool Receive(object message)
        {
            switch (message)
            {
                case UpdateIndex update:
                    this.indexUpdater.Tell(new UpdateIndex(this.clock, update.ToSequenceNr, update.Promise));
                    update.Promise.Task.PipeTo(Self);
                    return true;
                case UpdateIndexSuccess success:
                    this.clock = success.Clock;
                    return true;
                default: return false;
            }
        }

        public static Akka.Actor.Props Props(Cassandra cassandra, EventLogClock clock, CassandraEventLogStore logStore, CassandraIndexStore indexStore, string logId) =>
            Akka.Actor.Props.Create(() => new CassandraIndex(cassandra, clock, logStore, indexStore, logId));
    }

    internal sealed class CassandraIndexUpdater : ActorBase
    {
        private readonly Cassandra cassandra;
        private readonly CassandraEventLogStore logStore;
        private readonly CassandraIndexStore indexStore;
        private readonly ILoggingAdapter logger;

        public CassandraIndexUpdater(Cassandra cassandra, CassandraEventLogStore logStore, CassandraIndexStore indexStore)
        {
            this.cassandra = cassandra;
            this.logStore = logStore;
            this.indexStore = indexStore;
            this.logger = Context.GetLogger();
        }

        protected override bool Receive(object message) => Idle(message);

        private bool Idle(object message)
        {
            switch (message)
            {
                case CassandraIndex.UpdateIndex u:
                    Update(u.Clock.SequenceNr + 1, u.ToSequenceNr, new CassandraIndex.IndexIncrement(new CassandraIndex.AggregateEvents(ImmutableDictionary<string, ImmutableArray<DurableEvent>>.Empty), u.Clock));
                    Context.Become(Updating(0, u.ToSequenceNr, u.Promise));
                    return true;
                default: return false;
            }
        }

        private Receive Updating(int steps, long toSequenceNr, TaskCompletionSource<CassandraIndex.UpdateIndexSuccess> promise) => message =>
        {
            switch (message)
            {
                case CassandraIndex.UpdateIndexSuccess success:
                    promise.TrySetResult(new CassandraIndex.UpdateIndexSuccess(success.Clock, steps));
                    Context.Become(Idle);
                    return true;
                case CassandraIndex.UpdateIndexProgress progress:
                    var inc = progress.Increment;
                    Update(inc.Clock.SequenceNr + 1, toSequenceNr, inc.Clear());
                    Context.Become(Updating(steps + 1, toSequenceNr, promise));
                    return true;
                case CassandraIndex.UpdateIndexFailure failure:
                    promise.TrySetException(failure.Cause);
                    logger.Error(failure.Cause, "UpdateIndex failure");
                    Context.Become(Idle);
                    return true;
                default: return false;
            }
        };

        private void Update(long fromSequenceNr, long toSequenceNr, CassandraIndex.IndexIncrement increment)
        {
            UpdateAsync(fromSequenceNr, toSequenceNr, increment)
                .PipeTo(Self,
                    success: t => t.Item2 
                        ? (object)new CassandraIndex.UpdateIndexProgress(t.Item1) 
                        : new CassandraIndex.UpdateIndexSuccess(t.Item1.Clock) ,
                    failure: err => new CassandraIndex.UpdateIndexFailure(err));
        }
        
        private async Task<(CassandraIndex.IndexIncrement, bool)> UpdateAsync(long fromSequenceNr, long toSequenceNr, CassandraIndex.IndexIncrement increment)
        {
            var limit = cassandra.Settings.IndexUpdateLimit;
            var res = await this.logStore.ReadAsync(fromSequenceNr, toSequenceNr, limit, limit + 1);
            var events = res.Events.ToArray();
            var inc = await WriteAsync(increment.Update(events));

            return (inc, events.Length > 0);
        }

        private async Task<CassandraIndex.IndexIncrement> WriteAsync(CassandraIndex.IndexIncrement increment)
        {
            await indexStore.Write(increment.AggregateEvents, increment.Clock);
            return increment;
        }
    }
}