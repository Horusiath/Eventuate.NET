#region copyright
// -----------------------------------------------------------------------
//  <copyright file="DurableEventSource.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using Akka.Actor;
using Akka.Configuration;
using Akka.Event;
using Akka.Streams.Actors;
using Akka.Streams.Dsl;
using Eventuate.EventsourcingProtocol;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;

namespace Eventuate.Streams
{
    internal sealed class DurableEventSourceSettings
    {
        public DurableEventSourceSettings(Config config)
        {
            this.ReplayBatchSize = config.GetInt("eventuate.log.replay-batch-size");
            this.ReadTimeout = config.GetTimeSpan("eventuate.log.read-timeout");
            this.ReplayRetryDelay = config.GetTimeSpan("eventuate.log.replay-retry-delay");
        }

        public int ReplayBatchSize { get; }
        public TimeSpan ReadTimeout { get; }
        public TimeSpan ReplayRetryDelay { get; }
    }

    /// <summary>
    /// Stream-based alternative to <see cref="EventsourcedView"/>.
    /// </summary>
    public static class DurableEventSource
    {
        /// <summary>
        /// Creates an Akka.NET Streams source stage that emits the <see cref="DurableEvent"/>s
        /// stored in <paramref name="eventLog"/>. The source also emits events that have been written to the event log after
        /// source creation. Behavior of the source can be configured with:
        /// 
        ///  - `eventuate.log.replay-batch-size`. Maximum number of events to read from the event log when the internal
        ///    buffer (of same size) is empty.
        ///  - `eventuate.log.read-timeout`. Timeout for reading events from the event log.
        ///  - `eventuate.log.replay-retry-delay`. Delay between a failed read and the next read retry.
        /// </summary>
        /// <param name="eventLog">Source event log.</param>
        /// <param name="fromSequenceNr">A sequence number from where the [[DurableEvent]] stream should start.</param>
        /// <param name="aggregateId">
        /// if defined, emit only <see cref="DurableEvent"/>s with that aggregate id, otherwise, emit <see cref="DurableEvent"/>s 
        /// with any aggregate id (see also http://rbmhtechnology.github.io/eventuate/reference/event-sourcing.html#event-routing).
        /// </param>
        public static Source<DurableEvent, IActorRef> Create(IActorRef eventLog, long fromSequenceNr = 0L, string aggregateId = null) =>
            Source.ActorPublisher<DurableEvent>(DurableEventSourceActor.Props(eventLog, fromSequenceNr, aggregateId));
    }

    internal sealed class DurableEventSourceActor : ActorPublisher<DurableEvent>
    {
        public readonly struct Paused { }

        public static Props Props(IActorRef eventLog, long fromSequenceNr, string aggregateId) =>
            Akka.Actor.Props.Create(() => new DurableEventSourceActor(eventLog, fromSequenceNr, aggregateId)).WithDeploy(Deploy.Local);

        private readonly IActorRef eventLog;
        private readonly long fromSequenceNr;
        private readonly string aggregateId;
        private readonly DurableEventSourceSettings settings;
        private readonly ILoggingAdapter logger = Context.GetLogger();
        private List<DurableEvent> buffer = new List<DurableEvent>();
        private ICancelable schedule = null;
        private long progress;

        public DurableEventSourceActor(IActorRef eventLog, long fromSequenceNr, string aggregateId)
        {
            this.eventLog = eventLog;
            this.fromSequenceNr = fromSequenceNr;
            this.aggregateId = aggregateId;
            this.settings = new DurableEventSourceSettings(Context.System.Settings.Config);
            this.progress = this.fromSequenceNr - 1L;
        }
        protected override bool Receive(object message) => Reading(message);

        private bool Reading(object message)
        {
            switch (message)
            {
                case ReplaySuccess s:
                    if (s.Events.Any())
                    {
                        Buffer(s.Events, s.ReplayProgress);
                        if (Emit())
                            Read();
                        else
                            Context.Become(Waiting);
                    }
                    else
                    {
                        this.schedule = SchedulePaused();
                        Context.Become(Pausing);
                    }
                    return true;

                case ReplayFailure f:
                    this.schedule = SchedulePaused();
                    Context.Become(Pausing);
                    logger.Warning("Reading from log failed, because of: {0}", f.Cause);
                    return true;

                default: return false;
            }
        }

        private bool Waiting(object message)
        {
            if (message is Request request)
            {
                if (buffer.Count == 0 || Emit())
                {
                    Read();
                    Context.Become(Reading);
                }

                return true;
            }
            else return false;
        }

        private bool Pausing(object message)
        {
            switch (message)
            {
                case Paused _:
                    schedule = null;
                    if (TotalDemand > 0L)
                    {
                        Read();
                        Context.Become(Reading);
                    }
                    else
                    {
                        Context.Become(Waiting);
                    }
                    return true;

                case Written w:
                    schedule?.Cancel();
                    schedule = null;
                    if (TotalDemand > 0L)
                    {
                        Read();
                        Context.Become(Reading);
                    }
                    else
                    {
                        Context.Become(Waiting);
                    }
                    return true;

                default: return false;
            }
        }

        protected override void PreStart()
        {
            base.PreStart();
            Context.Watch(this.eventLog);
            Read(subscribe: true);
        }

        protected override void Unhandled(object message)
        {
            if (message is Cancel || message is SubscriptionTimeoutExceeded)
                Context.Stop(Self);
            else if (message is Terminated t && t.ActorRef.Equals(this.eventLog))
                OnCompleteThenStop();
            else
                base.Unhandled(message);
        }

        private void Read(bool subscribe = false)
        {
            var subscriber = subscribe ? Self : null;
            var fromSequenceNr = this.progress + 1L;

            eventLog.Ask(new Replay(subscriber, 1, fromSequenceNr, settings.ReplayBatchSize, aggregateId), timeout: settings.ReadTimeout)
                .PipeTo(Self, failure: e => new ReplayFailure(e, fromSequenceNr, 1));
        }

        private bool Emit()
        {
            var splitPosition = Math.Min(buffer.Count, (TotalDemand > int.MaxValue) ? int.MaxValue : (int)TotalDemand);
            for (int i = 0; i < splitPosition; i++)
            {
                OnNext(buffer[i]);
            }

            buffer.RemoveRange(0, splitPosition);
            return buffer.Count == 0 && TotalDemand > 0L;
        }

        private void Buffer(IEnumerable<DurableEvent> events, long to)
        {
            buffer.AddRange(events);
            progress = to;
        }

        private ICancelable SchedulePaused() =>
            Context.System.Scheduler.ScheduleTellOnceCancelable(settings.ReplayRetryDelay, Self, new Paused(), Self);
    }
}
