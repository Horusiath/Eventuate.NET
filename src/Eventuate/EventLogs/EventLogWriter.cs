using Akka.Actor;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Eventuate.EventLogs
{
    /// <summary>
    /// Utility for writing events to an event log.
    /// </summary>
    public sealed class EventLogWriter : IDisposable
    {
        #region actor

        private sealed class Actor : EventsourcedActor
        {
            public override string AggregateId { get; }
            public override string Id { get; }
            public override IActorRef EventLog { get; }

            public Actor(string id, IActorRef eventLog, string aggregateId)
            {
                Id = id;
                EventLog = eventLog;
                AggregateId = aggregateId;
            }

            protected override bool OnCommand(object e)
            {
                Persist(e, attempt =>
                {
                    if (attempt.IsSuccess) Sender.Tell(LastHandledEvent);
                    else Sender.Tell(new Status.Failure(attempt.Exception));
                });
                return true;
            }

            protected override bool OnEvent(object message) => true;
        }

        #endregion

        private readonly ActorSystem system;

        /// <summary>
        /// Unique emitter id.
        /// </summary>
        private readonly string id;

        /// <summary>
        /// Event log to write to.
        /// </summary>
        private readonly IActorRef eventLog;

        /// <summary>
        /// Optional aggregate id.
        /// </summary>
        private readonly string aggregateId;

        /// <summary>
        /// FIXME: make configurable.
        /// </summary>
        private readonly TimeSpan timeout = TimeSpan.FromSeconds(10);

        private readonly IActorRef actor;

        public EventLogWriter(ActorSystem system, string id, IActorRef eventLog, string aggregateId = null)
        {
            this.system = system;
            this.id = id;
            this.eventLog = eventLog;
            this.aggregateId = aggregateId;
            this.actor = this.system.ActorOf(Props.Create(() => new Actor(id, eventLog, aggregateId)));
        }

        /// <summary>
        /// Asynchronously writes the given <paramref name="events"/> to `eventLog`.
        /// </summary>
        public async Task<DurableEvent[]> Write<T>(IEnumerable<T> events)
        {
            var tasks = events.Select(e => this.actor.Ask<DurableEvent>(e, this.timeout));
            return await Task.WhenAll(tasks);
        }

        /// <summary>
        /// Stops this writer.
        /// </summary>
        public void Dispose() => system.Stop(actor);
    }
}
