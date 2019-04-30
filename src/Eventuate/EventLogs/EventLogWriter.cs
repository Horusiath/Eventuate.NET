using Akka.Actor;
using System;
using System.Threading.Tasks;

namespace Eventuate.EventLogs
{
    /// <summary>
    /// Utility for writing events to an event log.
    /// </summary>
    public sealed class EventLogWriter
    {
        #region actor

        private sealed class Actor : EventsourcedActor
        {
            private readonly string id;
            private readonly IActorRef eventLog;

            public override string AggregateId { get; }

            public Actor(string id, IActorRef eventLog, string aggregateId)
            {
                this.id = id;
                this.eventLog = eventLog;
                this.AggregateId = aggregateId;
            }

            protected override async ValueTask OnCommand(object e)
            {
                var sender = Sender;
                try
                {
                    await Persist(e);
                    sender.Tell(LastHandledEvent);
                }
                catch (Exception cause)
                {
                    sender.Tell(new Status.Failure(cause));
                }
            }

            protected override void OnEvent(object message) { }
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

        public EventLogWriter(ActorSystem system, string id, IActorRef eventLog, string aggregateId = null)
        {
            this.system = system;
            this.id = id;
            this.eventLog = eventLog;
            this.aggregateId = aggregateId;
        }
    }
}
