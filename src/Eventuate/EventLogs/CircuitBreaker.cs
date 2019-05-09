using Akka.Actor;
using Akka.Configuration;
using Eventuate.EventsourcingProtocol;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;

namespace Eventuate.EventLogs
{
    internal sealed class CircuitBreakerSettings
    {
        public CircuitBreakerSettings(Config config)
        {
            this.OpenAfterRetries = config.GetInt("eventuate.log.circuit-breaker.open-after-retries");
        }

        public int OpenAfterRetries { get; }
    }

    public class EventLogUnavailableException : Exception
    {
        public EventLogUnavailableException(): base("Circuit breaker is open. Event log is not available right now.") { }

        public EventLogUnavailableException(string message) : base(message)
        {
        }
    }

    /// <summary>
    /// A wrapper that can protect [[EventLog]] implementations from being overloaded while they are retrying to
    /// serve a write request. If the circuit breaker is closed, it forwards all requests to the underlying event
    /// log. If it is open, it replies with a failure message to the requestor. The circuit breaker can be opened
    /// by sending it `ServiceFailure` messages with a `retry` value greater than or equal to the configuration
    /// parameter `eventuate.log.circuit-breaker.open-after-retries`. It can be closed again by sending it a
    /// `ServiceNormal` or `ServiceInitialized` message. These messages are usually sent by [[EventLog]]
    /// implementations and not by applications.
    /// </summary>
    /// @see [[EventLogSPI.write]]
    /// 
    public sealed class CircuitBreaker : ActorBase
    {
        public static readonly EventLogUnavailableException Exception = new EventLogUnavailableException();

        private readonly CircuitBreakerSettings settings;
        private readonly IActorRef eventLog;

        public CircuitBreaker(Props logProps, bool batching)
        {
            this.settings = new CircuitBreakerSettings(Context.System.Settings.Config);
            this.eventLog = Context.Watch(CreateLog(logProps, batching));
        }

        private IActorRef CreateLog(Props logProps, bool batching)
        {
            if (batching)
                return Context.ActorOf(Props.Create(() => new BatchingLayer(logProps)));
            else
                return Context.ActorOf(logProps);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Publish(ServiceEvent e) => Context.System.EventStream.Publish(e);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected override bool Receive(object message) => Closed(message);

        private bool Closed(object message)
        {
            switch (message)
            {
                case ServiceEvent e:
                    if (e.Type == ServiceEvent.EventType.ServiceFailed)
                    {
                        if (e.Retry >= settings.OpenAfterRetries)
                        {
                            Publish(e);
                            Context.Become(Open);
                        }
                    }
                    return true;

                case Terminated _:
                    Context.Stop(Self);
                    return true;

                default:
                    eventLog.Forward(message);
                    return true;
            }
        }

        private bool Open(object message)
        {
            switch (message)
            {
                case ServiceEvent e:
                    if (e.Type != ServiceEvent.EventType.ServiceFailed)
                    {
                        Publish(e);
                        Context.Become(Closed);
                    }
                    return true;

                case Terminated _:
                    Context.Stop(Self);
                    return true;

                case Write w:
                    // Write requests are not made via ask
                    w.ReplyTo.Tell(new WriteFailure(w.Events, Exception, w.CorrelationId, w.InstanceId));
                    return true;

                default:
                    Sender.Tell(new Status.Failure(Exception));
                    return true;
            }
        }
    }

    /// <summary>
    /// An event that controls <see cref="CircuitBreaker"/> state.
    /// </summary>
    public sealed class ServiceEvent
    {
        public enum EventType
        {
            /// <summary>
            /// Sent by an event log to indicate that it has been successfully initialized.
            /// </summary>
            ServiceInitialized,

            /// <summary>
            /// Sent by an event log to indicate that it has successfully written an event batch.
            /// 
            /// This is also published on the event-stream when it closes the <see cref="CircuitBreaker"/>
            /// (after previous failures that opened the <see cref="CircuitBreaker"/>).
            /// </summary>
            ServiceNormal,

            /// <summary>
            /// Sent by an event log to indicate that it failed to write an event batch. The current
            /// retry count is given by the `retry` parameter.
            /// 
            /// This is also published on the event-stream when it opens the <see cref="CircuitBreaker"/>,
            /// i.e. when `retry` exceeds a configured limit.
            /// </summary>
            ServiceFailed
        }
        
        /// <summary>
        /// Sent by an event log to indicate that it has been successfully initialized.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ServiceEvent Initialized(string logId) => 
            new ServiceEvent(logId, EventType.ServiceInitialized); 
        
        /// <summary>
        /// Sent by an event log to indicate that it has successfully written an event batch.
        /// 
        /// This is also published on the event-stream when it closes the <see cref="CircuitBreaker"/>
        /// (after previous failures that opened the <see cref="CircuitBreaker"/>).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ServiceEvent Normal(string logId) => 
            new ServiceEvent(logId, EventType.ServiceNormal); 
        
        /// <summary>
        /// Sent by an event log to indicate that it failed to write an event batch. The current
        /// retry count is given by the `retry` parameter.
        /// 
        /// This is also published on the event-stream when it opens the <see cref="CircuitBreaker"/>,
        /// i.e. when `retry` exceeds a configured limit.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ServiceEvent Failed(string logId, int retry, Exception cause) =>
            new ServiceEvent(logId, EventType.ServiceFailed, retry, cause);

        public string LogId { get; }
        public EventType Type { get; }
        public int Retry { get; }
        public Exception Cause { get; }

        private ServiceEvent(string logId, EventType type, int retry = 0, Exception cause = null)
        {
            Type = type;
            LogId = logId;
            Retry = retry;
            Cause = cause;
        }
    }
}
