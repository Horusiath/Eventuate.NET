using Akka.Actor;

namespace Eventuate
{
    public sealed class DeliveryAttempt
    {
        public DeliveryAttempt(string deliveryId, object message, ActorPath destination) {
            DeliveryId = deliveryId;
            Message = message;
            Destination = destination;
        }

        public string DeliveryId { get; }
        public object Message { get; }
        public ActorPath Destination { get; }
    }

    /// <summary>
    /// Supports the reliable delivery of messages to destinations by enabling applications to
    /// redeliver messages until they are confirmed by their destinations. The correlation
    /// identifier between a reliable message and its confirmation message is an
    /// application-defined `deliveryId`. Reliable messages are delivered by calling `deliver` in
    /// an <see cref="EventsourcedActor"/>'s event handler. When the destination replies with a confirmation
    /// message, the event-sourced actor must persist an application-defined confirmation event
    /// together with the `deliveryId` using the [[persistConfirmation]] method. Until successful
    /// persistence of the confirmation event, delivered messages are tracked as ''unconfirmed''
    /// messages. Unconfirmed messages can be redelivered by calling `redeliverUnconfirmed`. This
    /// is usually done within a command handler by processing scheduler messages. Redelivery
    /// occurs automatically when the event-sourced actor successfully recovered after initial
    /// start or a re-start.
    /// </summary>
    public abstract class ConfirmedDelivery : EventsourcedActor
    {

    }
}