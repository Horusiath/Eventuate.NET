using System;
using System.Collections.Generic;
using System.Text;

namespace Eventuate
{
    /// <summary>
    /// A conditional request is a request to an actor in the <see cref="EventsourcedView"/>
    /// hierarchy whose delivery to the actor's command handler is delayed until
    /// the request's <see cref="Condition"/> is in the causal past of that actor (i.e. if the
    /// <see cref="Condition"/> is `<=` the actor's current version).
    /// </summary>
    public readonly struct ConditionalRequest
    {
        public ConditionalRequest(VectorTime condition, object request)
        {
            Condition = condition;
            Request = request;
        }

        public VectorTime Condition { get; }
        public object Request { get; }
    }

    /// <summary>
    /// Thrown by an actor in the <see cref="EventsourcedView"/> hierarchy if it receives
    /// a <see cref="ConditionalRequest"/> but does not extends the <see cref="ConditionalRequest"/>
    /// trait.
    /// </summary>
    public class ConditionalRequestException : Exception
    {
        public ConditionalRequestException(string message) : base(message) { }
    }

    public static class ConditionalRequests
    {

    }
}
