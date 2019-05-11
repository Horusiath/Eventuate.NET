#region copyright
// -----------------------------------------------------------------------
//  <copyright file="ConditionalRequest.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using Akka.Actor;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
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

    public abstract class ConditionalRequestActor : EventsourcedView, IEventsourcedVersion
    {
        #region internal classes

        private readonly struct Request
        {
            public Request(VectorTime condition, object Content, IActorRef sender)
            {
                Condition = condition;
                this.Content = Content;
                Sender = sender;
            }

            public VectorTime Condition { get; }
            public object Content { get; }
            public IActorRef Sender { get; }
        }

        private readonly struct Send
        {
            public Send(VectorTime olderThan)
            {
                OlderThan = olderThan;
            }

            public VectorTime OlderThan { get; }
        }
        
        private readonly struct Sent
        {
            public Sent(VectorTime olderThan, int num)
            {
                OlderThan = olderThan;
                Count = num;
            }

            public VectorTime OlderThan { get; }
            public int Count { get; }
        }

        private sealed class RequestManager : ActorBase
        {
            private readonly IActorRef requestBuffer;
            private readonly IActorRef owner;
            private VectorTime currentVersion;

            public RequestManager(IActorRef owner)
            {
                this.requestBuffer = Context.ActorOf(Props.Create(() => new RequestBuffer(owner)));
                this.currentVersion = VectorTime.Zero;
                this.owner = owner;
            }

            protected override bool Receive(object message) => Idle(message);

            private bool Idle(object message)
            {
                switch (message)
                {
                    case Request cr: Process(cr); return true;
                    case VectorTime t:
                        this.currentVersion = t;
                        this.requestBuffer.Tell(new Send(t));
                        Context.Become(Sending);
                        return true;
                    default: return false;
                }
            }

            private bool Sending(object message)
            {
                switch (message)
                {
                    case Request cr: Process(cr); return true;
                    case VectorTime t:
                        this.currentVersion = t;
                        return true;
                    case Sent s:
                        if (s.OlderThan == this.currentVersion)
                            Context.Become(Idle);
                        else
                            this.requestBuffer.Tell(new Send(this.currentVersion));
                        return true;
                    default: return false;
                }
            }

            private void Process(Request r)
            {
                if (r.Condition <= this.currentVersion)
                    owner.Tell(r.Content, r.Sender);
                else
                    requestBuffer.Tell(r);
            }
        }

        private sealed class RequestBuffer : ReceiveActor
        {
            private readonly IActorRef owner;
            private ImmutableArray<Request> requests = ImmutableArray<Request>.Empty;

            //TODO: cleanup requests older than threshold
            public RequestBuffer(IActorRef owner)
            {
                Receive<Send>(send => Sender.Tell(new Sent(send.OlderThan, Send(send.OlderThan))));
                Receive<Request>(cc => this.requests.Add(cc));
                this.owner = owner;
            }

            private int Send(VectorTime olderThan)
            {
                var i = 0;
                var builder = ImmutableArray.CreateBuilder<Request>(this.requests.Length);
                foreach (var request in this.requests)
                {
                    if (request.Condition <= olderThan)
                    {
                        this.owner.Tell(request.Content, request.Sender);
                        i++;
                    }
                    else
                        builder.Add(request);
                }
                this.requests = builder.ToImmutable();
                return i;
            }
        }

        #endregion

        private readonly IActorRef requestManager;

        protected ConditionalRequestActor()
        {
            this.requestManager = Context.ActorOf(Props.Create(() => new RequestManager(Self)));
        }

        internal override void ConditionalSend(VectorTime condition, object command) => 
            this.requestManager.Tell(new Request(condition, command, Sender));

        internal override void VersionChanged(VectorTime condition) => 
            this.requestManager.Tell(condition);
    }
}
