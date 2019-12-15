#region copyright
// -----------------------------------------------------------------------
//  <copyright file="Chat.cs" company="Akka.NET Project">
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using Akka.Actor;
using Akka.Event;
using Eventuate;

namespace Chatter
{
    public class Chat : EventsourcedActor
    {
        #region commands

        public sealed class Send
        {
            public string User { get; }
            public string Message { get; }

            public Send(string user, string message)
            {
                User = user;
                Message = message;
            }
        }

        #endregion

        #region events

        public sealed class Sent
        {
            public string User { get; }
            public string Message { get; }

            public Sent(string user, string message)
            {
                User = user;
                Message = message;
            }
        }
        
        #endregion
        
        public Chat(string chatroom, IActorRef eventLog)
        {
            Id = chatroom;
            EventLog = eventLog;
        }

        public override string Id { get; }
        public override IActorRef EventLog { get; }
        protected override bool OnCommand(object message)
        {
            switch (message)
            {
                case Send send:
                {
                    var sender = Sender;
                    Persist(new Sent(send.User, send.Message), attempt =>
                    {
                        if (attempt.IsFailure)
                            Logger.Error(attempt.Exception, "Failed to send message from {0}: {1}", send.User, send.Message);
                        else
                            Logger.Info("{0} wrote: {1}", attempt.Value.User, attempt.Value.Message);
                        sender.Tell(new object());
                    });
                    return true;
                }
                default: return false;
            }
        }

        protected override bool OnEvent(object message)
        {
            switch (message)
            {
                case Sent sent:
                {
                    Logger.Info("{0} wrote: {1}", sent.User, sent.Message);
                    return true;
                }
                default: return false;
            }
        }
    }
}