#region copyright
// -----------------------------------------------------------------------
//  <copyright file="NotificationChannel.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using Akka.Actor;
using Akka.Configuration;
using Eventuate.ReplicationProtocol;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;

namespace Eventuate.EventLogs
{
    public sealed class NotificationChannelSettings
    {
        public NotificationChannelSettings(Config config)
        {
            this.RegistrationExpirationDuration = config.GetTimeSpan("eventuate.log.replication.retry-delay");
        }

        public TimeSpan RegistrationExpirationDuration { get; }
    }

    /// <summary>
    /// Notifies registered replicators about source log updates.
    /// </summary>
    public sealed class NotificationChannel : ActorBase
    {
        #region internal messages

        internal readonly struct Updated
        {
            public Updated(IEnumerable<DurableEvent> events)
            {
                Events = events;
            }

            public IEnumerable<DurableEvent> Events { get; }
        }

        internal sealed class Registration
        {
            public Registration(ReplicationRead read)
                : this(read.Replicator, read.CurrentTargetVersionVector, read.Filter, DateTime.UtcNow)
            {
            }

            public Registration(IActorRef replicator, VectorTime currentTargetVersionVector, ReplicationFilter filter, DateTime registrationTime)
            {
                Replicator = replicator;
                CurrentTargetVersionVector = currentTargetVersionVector;
                Filter = filter;
                RegistrationTime = registrationTime;
            }

            public IActorRef Replicator { get; }
            public VectorTime CurrentTargetVersionVector { get; }
            public ReplicationFilter Filter { get; }
            public DateTime RegistrationTime { get; }
        }

        #endregion

        private readonly string logId;
        private readonly NotificationChannelSettings settings;

        /// <summary>
        /// targetLogId -> subscription
        /// </summary>
        private ImmutableDictionary<string, Registration> registry = ImmutableDictionary<string, Registration>.Empty;

        /// <summary>
        /// targetLogIds for which a read operation is in progress
        /// </summary>
        private ImmutableHashSet<string> reading = ImmutableHashSet<string>.Empty;

        public NotificationChannel(string logId)
        {
            this.logId = logId;
            this.settings = new NotificationChannelSettings(Context.System.Settings.Config);
        }

        protected override bool Receive(object message)
        {
            switch (message)
            {
                case Updated u:
                    var currentTime = DateTime.UtcNow;
                    foreach (var (targetLogId, reg) in this.registry)
                    {
                        if (!reading.Contains(targetLogId) 
                            && u.Events.Any(e => e.IsReplicable(reg.CurrentTargetVersionVector, reg.Filter))
                            && currentTime - reg.RegistrationTime <= settings.RegistrationExpirationDuration)
                        {
                            reg.Replicator.Tell(new ReplicationDue());
                        }
                    }

                    return true;

                case ReplicationRead r:
                    this.registry = this.registry.SetItem(r.TargetLogId, new Registration(r));
                    this.reading = this.reading.Add(r.TargetLogId);
                    return true;

                case ReplicationReadSuccess s:
                    this.reading = this.reading.Remove(s.TargetLogId);
                    return true;

                case ReplicationReadFailure f:
                    this.reading = this.reading.Remove(f.TargetLogId);
                    return true;

                case ReplicationWrite w:

                    return true;

                default: return false;
            }
        }
    }
}
