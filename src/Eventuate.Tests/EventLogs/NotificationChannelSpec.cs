#region copyright
// -----------------------------------------------------------------------
//  <copyright file="NotificationChannelSpec.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Collections.Immutable;
using System.Threading;
using Akka.Actor;
using Akka.TestKit;
using Akka.TestKit.Xunit2;
using Eventuate.EventLogs;
using Eventuate.ReplicationProtocol;
using Xunit;
using Xunit.Abstractions;

namespace Eventuate.Tests.EventLogs
{
    public class NotificationChannelSpec : TestKit
    {
        internal sealed class PayloadFilter : ReplicationFilter
        {
            private readonly object accept;

            public PayloadFilter(object accept)
            {
                this.accept = accept;
            }

            public override bool Invoke(DurableEvent durableEvent) => durableEvent.Payload.Equals(accept);
        }
        
        private static readonly TimeSpan Timeout = TimeSpan.FromMilliseconds(200);
        private const string sourceLogId = "slid";
        private const string targetLogId1 = "tlid1";
        private const string targetLogId2 = "tlid2";
        
        private static DurableEvent Event(object payload, VectorTime vectorTimestamp) =>
            new DurableEvent(payload, "emitter", vectorTimestamp: vectorTimestamp);
        
        private static VectorTime VectorTime(long s, long t1, long t2) =>
            new VectorTime((sourceLogId, s), (targetLogId1, t1), (targetLogId2, t2));

        private readonly IActorRef channel;
        private readonly TestProbe probe;
        private readonly TimeSpan expirationDuration = TimeSpan.FromMilliseconds(400);
        
        public NotificationChannelSpec(ITestOutputHelper output) : base(config: "eventuate.log.replication.retry-delay = 400ms", output: output)
        {
            this.channel = Sys.ActorOf(Props.Create(() => new NotificationChannel(sourceLogId)));
            this.probe = CreateTestProbe();
        }

        private void SourceRead(string targetLogId, VectorTime targetVersionVector, ReplicationFilter filter = null, TestProbe probe = null)
        {
            channel.Tell(SourceReadMessage(targetLogId, targetVersionVector, filter, probe));
            channel.Tell(SourceReadSuccessMessage(targetLogId));
        }

        private ReplicationRead SourceReadMessage(string targetLogId, VectorTime targetVersionVector,
            ReplicationFilter filter = null, TestProbe probe = null)
        {
            filter = filter ?? NoFilter.Instance;
            probe = probe ?? this.probe;
            return new ReplicationRead(1L, 10, 100, filter, targetLogId, probe.Ref, targetVersionVector);
        }
        
        private ReplicationReadSuccess SourceReadSuccessMessage(string targetLogId) =>
            new ReplicationReadSuccess(Array.Empty<DurableEvent>(), 10L, 9L, targetLogId, Eventuate.VectorTime.Zero);

        private void SourceUpdate(params DurableEvent[] events) => channel.Tell(new NotificationChannel.Updated(events));
        
        private void ReplicaVersionUpdate(string targetLogId, VectorTime targetVersionVector) => 
            channel.Tell(new ReplicationWrite(Array.Empty<DurableEvent>(), ImmutableDictionary<string, ReplicationMetadata>.Empty.Add(targetLogId, new ReplicationMetadata(10L, targetVersionVector))));

        [Fact]
        public void NotificationChannel_must_send_notification_if_an_update_is_not_the_causal_past_of_the_target()
        {
            SourceRead(targetLogId1, VectorTime(0, 1, 0));
            SourceUpdate(Event("a", VectorTime(1, 0, 0)));

            probe.ExpectMsg<ReplicationDue>();
            probe.ExpectNoMsg(Timeout);
        }
        
        [Fact]
        public void NotificationChannel_must_send_notification_if_part_of_update_is_not_in_the_causal_past_of_the_target()
        {
            SourceRead(targetLogId1, VectorTime(0, 1, 0));
            SourceUpdate(
                Event("a", VectorTime(0, 1, 0)),
                Event("b", VectorTime(1, 0, 0)));

            probe.ExpectMsg<ReplicationDue>();
            probe.ExpectNoMsg(Timeout);
        }
        
        [Fact]
        public void NotificationChannel_must_not_send_notification_if_update_is_in_the_causal_past_of_the_target()
        {
            SourceRead(targetLogId1, VectorTime(0, 2, 0));
            SourceUpdate(
                Event("a", VectorTime(0, 1, 0)),
                Event("b", VectorTime(0, 2, 0)));

            probe.ExpectNoMsg(Timeout);
        }
        
        [Fact]
        public void NotificationChannel_must_not_send_notification_if_update_does_not_pass_the_targets_replication_filter()
        {
            SourceRead(targetLogId1, VectorTime(0, 1, 0), new PayloadFilter("a"));
            SourceUpdate(Event("b", VectorTime(1, 0, 0)));

            probe.ExpectNoMsg(Timeout);
        }
        
        [Fact]
        public void NotificationChannel_must_not_send_notification_if_target_is_currently_running_replication_read()
        {
            channel.Tell(SourceReadMessage(targetLogId1, VectorTime(0,1,0)));
            SourceUpdate(
                Event("a", VectorTime(1, 0, 0)),
                Event("b", VectorTime(2, 0, 0)));
            
            probe.ExpectNoMsg(Timeout);
        }
        
        [Fact]
        public void NotificationChannel_must_apply_target_version_vector_updates()
        {
            SourceRead(targetLogId1, VectorTime(0, 1, 0));
            SourceUpdate(Event("a", VectorTime(0, 2, 0)));

            probe.ExpectMsg<ReplicationDue>();
            probe.ExpectNoMsg(Timeout);
            
            ReplicaVersionUpdate(targetLogId1, VectorTime(0, 2, 0));
            SourceUpdate(Event("a", VectorTime(0, 2, 0)));
            
            probe.ExpectNoMsg(Timeout);
        }
        
        [Fact]
        public void NotificationChannel_must_send_notification_to_several_targets()
        {
            SourceRead(targetLogId1, VectorTime(0, 1, 0));
            SourceRead(targetLogId2, VectorTime(0, 0, 1));
            SourceUpdate(Event("a", VectorTime(1, 0, 0)));
            
            probe.ExpectMsg<ReplicationDue>();
            probe.ExpectMsg<ReplicationDue>();
            probe.ExpectNoMsg(Timeout);
        }
        
        [Fact]
        public void NotificationChannel_must_not_send_notification_if_the_registration_is_expired()
        {
            SourceRead(targetLogId1, Eventuate.VectorTime.Zero);
            Thread.Sleep(expirationDuration);
            SourceUpdate(Event("a", VectorTime(1, 0, 0)));
            probe.ExpectNoMsg(Timeout);
        }
        
        [Fact]
        public void NotificationChannel_must_send_notification_after_the_registration_expired_but_new_replication_request_is_received()
        {
            SourceRead(targetLogId1, Eventuate.VectorTime.Zero);
            Thread.Sleep(expirationDuration);
            SourceRead(targetLogId1, Eventuate.VectorTime.Zero);
            SourceUpdate(Event("a", VectorTime(1, 0, 0)));
            probe.ExpectMsg<ReplicationDue>();
        }
    }
}