#region copyright
// -----------------------------------------------------------------------
//  <copyright file="SnapshotSerializer.cs" company="Akka.NET Project">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Collections.Immutable;
using System.Reflection;
using Akka.Actor;
using Akka.Serialization;
using Eventuate.EventLogs;
using Eventuate.Serialization.Proto;
using Google.Protobuf;

namespace Eventuate.Serialization
{
    public class SnapshotSerializer : SerializerWithStringManifest
    {
        private const string SnapshotManifest = "a";
        private const string ClockManifest = "b";
        private const string ConcurrentVersionsTreeManifest = "c";

        private static readonly Type ConcurrentVersionsTreeType = typeof(ConcurrentVersionsTree<,>);
        private static readonly MethodInfo ConcurrentVersionsTreeFromProtoHandler;
        
        private readonly DelegatingDurableEventSerializer eventSerializer;

        static SnapshotSerializer()
        {
            ConcurrentVersionsTreeFromProtoHandler = typeof(SnapshotSerializer)
                .GetMethod(nameof(ConcurrentVersionsTreeFromProto), BindingFlags.NonPublic);
        }

        public SnapshotSerializer(ExtendedActorSystem system) : base(system)
        {
            this.eventSerializer = new DelegatingDurableEventSerializer(system);
        }

        public override int Identifier { get; } = 22566;
        
        public override string Manifest(object o)
        {
            switch (o)
            {
                case EventLogClock _: return ClockManifest;
                case Snapshot _: return SnapshotManifest;
                default:
                    var t = o.GetType();
                    if (t.IsGenericType && t.GetGenericTypeDefinition() == ConcurrentVersionsTreeType)
                        return ConcurrentVersionsTreeManifest;
                    else
                        throw new NotSupportedException($"{nameof(SnapshotSerializer)} cannot serialize '{t.FullName}'");
            }
        }
        
        public override byte[] ToBinary(object o)
        {
            switch (o)
            {
                case EventLogClock clock: return EventLogClockToProto(clock).ToByteArray();
                case Snapshot snapshot: return SnapshotToProto(snapshot).ToByteArray();
                default:
                    dynamic tree = o;
                    return ConcurrentVersionsTreeToProto(tree).ToByteArray();
            }
        }

        private ConcurrentVersionsTreeFormat ConcurrentVersionsTreeToProto<TValue, TUpdate>(ConcurrentVersionsTree<TValue, TUpdate> tree) =>
            new ConcurrentVersionsTreeFormat
            {
                Owner = tree.Owner,
                Root = ConcurrentVersionsTreeNodeToProto(tree.root)
            };

        private ConcurrentVersionsTreeNodeFormat ConcurrentVersionsTreeNodeToProto<TValue, TUpdate>(ConcurrentVersionsTree<TValue, TUpdate>.Node node)
        {
            var proto = new ConcurrentVersionsTreeNodeFormat
            {
                Rejected = node.rejected,
                Versioned = eventSerializer.commonSerializer.VersionedFormatBuilder(node.Versioned)
            };

            foreach (var child in node.children) 
                proto.Children.Add(ConcurrentVersionsTreeNodeToProto(child));

            return proto;
        }

        private EventLogClockFormat EventLogClockToProto(EventLogClock clock) =>
            new EventLogClockFormat
            {
                SequenceNr = clock.SequenceNr,
                VersionVector = eventSerializer.commonSerializer.VectorTimeFormatBuilder(clock.VersionVector)
            };

        private SnapshotFormat SnapshotToProto(Snapshot snapshot)
        {
            var proto = new SnapshotFormat
            {
                Payload = eventSerializer.commonSerializer.payloadSerializer.PayloadFormatBuilder(snapshot.Payload),
                CurrentTime = eventSerializer.commonSerializer.VectorTimeFormatBuilder(snapshot.CurrentTime),
                EmitterId = snapshot.EmitterId,
                SequenceNr = snapshot.SequenceNr,
                LastEvent = eventSerializer.DurableEventFormatBuilder(snapshot.LastEvent)
            };

            foreach (var deliveryAttempt in snapshot.DeliveryAttempts)
            {
                proto.DeliveryAttempts.Add(DeliveryAttemptToProto(deliveryAttempt));
            }

            foreach (var request in snapshot.PersistOnEventRequests)
            {
                proto.PersistOnEventRequests.Add(PersistOnEventRequestToProto(request));
            }
            
            return proto;
        }

        private PersistOnEventRequestFormat PersistOnEventRequestToProto(PersistOnEventRequest request)
        {
            var proto = new PersistOnEventRequestFormat
            {
                SequenceNr = request.PersistOnEventSequenceNr,
                InstanceId = request.InstanceId
            };

            if (request.PersistOnEventId.HasValue)
                proto.EventId = eventSerializer.EventIdFormatBuilder(request.PersistOnEventId.Value);

            foreach (var invocation in request.Invocations)
            {
                proto.Invocation.Add(InvocationToProto(invocation));
            }

            return proto;
        }

        private PersistOnEventInvocationFormat InvocationToProto(in PersistOnEventInvocation format)
        {
            var proto =new PersistOnEventInvocationFormat
            {
                Event = eventSerializer.commonSerializer.payloadSerializer.PayloadFormatBuilder(format.Event)
            };

            foreach (var aggregateId in format.CustomDestinationAggregateIds)
            {
                proto.CustomDestinationAggregateIds.Add(aggregateId);
            }

            return proto;
        }

        private DeliveryAttemptFormat DeliveryAttemptToProto(DeliveryAttempt deliveryAttempt) =>
            new DeliveryAttemptFormat
            {
                DeliveryId = deliveryAttempt.DeliveryId,
                Message = eventSerializer.commonSerializer.payloadSerializer.PayloadFormatBuilder(deliveryAttempt.Message),
                Destination = deliveryAttempt.Destination.ToSerializationFormat()
            };


        public override object FromBinary(byte[] bytes, string manifest)
        {
            switch (manifest)
            {
                case ClockManifest: return EventLogClockFromProto(EventLogClockFormat.Parser.ParseFrom(bytes));
                case SnapshotManifest: return SnapshotFromProto(SnapshotFormat.Parser.ParseFrom(bytes));
                case ConcurrentVersionsTreeManifest:
                    var format = ConcurrentVersionsTreeFormat.Parser.ParseFrom(bytes);
                    return ConcurrentVersionsTreeFromProtoHandler
                        .MakeGenericMethod(ClassTagAttribute.TypeFor(format.ValueClassTag), ClassTagAttribute.TypeFor(format.UpdateClassTag))
                        .Invoke(this, new[] {format});
                default:
                    throw new NotSupportedException($"{nameof(SnapshotSerializer)} cannot deserialize type for unknown manifest of '{manifest}'");
            }
        }

        private EventLogClock EventLogClockFromProto(EventLogClockFormat format) => 
            new EventLogClock(format.SequenceNr, eventSerializer.commonSerializer.VectorTime(format.VersionVector));

        private ConcurrentVersionsTree<TValue, TUpdate> ConcurrentVersionsTreeFromProto<TValue, TUpdate>(ConcurrentVersionsTreeFormat format) => 
            new ConcurrentVersionsTree<TValue, TUpdate>(ConcurrentVersionsTreeNodeFromProto<TValue, TUpdate>(format.Root), format.Owner);

        private ConcurrentVersionsTree<TValue, TUpdate>.Node ConcurrentVersionsTreeNodeFromProto<TValue, TUpdate>(ConcurrentVersionsTreeNodeFormat format)
        {
            var node = new ConcurrentVersionsTree<TValue, TUpdate>.Node(eventSerializer.commonSerializer.Versioned<TValue>(format.Versioned));
            node.rejected = format.Rejected;

            if (format.Children.Count != 0)
            {
                var builder = ImmutableArray.CreateBuilder<ConcurrentVersionsTree<TValue, TUpdate>.Node>(format.Children.Count);
                foreach (var childFormat in format.Children)
                {
                    builder.Add(ConcurrentVersionsTreeNodeFromProto<TValue, TUpdate>(childFormat));
                }

                node.children = builder.ToImmutable();
            }

            return node;
        }

        private Snapshot SnapshotFromProto(SnapshotFormat format)
        {
            var deliveryAttempts = ImmutableHashSet.CreateBuilder<DeliveryAttempt>();
            foreach (var attempt in format.DeliveryAttempts)
            {
                deliveryAttempts.Add(DeliveryAttemptFromProto(attempt));
            }
            
            var persistOnEventRequests = ImmutableHashSet.CreateBuilder<PersistOnEventRequest>();
            foreach (var request in format.PersistOnEventRequests)
            {
                persistOnEventRequests.Add(PersistOnEventRequestFromProto(request));
            }
            
            return new Snapshot(
                payload: eventSerializer.commonSerializer.payloadSerializer.Payload(format.Payload),
                emitterId: format.EmitterId,
                lastEvent: eventSerializer.ToDurableEvent(format.LastEvent),
                currentTime: eventSerializer.commonSerializer.VectorTime(format.CurrentTime),
                sequenceNr: format.SequenceNr == 0 ? default : format.SequenceNr,
                deliveryAttempts: deliveryAttempts.ToImmutable(),
                persistOnEventRequests: persistOnEventRequests.ToImmutable());
        }

        private PersistOnEventRequest PersistOnEventRequestFromProto(PersistOnEventRequestFormat format)
        {
            var invocations = ImmutableArray.CreateBuilder<PersistOnEventInvocation>(format.Invocation.Count);
            foreach (var invocationFormat in format.Invocation)
            {
                invocations.Add(PersistOnEventInvocationFromProto(invocationFormat));
            }
            return new PersistOnEventRequest(
                persistOnEventSequenceNr: format.SequenceNr,
                persistOnEventId: format.EventId is null ? default : eventSerializer.ToEventId(format.EventId),
                invocations: invocations.ToImmutable(),
                instanceId: format.InstanceId);
        }

        private PersistOnEventInvocation PersistOnEventInvocationFromProto(PersistOnEventInvocationFormat format)
        {
            var aggregateIds = ImmutableHashSet.CreateBuilder<string>();
            foreach (var aggregateId in format.CustomDestinationAggregateIds) 
                aggregateIds.Add(aggregateId);

            return new PersistOnEventInvocation(
                eventSerializer.commonSerializer.payloadSerializer.Payload(format.Event),
                aggregateIds.ToImmutable());
        }

        private DeliveryAttempt DeliveryAttemptFromProto(DeliveryAttemptFormat format) =>
            new DeliveryAttempt(
                deliveryId: format.DeliveryId,
                message: eventSerializer.commonSerializer.payloadSerializer.Payload(format.Message),
                destination: ActorPath.Parse(format.Destination));
    }
}