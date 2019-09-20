#region copyright
// -----------------------------------------------------------------------
//  <copyright file="DelegatingDurableEventSerializer.cs">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Collections.Immutable;
using Akka.Actor;
using Akka.Serialization;
using Eventuate.Serialization.Proto;
using Google.Protobuf;

namespace Eventuate.Serialization
{
    /// <summary>
    /// A Protocol Buffers based serializer for <see cref="DurableEvent"/>s.
    /// Serialization of `DurableEvent`'s `payload` is delegated to a serializer that is configured with Akka's
    /// serialization extension mechanism.
    /// 
    /// This serializer is configured by default (in `reference.conf`) for <see cref="DurableEvent"/>s
    /// </summary>
    internal sealed class DelegatingDurableEventSerializer : DurableEventSerializer
    {
        public DelegatingDurableEventSerializer(ExtendedActorSystem system) : base(system, new DelegatingPayloadSerializer())
        {
        }
    }

    /// <summary>
    /// A Protocol Buffers based serializer for <see cref="DurableEvent"/>s.
    /// Serialization of `DurableEvent`'s `payload` is delegated to <see cref="BinaryPayloadSerializer"/>.
    /// 
    /// To use this serializer the default config provided by `reference.conf` has to be overwritten in an
    /// `application.conf` like follows:
    /// 
    /// <code>
    ///   akka.actor.serializers.eventuate-durable-event = com.rbmhtechnology.eventuate.serializer.DurableEventSerializerWithBinaryPayload
    /// </code>
    /// 
    /// This serializer can be useful in scenarios where an Eventuate application is just used for
    /// event forwarding (through replication) and only handles the payload of events
    /// based on their metadata (like the manifest).
    /// </summary>
    internal sealed class DurableEventSerializerWithBinaryPayload : DurableEventSerializer
    {
        public DurableEventSerializerWithBinaryPayload(ExtendedActorSystem system) 
            : base(system, new BinaryPayloadSerializer())
        {
        }
    }
    
    internal abstract class DurableEventSerializer : Serializer
    {
        private readonly ExtendedActorSystem system;
        private readonly IPayloadSerializer payloadSerializer;
        internal readonly CommonSerializer commonSerializer;

        protected DurableEventSerializer(ExtendedActorSystem system, IPayloadSerializer payloadSerializer) : base(system)
        {
            this.system = system;
            this.payloadSerializer = payloadSerializer;
            this.commonSerializer = new CommonSerializer(system);
        }
        
        public sealed override int Identifier { get; } = 22563;
        public sealed override bool IncludeManifest { get; } = false;

        public override byte[] ToBinary(object obj)
        {
            if (obj is DurableEvent e)
            {
                return DurableEventFormatBuilder(e).ToByteArray();
            }
            
            throw new NotSupportedException($"{this.GetType().FullName} cannot serialize type '{obj.GetType().FullName}'");
        }

        public override object FromBinary(byte[] bytes, Type type)
        {
            return ToDurableEvent(DurableEventFormat.Parser.ParseFrom(bytes));
        }

        internal DurableEvent ToDurableEvent(DurableEventFormat format)
        {
            var customAggregateIds = ImmutableHashSet.CreateBuilder<string>();
            foreach (var id in format.CustomDestinationAggregateIds)
            {
                customAggregateIds.Add(id);
            }
            
            return new DurableEvent(
                payload: payloadSerializer.Payload(format.Payload),
                emitterId: format.EmitterId,
                emitterAggregateId: format.EmitterId,
                customDestinationAggregateIds: customAggregateIds.ToImmutable(),
                systemTimestamp: new DateTime(format.SystemTimestamp),
                vectorTimestamp: commonSerializer.VectorTime(format.VectorTimestamp),
                processId: format.ProcessId,
                localLogId: format.LocalLogId,
                localSequenceNr: format.LocalSequenceNr,
                deliveryId: format.DeliveryId,
                persistOnEventId: format.PersistOnEventId is null ? default : ToEventId(format.PersistOnEventId),
                persistOnEventSequenceNr: format.PersistOnEventSequenceNr == 0L ? default : format.PersistOnEventSequenceNr);
        }

        internal EventId ToEventId(EventIdFormat format) => new EventId(format.ProcessId, format.SequenceNr);

        internal DurableEventFormat DurableEventFormatBuilder(DurableEvent e)
        {
            var proto = new DurableEventFormat
            {
                Payload = payloadSerializer.PayloadFormatBuilder(e.Payload),
                EmitterId = e.EmitterId,
                SystemTimestamp = e.SystemTimestamp.Ticks,
                VectorTimestamp = commonSerializer.VectorTimeFormatBuilder(e.VectorTimestamp),
                ProcessId = e.ProcessId,
                LocalLogId = e.LocalLogId,
                LocalSequenceNr = e.LocalSequenceNr
            };

            if (!string.IsNullOrEmpty(e.DeliveryId))
                proto.DeliveryId = e.DeliveryId;

            if (e.PersistOnEventSequenceNr.HasValue)
                proto.PersistOnEventSequenceNr = e.PersistOnEventSequenceNr.Value;

            if (e.PersistOnEventId.HasValue)
                proto.PersistOnEventId = EventIdFormatBuilder(e.PersistOnEventId.Value);

            if (!string.IsNullOrEmpty(e.EmitterAggregateId))
                proto.EmitterAggregateId = e.EmitterAggregateId;

            foreach (var id in e.CustomDestinationAggregateIds) 
                proto.CustomDestinationAggregateIds.Add(id);

            return proto;
        }

        internal EventIdFormat EventIdFormatBuilder(in EventId id) =>
            new EventIdFormat
            {
                ProcessId = id.ProcessId,
                SequenceNr = id.SequenceNr
            };
    }
}