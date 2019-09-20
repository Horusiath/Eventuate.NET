#region copyright
// -----------------------------------------------------------------------
//  <copyright file="PayloadSerializer.cs">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Buffers;
using Akka.Actor;
using Akka.Serialization;
using Akka.Util;
using Eventuate.Serialization.Proto;
using Google.Protobuf;

namespace Eventuate.Serialization
{
    /// <summary>
    /// Interface of a serializer converting between payloads and <see cref="PayloadFormat"/>.
    /// </summary>
    internal interface IPayloadSerializer
    {
        PayloadFormat PayloadFormatBuilder(object payload);
        object Payload(PayloadFormat format);
    }

    /// <summary>
    /// A <see cref="IPayloadSerializer"/> delegating to a serializer that is
    /// configured with Akka's serialization extension mechanism.
    /// </summary>
    internal readonly struct DelegatingPayloadSerializer : IPayloadSerializer
    {
        private readonly Akka.Serialization.Serialization serialization;
        public DelegatingPayloadSerializer(ExtendedActorSystem system)
        {
            this.serialization = system.Serialization;
        }

        public PayloadFormat PayloadFormatBuilder(object payload)
        {
            var serializer = serialization.FindSerializerFor(payload);
            var proto = new PayloadFormat();

            if (serializer.IncludeManifest)
            {
                if (serializer is SerializerWithStringManifest s)
                {
                    proto.PayloadManifest = s.Manifest(payload);
                    proto.IsStringManifest = true;
                }
                else
                {
                    proto.PayloadManifest = payload.GetType().TypeQualifiedName();
                }
            }
            
            proto.Payload = ByteString.CopyFrom(serializer.ToBinary(payload));
            proto.SerializerId = serializer.Identifier;
            return proto;
        }

        public object Payload(PayloadFormat format)
        {
            if (format.IsStringManifest)
                return this.serialization.Deserialize(format.Payload.ToByteArray(), format.SerializerId,
                    format.PayloadManifest);
            else if (format.PayloadManifest is null)
                return this.serialization.Deserialize(format.Payload.ToByteArray(), format.SerializerId, default(Type));
            else
            {
                var type = Type.GetType(format.PayloadManifest, throwOnError: true);
                return this.serialization.Deserialize(format.Payload.ToByteArray(), format.SerializerId, type);
            }
        }
    }

    /// <summary>
    /// A <see cref="IPayloadSerializer"/> converting between <see cref="BinaryPayload"/> and <see cref="PayloadFormat"/>.
    /// </summary>
    internal readonly struct BinaryPayloadSerializer : IPayloadSerializer
    {
        public PayloadFormat PayloadFormatBuilder(object payload)
        {
            var p = (BinaryPayload) payload;
            var proto = new PayloadFormat
            {
                SerializerId = p.SerializerId,
                IsStringManifest = p.IsStringManifest,
                Payload = ByteString.CopyFrom(p.Bytes.ToArray())
            };

            if (!(p.Manifest is null))
                proto.PayloadManifest = p.Manifest;

            return proto;
        }

        public object Payload(PayloadFormat format)
        {
            return new BinaryPayload(
                bytes: new ReadOnlySequence<byte>(format.Payload.ToByteArray()), 
                serializerId: format.SerializerId,
                manifest: format.PayloadManifest,
                isStringManifest: format.IsStringManifest);
        }
    }
}