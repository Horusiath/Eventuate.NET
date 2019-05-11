#region copyright
// -----------------------------------------------------------------------
//  <copyright file="BinaryPayload.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Collections.Generic;
using System.Text;

namespace Eventuate
{
    /// <summary>
    /// Represents a serialized payload.
    /// </summary>
    internal readonly struct BinaryPayload
    {
        public BinaryPayload(System.Buffers.ReadOnlySequence<byte> bytes, int serializerId, string manifest = null, bool isStringManifest = false)
        {
            Bytes = bytes;
            SerializerId = serializerId;
            Manifest = manifest;
            IsStringManifest = isStringManifest;
        }

        /// <summary>
        /// Serialized payload.
        /// </summary>
        public System.Buffers.ReadOnlySequence<byte> Bytes { get; }

        /// <summary>
        /// The optional manifest provided by the <see cref="Akka.Serialization.Serializer"/>.
        /// </summary>
        public string Manifest { get; }

        /// <summary>
        /// <see cref="Akka.Serialization.Serializer.Identifier"/> of the <see cref="Akka.Serialization.Serializer"/> that creates the serialized payload
        /// </summary>
        public int SerializerId { get; }

        /// <summary>
        /// `true` if <see cref="Manifest"/> is a string manifest (from a <see cref="Akka.Serialization.SerializerWithStringManifest"/>)
        /// </summary>
        public bool IsStringManifest { get; }
    }
}
