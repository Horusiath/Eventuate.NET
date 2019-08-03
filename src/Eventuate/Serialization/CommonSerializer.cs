#region copyright
// -----------------------------------------------------------------------
//  <copyright file="CommonSerializer.cs" company="Akka.NET Project">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Collections.Immutable;
using Akka.Actor;
using Eventuate.Serialization.Proto;

namespace Eventuate.Serialization
{
    internal readonly struct CommonSerializer
    {
        internal readonly DelegatingPayloadSerializer payloadSerializer;

        public CommonSerializer(ExtendedActorSystem system)
        {
            this.payloadSerializer = new DelegatingPayloadSerializer(system);
        }

        public VectorTimeFormat VectorTimeFormatBuilder(VectorTime vectorTime)
        {
            var proto = new VectorTimeFormat();
            foreach (var entry in vectorTime.Value)
            {
                proto.Entries.Add(new VectorTimeEntryFormat
                {
                    ProcessId = entry.Key,
                    LogicalTime = entry.Value
                });
            }
            return proto;
        }
        
        public VersionedFormat VersionedFormatBuilder<T>(in Versioned<T> version)
        {
            var proto = new VersionedFormat
            {
                Payload = payloadSerializer.PayloadFormatBuilder(version.Value),
                VectorTimestamp = VectorTimeFormatBuilder(version.VectorTimestamp),
                SystemTimestamp = version.SystemTimestamp.Ticks,
                Creator = version.Creator
            };
            return proto;
        }

        public VectorTime VectorTime(VectorTimeFormat format)
        {
            var builder = ImmutableDictionary.CreateBuilder<string, long>();
            foreach (var entry in format.Entries)
            {
                builder[entry.ProcessId] = entry.LogicalTime;
            }
            return new VectorTime(builder.ToImmutable());
        }

        public Versioned<T> Versioned<T>(VersionedFormat format)
        {
            return new Versioned<T>(
                value: (T)this.payloadSerializer.Payload(format.Payload),
                vectorTimestamp: VectorTime(format.VectorTimestamp),
                systemTimestamp: new DateTime(format.SystemTimestamp),
                creator: format.Creator);
        }
    }
}