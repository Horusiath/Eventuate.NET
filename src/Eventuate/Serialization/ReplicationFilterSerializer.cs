#region copyright
// -----------------------------------------------------------------------
//  <copyright file="ReplicationFilterSerializer.cs" company="Akka.NET Project">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Linq;
using Akka.Actor;
using Akka.Serialization;
using Eventuate.Serialization.Proto;
using Google.Protobuf;

namespace Eventuate.Serialization
{
    public sealed class ReplicationFilterSerializer : SerializerWithStringManifest
    {
        private const string NoFilterManifest = "A";
        private const string AndFitlerManifest = "B";
        private const string OrFilterManifest = "C";
        
        private readonly DelegatingPayloadSerializer payloadSerializer;

        public ReplicationFilterSerializer(ExtendedActorSystem system) : base(system)
        {
            this.payloadSerializer = new DelegatingPayloadSerializer(system);
        }
        
        public override int Identifier { get; } = 22564;
        public override string Manifest(object o)
        {
            switch (o)
            {
                case NoFilter _: return NoFilterManifest;
                case AndFilter _: return AndFitlerManifest;
                case OrFilter _: return OrFilterManifest;
                default: throw new NotSupportedException($"{nameof(ReplicationFilterSerializer)} cannot serialize '{o.GetType().FullName}'");
            }
        }
        public override byte[] ToBinary(object obj)
        {
            switch (obj)
            {
                case NoFilter _: return Array.Empty<byte>();
                case ReplicationFilter r: return FilterTreeFormatBuilder(r).ToByteArray();
                default: throw new NotSupportedException($"{nameof(ReplicationFilterSerializer)} cannot serialize '{obj.GetType().FullName}'");
            }
        }

        internal ReplicationFilterTreeFormat FilterTreeFormatBuilder(ReplicationFilter filter)
        {
            var proto = new ReplicationFilterTreeFormat();
            switch (filter)
            {
                case AndFilter and:
                    proto.NodeType = ReplicationFilterTreeFormat.Types.NodeType.And;
                    foreach (var replicationFilter in and.Filters)
                        proto.Children.Add(FilterTreeFormatBuilder(replicationFilter));
                    break;
                
                case OrFilter or:
                    proto.NodeType = ReplicationFilterTreeFormat.Types.NodeType.Or;
                    foreach (var replicationFilter in or.Filters)
                        proto.Children.Add(FilterTreeFormatBuilder(replicationFilter));
                    break;
                
                default:
                    proto.NodeType = ReplicationFilterTreeFormat.Types.NodeType.Leaf;
                    proto.Filter = payloadSerializer.PayloadFormatBuilder(filter);
                    break;
            }

            return proto;
        }

        public override object FromBinary(byte[] bytes, string manifest)
        {
            switch (manifest)
            {
                case NoFilterManifest: return NoFilter.Instance;
                case AndFitlerManifest:
                case OrFilterManifest:
                    return FilterTree(ReplicationFilterTreeFormat.Parser.ParseFrom(bytes));
                default: throw new NotSupportedException($"{nameof(ReplicationFilterSerializer)} cannot deserialize object described by manifest '{manifest}'.");
            }
        }

        internal ReplicationFilter FilterTree(ReplicationFilterTreeFormat format)
        {
            switch (format.NodeType)
            {
                case ReplicationFilterTreeFormat.Types.NodeType.Leaf: return (ReplicationFilter)payloadSerializer.Payload(format.Filter);
                case ReplicationFilterTreeFormat.Types.NodeType.And: return new AndFilter(format.Children.Select(FilterTree).ToArray());
                case ReplicationFilterTreeFormat.Types.NodeType.Or: return new OrFilter(format.Children.Select(FilterTree).ToArray());
                default: throw new NotSupportedException($"{nameof(ReplicationFilterSerializer)} cannot deserialize replication filter with node type '{format.NodeType}'");
            }
        }
    }
}