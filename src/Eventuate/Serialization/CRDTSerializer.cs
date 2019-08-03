#region copyright
// -----------------------------------------------------------------------
//  <copyright file="CRDTSerializer.cs" company="Akka.NET Project">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Reflection;
using Akka.Actor;
using Akka.Serialization;
using Eventuate.Crdt;
using Eventuate.Serialization.Proto;
using Google.Protobuf;

namespace Eventuate.Serialization
{
    public sealed class CRDTSerializer : SerializerWithStringManifest
    {
        public const string MVRegisterManifest = "A";
        public const string LWWRegisterManifest = "B";
        public const string ORSetManifest = "C";
        public const string ORCartManifest = "D";
        public const string ORCartEntryManifest = "E";
        public const string ValueUpdatedManifest = "F";
        public const string UpdatedOpManifest = "G";
        public const string AssignOpManifest = "H";
        public const string AddOpManifest = "I";
        public const string RemoveOpManifest = "J";
        
        private static readonly Type MVRegisterClass = typeof(MVRegister<>);
        private static readonly Type LWWRegisterClass = typeof(LWWRegister<>);
        private static readonly Type ORSetClass = typeof(ORSet<>);
        private static readonly Type ORCartClass = typeof(ORCart<>);

        private static readonly MethodInfo MVRegisterDeserializeHandle;
        private static readonly MethodInfo LWWRegisterDeserializeHandle;
        private static readonly MethodInfo ORSetDeserializeHandle;
        private static readonly MethodInfo ORCartDeserializeHandle;
        
        private readonly CommonSerializer commonSerializer;

        public override int Identifier { get; } = 22567;

        static CRDTSerializer()
        {
            var type = typeof(CRDTSerializer);
            MVRegisterDeserializeHandle = type.GetMethod(nameof(DeserializeMVRegister), BindingFlags.NonPublic);
            LWWRegisterDeserializeHandle = type.GetMethod(nameof(DeserializeLWWRegister), BindingFlags.NonPublic);
            ORSetDeserializeHandle = type.GetMethod(nameof(DeserializeORSet), BindingFlags.NonPublic);
            ORCartDeserializeHandle = type.GetMethod(nameof(DeserializeORCart), BindingFlags.NonPublic);
        }
        
        public CRDTSerializer(ExtendedActorSystem system) : base(system)
        {
            this.commonSerializer = new CommonSerializer(system);
        }

        public override string Manifest(object o)
        {
            switch (o)
            {
                case RemoveOp _: return RemoveOpManifest;
                case AddOp _: return AddOpManifest;
                case AssignOp _: return AssignOpManifest;
                case Counter.UpdateOp _: return UpdatedOpManifest;
                case ValueUpdated _: return ValueUpdatedManifest;
                case ORCartEntry _: return ORCartEntryManifest;
                default:
                    var type = o.GetType();
                    if (type.IsGenericType)
                    {
                        var genericType = type.GetGenericTypeDefinition();
                        if (genericType == ORSetClass) return ORSetManifest;
                        else if (genericType == ORCartClass) return ORCartManifest;
                        else if (genericType == LWWRegisterClass) return LWWRegisterManifest;
                        else if (genericType == MVRegisterClass) return MVRegisterManifest;
                    }
                    
                    throw new NotSupportedException($"{nameof(CRDTSerializer)} doesn't support serialization of '{type.FullName}'");
            }
        }

        public override byte[] ToBinary(object o)
        {
            switch (o)
            {
                case RemoveOp remove: return Serialize(remove);
                case AddOp add: return Serialize(add);
                case AssignOp assign: return Serialize(assign);
                case Counter.UpdateOp update: return Serialize(update);
                case ValueUpdated vupdated: return Serialize(vupdated);
                case ORCartEntry entry: return Serialize(entry);
                default:
                    dynamic value = o;
                    return Serialize(value);
            }
        }

        private byte[] Serialize<T>(ORSet<T> set)
        {
            var proto = OrSetToProto(set);
            return proto.ToByteArray();
        }

        private ORSetFormat OrSetToProto<T>(ORSet<T> set)
        {
            var proto = new ORSetFormat
            {
                ClassTag = ClassTagAttribute.ValueFor(typeof(T))
            };
            foreach (var versioned in set.Versioned)
            {
                proto.VersionedEntries.Add(commonSerializer.VersionedFormatBuilder(versioned));
            }

            return proto;
        }

        private byte[] Serialize<T>(ORCart<T> cart)
        {
            var proto = new ORCartFormat
            {
                OrSet = OrSetToProto(cart.inner)
            };
            return proto.ToByteArray();
        }
        
        private byte[] Serialize<T>(LWWRegister<T> register)
        {
            return new LWWRegisterFormat { MvRegister = MvRegisterToProto(register.inner) }.ToByteArray();
        }
        
        private byte[] Serialize<T>(MVRegister<T> register)
        {
            var proto = MvRegisterToProto(register);
            return proto.ToByteArray();
        }

        private MVRegisterFormat MvRegisterToProto<T>(MVRegister<T> register)
        {
            var proto = new MVRegisterFormat
            {
                ClassTag = ClassTagAttribute.ValueFor(typeof(T))
            };

            foreach (var version in register.Versions)
            {
                proto.Versioned.Add(commonSerializer.VersionedFormatBuilder(version));
            }

            return proto;
        }

        private byte[] Serialize(in ORCartEntry value)
        {
            var proto = OrCartEntryToProto(value);
            return proto.ToByteArray();
        }

        private ORCartEntryFormat OrCartEntryToProto(ORCartEntry value)
        {
            var proto = new ORCartEntryFormat
            {
                Key = commonSerializer.payloadSerializer.PayloadFormatBuilder(value.Key),
                Quantity = value.Quantity
            };
            return proto;
        }

        private byte[] Serialize(in ValueUpdated value)
        {
            return new ValueUpdatedFormat
            {
                Operation = commonSerializer.payloadSerializer.PayloadFormatBuilder(value.Operation)
            }.ToByteArray();
        }

        private byte[] Serialize(in Counter.UpdateOp update)
        {
            return new UpdateOpFormat
            {
                Delta = commonSerializer.payloadSerializer.PayloadFormatBuilder(update.Delta)
            }.ToByteArray();
        }

        private byte[] Serialize(in AssignOp assign)
        {
            return new AssignOpFormat
            {
                Value = commonSerializer.payloadSerializer.PayloadFormatBuilder(assign.Value)
            }.ToByteArray();
        }

        private byte[] Serialize(in AddOp remove)
        {
            return new AddOpFormat
            {
                Entry = commonSerializer.payloadSerializer.PayloadFormatBuilder(remove.Entry)
            }.ToByteArray();
        }

        private byte[] Serialize(in RemoveOp remove)
        {
            var proto = new RemoveOpFormat
            {
                Entry = commonSerializer.payloadSerializer.PayloadFormatBuilder(remove.Entry)
            };

            foreach (var timestamp in remove.Timestamps)
                proto.Timestamps.Add(commonSerializer.VectorTimeFormatBuilder(timestamp));

            return proto.ToByteArray();
        }

        public override object FromBinary(byte[] bytes, string manifest)
        {
            switch (manifest)
            {
                case MVRegisterManifest:
                {
                    var proto = MVRegisterFormat.Parser.ParseFrom(bytes);
                    return MVRegisterDeserializeHandle
                        .MakeGenericMethod(ClassTagAttribute.TypeFor(proto.ClassTag))
                        .Invoke(this, new[] {proto});
                }
                case LWWRegisterManifest:
                {
                    var proto = LWWRegisterFormat.Parser.ParseFrom(bytes);
                    return LWWRegisterDeserializeHandle
                        .MakeGenericMethod(ClassTagAttribute.TypeFor(proto.MvRegister.ClassTag))
                        .Invoke(this, new[] {proto});
                }
                case ORSetManifest:
                {
                    var proto = ORSetFormat.Parser.ParseFrom(bytes);
                    return ORSetDeserializeHandle
                        .MakeGenericMethod(ClassTagAttribute.TypeFor(proto.ClassTag))
                        .Invoke(this, new[] {proto});
                }
                case ORCartManifest:
                {
                    var proto = ORCartFormat.Parser.ParseFrom(bytes);
                    return ORCartDeserializeHandle
                        .MakeGenericMethod(ClassTagAttribute.TypeFor(proto.OrSet.ClassTag))
                        .Invoke(this, new[] {proto});
                }
                case ORCartEntryManifest: return DeserializeORCartEntry(ORCartEntryFormat.Parser.ParseFrom(bytes));
                case ValueUpdatedManifest: return DeserializeValueUpdated(ValueUpdatedFormat.Parser.ParseFrom(bytes));
                case UpdatedOpManifest: return DeserializeUpdated(UpdateOpFormat.Parser.ParseFrom(bytes));
                case AssignOpManifest: return DeserializeAssign(AssignOpFormat.Parser.ParseFrom(bytes));
                case AddOpManifest: return DeserializeAdd(AddOpFormat.Parser.ParseFrom(bytes));
                case RemoveOpManifest: return DeserializeRemove(RemoveOpFormat.Parser.ParseFrom(bytes));
                default:
                    throw new NotSupportedException($"{nameof(CRDTSerializer)} doesn't know how to deserialize object from manifest '{manifest}'");
            }
        }

        private object DeserializeRemove(RemoveOpFormat format)
        {
            var timestamps = ImmutableHashSet.CreateBuilder<VectorTime>();
            foreach (var timestamp in format.Timestamps)
            {
                timestamps.Add(commonSerializer.VectorTime(timestamp));
            }
            
            return new RemoveOp(commonSerializer.payloadSerializer.Payload(format.Entry), timestamps.ToImmutable());
        }

        private object DeserializeAdd(AddOpFormat format) => 
            new AddOp(commonSerializer.payloadSerializer.Payload(format.Entry));

        private object DeserializeAssign(AssignOpFormat format) => 
            new AssignOp(commonSerializer.payloadSerializer.Payload(format.Value));

        private object DeserializeUpdated(UpdateOpFormat format) => 
            new Counter.UpdateOp(commonSerializer.payloadSerializer.Payload(format.Delta));

        private object DeserializeValueUpdated(ValueUpdatedFormat format) => 
            new ValueUpdated(commonSerializer.payloadSerializer.Payload(format.Operation));

        private object DeserializeORCartEntry(ORCartEntryFormat format) => 
            new ORCartEntry(commonSerializer.payloadSerializer.Payload(format.Key), format.Quantity);

        private object DeserializeORCart<T>(ORCartFormat format)
        {
            var orSet = ORSetFromProto<ORCartEntry<T>>(format.OrSet);
            return new ORCart<T>(orSet);
        }

        private object DeserializeORSet<T>(ORSetFormat format) => ORSetFromProto<T>(format);

        private ORSet<T> ORSetFromProto<T>(ORSetFormat format)
        {
            var builder = ImmutableHashSet.CreateBuilder<Versioned<T>>();
            foreach (var entry in format.VersionedEntries)
            {
                builder.Add(commonSerializer.Versioned<T>(entry));
            }
            
            return new ORSet<T>(builder.ToImmutable());
        }

        private object DeserializeLWWRegister<T>(LWWRegisterFormat format) => 
            new LWWRegister<T>(MVRegisterFromProto<T>(format.MvRegister));


        private object DeserializeMVRegister<T>(MVRegisterFormat format) => MVRegisterFromProto<T>(format);

        private MVRegister<T> MVRegisterFromProto<T>(MVRegisterFormat format)
        {
            var builder = ImmutableHashSet.CreateBuilder<Versioned<T>>();
            foreach (var vformat in format.Versioned)
            {
                builder.Add(commonSerializer.Versioned<T>(vformat));
            }
            return new MVRegister<T>(builder.ToImmutable());
        }
    }
}