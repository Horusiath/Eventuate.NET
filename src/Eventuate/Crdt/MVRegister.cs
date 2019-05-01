using System;
using System.Collections.Immutable;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Akka.Actor;

namespace Eventuate.Crdt
{
    /// <summary>
    /// Operation-based MV-Register CRDT. Has several<see cref="Versioned"/>] values assigned in case of concurrent assignments,
    /// otherwise, a single <see cref="Versioned{T}"/> value. Concurrent assignments can be reduced to a single assignment by
    /// assigning a <see cref="Versioned{T}"/> value with a vector timestamp that is greater than those of the currently assigned
    /// <see cref="Versioned{T}"/> values.
    /// </summary>
    public sealed class MVRegister<T> : ISerializable
    {
        public static readonly MVRegister<T> Empty = new MVRegister<T>();

        public MVRegister() : this(ImmutableHashSet<Versioned<T>>.Empty)
        {
        }

        public MVRegister(ImmutableHashSet<Versioned<T>> versions)
        {
            Versions = versions;
        }

        public ImmutableHashSet<Versioned<T>> Versions { get; }

        public ImmutableHashSet<T> Value
        {
            get
            {
                var builder = ImmutableHashSet.CreateBuilder<T>();
                foreach (var version in Versions)
                {
                    builder.Add(version.Value);
                }
                return builder.ToImmutable();
            }
        }

        /// <summary>
        /// Assigns a <see cref="Versioned{T}"/> value and <paramref name="vectorTimestamp"/> and returns an updated MV-Register.
        /// </summary>
        /// <param name="value">A value to assign.</param>
        /// <param name="vectorTimestamp">A vector timestamp of the value to assign.</param>
        /// <param name="systemTimestamp">A system timestamp of the value to assign.</param>
        /// <param name="emitterId">An id of the value emitter.</param>
        public MVRegister<T> Assign(T value, VectorTime vectorTimestamp, DateTime? systemTimestamp = null, string emitterId = null)
        {
            var builder = ImmutableHashSet.CreateBuilder<Versioned<T>>();
            foreach (var v in this.Versions)
            {
                if (v.VectorTimestamp.IsConcurrent(vectorTimestamp))
                    builder.Add(v);
            }
            builder.Add(new Versioned<T>(value, vectorTimestamp, systemTimestamp, emitterId));
            return new MVRegister<T>(builder.ToImmutable());
        }

        public readonly struct Operations : ICrdtOperations<MVRegister<T>, ImmutableHashSet<T>>
        {
            public MVRegister<T> Zero
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => MVRegister<T>.Empty;
            }

            public bool Precondition
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => false;
            }

            public MVRegister<T> Effect(MVRegister<T> crdt, object operation, DurableEvent e)
            {
                var op = (AssignOp)operation;
                return crdt.Assign((T)op.Value, e.VectorTimestamp, e.SystemTimestamp, e.EmitterId);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public object Prepare(MVRegister<T> crdt, object operation) => operation;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ImmutableHashSet<T> Value(MVRegister<T> crdt) => crdt.Value;
        }
    }

    /// <summary>
    /// Persistent assign operation used for <see cref="MVRegister{T}"/> and <see cref="LWWRegister{T}"/>.
    /// </summary>
    public readonly struct AssignOp : ISerializable
    {
        public AssignOp(object value)
        {
            Value = value;
        }

        public object Value { get; }
    }

    /// <summary>
    /// Replicated <see cref="MVRegister{T}"/> CRDT service.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public sealed class MVRegisterService<T> : CrdtService<MVRegister<T>, ImmutableHashSet<T>, MVRegister<T>.Operations>
    {
        public MVRegisterService(ActorSystem system, string serviceId, IActorRef eventLog)
        {
            System = system;
            ServiceId = serviceId;
            EventLog = eventLog;

            this.Start();
        }

        public override ActorSystem System { get; }

        public override string ServiceId { get; }

        public override IActorRef EventLog { get; }

        public Task<ImmutableHashSet<T>> Assign(string id, T value) => Update(id, new AssignOp(value));
    }
}
