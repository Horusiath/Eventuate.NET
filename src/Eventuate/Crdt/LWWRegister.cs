using Akka.Actor;
using Akka.Streams.Util;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Eventuate.Crdt
{
    /// <summary>
    /// Operation-based LWW-Register CRDT with an <see cref="LWWRegister"/>-based implementation. Instead of returning multiple
    /// values in case of concurrent assignments, the last written value is returned. The last written value is
    /// determined by comparing the following <see cref="Versioned{T}"/> fields in given order:
    /// 
    ///  - `vectorTimestamp`: if causally related, return the value with the higher timestamp, otherwise compare
    ///  - `systemTimestamp`: if not equal, return the value with the higher timestamp, otherwise compare
    ///  - `emitterId`
    /// 
    /// Note that this relies on synchronized system clocks. <see cref="LWWRegister{T}"/> should only be used when the choice of
    /// value is not important for concurrent updates occurring within the clock skew.
    /// </summary>
    public sealed class LWWRegister<T> : ISerializable
    {
        public static readonly LWWRegister<T> Empty = new LWWRegister<T>();
        private readonly MVRegister<T> inner;

        public LWWRegister() : this(MVRegister<T>.Empty)
        {
        }

        public LWWRegister(MVRegister<T> inner)
        {
            this.inner = inner;
        }

        public Option<T> Value
        {
            get
            {
                using (var enumerator = inner.Versions.GetEnumerator())
                {
                    if (!enumerator.MoveNext()) return Option<T>.None;

                    var result = enumerator.Current;
                    while (enumerator.MoveNext())
                    {
                        var curr = enumerator.Current;
                        var cmp = result.SystemTimestamp.CompareTo(curr.SystemTimestamp);
                        if (cmp < 0 || (cmp == 0 && string.CompareOrdinal(result.Creator, curr.Creator) < 0))
                            result = curr;
                    }

                    return result.Value;
                }
            }
        }

        /// <summary>
        /// Assigns a <see cref="Versioned{T}"/> value and <paramref name="vectorTimestamp"/> and returns an updated MV-Register.
        /// </summary>
        /// <param name="value">A value to assign.</param>
        /// <param name="vectorTimestamp">A vector timestamp of the value to assign.</param>
        /// <param name="systemTimestamp">A system timestamp of the value to assign.</param>
        /// <param name="emitterId">An id of the value emitter.</param>
        public LWWRegister<T> Assign(T value, VectorTime vectorTimestamp, DateTime? systemTimestamp = null, string emitterId = null) =>
            new LWWRegister<T>(inner.Assign(value, vectorTimestamp, systemTimestamp, emitterId));

        public readonly struct Operations : ICrdtOperations<LWWRegister<T>, Option<T>>
        {
            public LWWRegister<T> Zero
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => LWWRegister<T>.Empty;
            }

            public bool Precondition
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => false;
            }

            public LWWRegister<T> Effect(LWWRegister<T> crdt, object operation, DurableEvent e)
            {
                var op = (AssignOp)operation;
                return crdt.Assign((T)op.Value, e.VectorTimestamp, e.SystemTimestamp, e.EmitterId);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public object Prepare(LWWRegister<T> crdt, object operation) => operation;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public Option<T> Value(LWWRegister<T> crdt) => crdt.Value;
        }
    }

    /// <summary>
    /// Replicated <see cref="LWWRegister{T}"/> CRDT service.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public sealed class LWWRegisterService<T> : CrdtService<LWWRegister<T>, Option<T>, LWWRegister<T>.Operations>
    {
        public LWWRegisterService(ActorSystem system, string serviceId, IActorRef eventLog)
        {
            System = system;
            ServiceId = serviceId;
            EventLog = eventLog;

            this.Start();
        }

        public override ActorSystem System { get; }

        public override string ServiceId { get; }

        public override IActorRef EventLog { get; }

        public Task<Option<T>> Assign(string id, T value) => Update(id, new AssignOp(value));
    }
}
