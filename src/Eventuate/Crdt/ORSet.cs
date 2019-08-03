#region copyright
// -----------------------------------------------------------------------
//  <copyright file="ORSet.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Akka.Actor;

namespace Eventuate.Crdt
{
    /// <summary>
    /// Operation-based OR-Set CRDT. <see cref="Versioned{T}"/> entries are uniquely identified with vector timestamps.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public sealed class ORSet<T> : ICrdtFormat
    {
        public static readonly ORSet<T> Empty = new ORSet<T>();

        public ORSet() : this(ImmutableHashSet<Versioned<T>>.Empty) { }

        public ORSet(ImmutableHashSet<Versioned<T>> versioned)
        {
            Versioned = versioned;
        }

        public ImmutableHashSet<Versioned<T>> Versioned { get; }

        /// <summary>
        /// Returns all entries, masking duplicates of different version.
        /// </summary>
        public ImmutableHashSet<T> Value => Versioned.Select(v => v.Value).ToImmutableHashSet();

        /// <summary>
        /// Adds a <see cref="Versioned{T}"/> entry from <paramref name="entry"/>, identified by <paramref name="timestamp"/>, 
        /// and returns an updated <see cref="ORSet{T}"/>.
        /// </summary>
        public ORSet<T> Add(T entry, VectorTime timestamp) =>
            new ORSet<T>(Versioned.Add(new Versioned<T>(entry, timestamp)));

        /// <summary>
        /// Collects all timestamps of given <paramref name="entry"/>.
        /// </summary>
        /// <param name="entry"></param>
        /// <returns></returns>
        public ImmutableHashSet<VectorTime> PrepareRemove(T entry)
        {
            var builder = ImmutableHashSet.CreateBuilder<VectorTime>();
            foreach (var v in this.Versioned)
            {
                if (Equals(entry, v.Value))
                    builder.Add(v.VectorTimestamp);
            }
            return builder.ToImmutable();
        }

        /// <summary>
        /// Removes all <see cref="Versioned{T}"/> entries identified by given <paramref name="timestamps"/> 
        /// and returns an updated <see cref="ORSet{T}"/>.
        /// </summary>
        public ORSet<T> Remove(ImmutableHashSet<VectorTime> timestamps) =>
            new ORSet<T>(Versioned.Where(x => !timestamps.Contains(x.VectorTimestamp)).ToImmutableHashSet());

        /// <summary>
        /// Removes all <see cref="Versioned{T}"/> entries identified by given <paramref name="timestamps"/> 
        /// and returns an updated <see cref="ORSet{T}"/>.
        /// </summary>
        public ORSet<T> Remove(params VectorTime[] timestamps) => Remove(timestamps.ToImmutableHashSet());

        public readonly struct Operations : ICrdtOperations<ORSet<T>, ImmutableHashSet<T>>
        {
            public ORSet<T> Zero
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => ORSet<T>.Empty;
            }

            public bool Precondition
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => false;
            }

            public ORSet<T> Effect(ORSet<T> crdt, object operation, DurableEvent e)
            {
                switch (operation)
                {
                    case AddOp add: return crdt.Add((T)add.Entry, e.VectorTimestamp);
                    case RemoveOp rem: return crdt.Remove(rem.Timestamps);
                    default: throw new NotSupportedException($"ORSet doesn't support [{operation.GetType().FullName}] operation.");
                }
            }

            public object Prepare(ORSet<T> crdt, object operation)
            {
                switch (operation)
                {
                    case RemoveOp rem:
                        var timestamps = crdt.PrepareRemove((T)rem.Entry);
                        if (timestamps.IsEmpty)
                            return null;
                        else
                            return new RemoveOp(rem.Entry, timestamps);
                    default: return operation;
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ImmutableHashSet<T> Value(ORSet<T> crdt) => crdt.Value;
        }
    }

    /// <summary>
    /// Replicated <see cref="ORSet{T}"/> CRDT service.
    /// </summary>
    public class ORSetService<T> : CrdtService<ORSet<T>, ImmutableHashSet<T>, ORSet<T>.Operations>
    {
        public ORSetService(ActorSystem system, string serviceId, IActorRef eventLog)
        {
            System = system;
            ServiceId = serviceId;
            EventLog = eventLog;

            this.Start();
        }

        public override ActorSystem System { get; }
        public override string ServiceId { get; }
        public override IActorRef EventLog { get; }

        /// <summary>
        /// Adds <paramref name="entry"/> to the OR-Set identified by <paramref name="id"/> and returns the updated entry set.
        /// </summary>
        public Task<ImmutableHashSet<T>> Add(string id, T entry) => Update(id, new AddOp(entry));

        /// <summary>
        /// Removes <paramref name="entry"/> from the OR-Set identified by <paramref name="id"/> and returns the updated entry set.
        /// </summary>
        public Task<ImmutableHashSet<T>> Remove(string id, T entry) => Update(id, new RemoveOp(entry));
    }

    public readonly struct AddOp : ICrdtFormat
    {
        public AddOp(object entry)
        {
            Entry = entry;
        }

        public object Entry { get; }
    }

    /// <summary>
    /// Persistent remove operation used for <see cref="ORSet{T}"/> and <see cref="ORCart{T}"/>.
    /// </summary>
    public readonly struct RemoveOp : ICrdtFormat
    {
        public RemoveOp(object entry, ImmutableHashSet<VectorTime> timestamps = null)
        {
            Entry = entry;
            Timestamps = timestamps ?? ImmutableHashSet<VectorTime>.Empty;
        }

        public object Entry { get; }
        public ImmutableHashSet<VectorTime> Timestamps { get; }
    }
}
