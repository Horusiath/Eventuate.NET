#region copyright
// -----------------------------------------------------------------------
//  <copyright file="ORCart.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Akka.Actor;

namespace Eventuate.Crdt
{
    public readonly struct ORCartEntry : ISerializable
    {
        public ORCartEntry(object key, int quantity)
        {
            Key = key;
            Quantity = quantity;
        }

        public object Key { get; }
        public int Quantity { get; }
    }

    /// <summary>
    /// <see cref="ORCart{T}"/> entry.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public readonly struct ORCartEntry<T> : IEquatable<ORCartEntry<T>>
    {
        public ORCartEntry(T key, int quantity)
        {
            Key = key;
            Quantity = quantity;
        }

        /// <summary>
        /// Entry key. Used to identify a product in the shopping cart.
        /// </summary>
        public T Key { get; }

        /// <summary>
        /// Entry quantity.
        /// </summary>
        public int Quantity { get; }

        public bool Equals(ORCartEntry<T> other)
        {
            return Quantity == other.Quantity && EqualityComparer<T>.Default.Equals(Key, other.Key);
        }

        public override bool Equals(object obj) => obj is ORCartEntry<T> entry && Equals(entry);

        public override int GetHashCode()
        {
            unchecked
            {
                return Quantity ^ EqualityComparer<T>.Default.GetHashCode(Key);
            }
        }
    }

    /// <summary>
    /// Operation-based OR-Cart CRDT with an <see cref="ORSet{T}"/>-based implementation. <see cref="Versioned{T}"/> entry values are of
    /// type <see cref="ORCartEntry{T}"/> and are uniquely identified with vector timestamps.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public sealed class ORCart<T> : ISerializable
    {
        public static readonly ORCart<T> Empty = new ORCart<T>();
        private readonly ORSet<ORCartEntry<T>> inner;

        public ORCart() : this(ORSet<ORCartEntry<T>>.Empty) { }

        public ORCart(ORSet<ORCartEntry<T>> inner)
        {
            this.inner = inner;
        }

        /// <summary>
        /// Returns the `quantity`s of the contained `key`s, reducing multiple [[ORCartEntry]]s with the same `key` 
        /// by adding their `quantity`s.
        /// </summary>
        public ImmutableDictionary<T, int> Value
        {
            get
            {
                var builder = ImmutableDictionary.CreateBuilder<T, int>();
                foreach (var v in inner.Versioned)
                {
                    var entry = v.Value;
                    if (builder.TryGetValue(entry.Key, out var quantity))
                    {
                        builder[entry.Key] = entry.Quantity + quantity;
                    }
                    else builder[entry.Key] = entry.Quantity;
                }

                return builder.ToImmutable();
            }
        }

        /// <summary>
        /// Adds the given <paramref name="quantity"/> of <paramref name="key"/>, 
        /// uniquely identified by <paramref name="timestamp"/>, and returns an updated <see cref="ORCart{T}"/>.
        /// </summary>
        public ORCart<T> Add(T key, int quantity, VectorTime timestamp) =>
            new ORCart<T>(inner.Add(new ORCartEntry<T>(key, quantity), timestamp));

        /// <summary>
        /// Collects all timestamps of given <paramref name="key"/>.
        /// </summary>
        public ImmutableHashSet<VectorTime> PrepareRemove(T key)
        {
            var builder = ImmutableHashSet.CreateBuilder<VectorTime>();
            foreach (var v in inner.Versioned)
            {
                if (EqualityComparer<T>.Default.Equals(v.Value.Key, key))
                    builder.Add(v.VectorTimestamp);
            }
            return builder.ToImmutable();
        }

        /// <summary>
        /// Removes all <see cref="ORCartEntry"/>s identified by given <paramref name="timestamps"/> and returns an updated <see cref="ORCart{T}"/>.
        /// </summary>
        public ORCart<T> Remove(ImmutableHashSet<VectorTime> timestamps) =>
            new ORCart<T>(inner.Remove(timestamps));
        
        /// <summary>
        /// Removes all <see cref="ORCartEntry"/>s identified by given <paramref name="timestamps"/> and returns an updated <see cref="ORCart{T}"/>.
        /// </summary>
        public ORCart<T> Remove(params VectorTime[] timestamps) => new ORCart<T>(inner.Remove(timestamps));

        public readonly struct Operations : ICrdtOperations<ORCart<T>, ImmutableDictionary<T, int>>
        {
            public ORCart<T> Zero
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => ORCart<T>.Empty;
            }

            public bool Precondition
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => false;
            }

            public ORCart<T> Effect(ORCart<T> crdt, object operation, DurableEvent e)
            {
                switch (operation)
                {
                    case AddOp add:
                        var entry = (ORCartEntry)add.Entry;
                        return crdt.Add((T)entry.Key, entry.Quantity, e.VectorTimestamp);
                    case RemoveOp rem: return crdt.Remove(rem.Timestamps);
                    default: throw new NotSupportedException($"ORSet doesn't support [{operation.GetType().FullName}] operation.");
                }
            }

            public object Prepare(ORCart<T> crdt, object operation)
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
            public ImmutableDictionary<T, int> Value(ORCart<T> crdt) => crdt.Value;
        }
    }

    /// <summary>
    /// Replicated <see cref="ORCart{T}"/> CRDT service.
    /// 
    ///  - For adding a new `key` of given `quantity` a client should call `add`.
    ///  - For incrementing the `quantity` of an existing `key` a client should call `add`.
    ///  - For decrementing the `quantity` of an existing `key` a client should call `remove`, followed by `add`
    ///    (after `remove` successfully completed).
    ///  - For removing a `key` a client should call `remove`.
    /// </summary>
    public sealed class ORCartService<T> : CrdtService<ORCart<T>, ImmutableDictionary<T, int>, ORCart<T>.Operations>
    {
        public ORCartService(ActorSystem system, string serviceId, IActorRef eventLog)
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
        /// Adds the given <paramref name="quantity"/> of <paramref name="key"/> to the OR-Cart identified 
        /// by <paramref name="id"/> and returns the updated OR-Cart content.
        /// </summary>
        public Task<ImmutableDictionary<T, int>> Add(string id, T key, uint quantity) =>
            Update(id, new AddOp(new ORCartEntry(key, (int)quantity)));

        /// <summary>
        /// Removes the given <paramref name="key"/> from the OR-Cart identified 
        /// by <paramref name="id"/> and returns the updated OR-Cart content.
        /// </summary>
        public Task<ImmutableDictionary<T, int>> Remove(string id, T key) =>
            Update(id, new RemoveOp(key));
    }
}
