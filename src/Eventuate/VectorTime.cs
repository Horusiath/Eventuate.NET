#region copyright
// -----------------------------------------------------------------------
//  <copyright file="VectorTime.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Collections.Immutable;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;

namespace Eventuate
{
    public sealed class VectorTime : IPartiallyComparable<VectorTime>, IEquatable<VectorTime>
    {
        #region comparer

        public struct PartialComparer : IPartialComparer<VectorTime>
        {
            public int? PartiallyCompare(VectorTime left, VectorTime right)
            {
                var result = 0;
                var allKeys = left.Value.Keys.Union(right.Value.Keys);
                foreach (var key in allKeys)
                {
                    var l = left.Value.GetValueOrDefault(key, 0);
                    var r = right.Value.GetValueOrDefault(key, 0);
                    var cmp = l.CompareTo(r);
                    if (cmp == -1) // left is lesser
                    {
                        if (result == 1) return null; // left was greater on another key, return concurrent (null)
                        else result = -1;
                    }
                    else if (cmp == 1) // left is greater
                    {
                        if (result == -1) return null; // left was lesser on another key, return concurrent (null)
                        else result = 1;
                    }
                }
                return result;
            }
        }

        #endregion

        private static PartialComparer Comparer = default;
        public static VectorTime Zero { get; } = new VectorTime();

        public ImmutableDictionary<string, long> Value { get; }

        private VectorTime() : this(ImmutableDictionary<string, long>.Empty) { }

        public VectorTime(ImmutableDictionary<string, long> value)
        {
            this.Value = value;
        }

        public VectorTime(params (string, long)[] values)
        {
            var builder = ImmutableDictionary.CreateBuilder<string, long>();
            foreach (var (k,v) in values)
            {
                builder[k] = v;
            }
            this.Value = builder.ToImmutable();
        }

        /// <summary>
        /// Returns the local time of <paramref name="processId"/>.
        /// </summary>
        public long this[string processId] => Value.GetValueOrDefault(processId, 0);

        public bool IsConcurrent(VectorTime other) => !Comparer.PartiallyCompare(this, other).HasValue;

        public VectorTime SetLocalTime(string processId, long localTime) => new VectorTime(this.Value.SetItem(processId, localTime));

        public VectorTime LocalCopy(string processId) => new VectorTime((processId, Value.GetValueOrDefault(processId, 0)));

        public VectorTime Increment(string processId)
        {
            if (Value.TryGetValue(processId, out var time)) return new VectorTime(Value.SetItem(processId, time + 1));
            else return new VectorTime(Value.SetItem(processId, 1));
        }

        public VectorTime Merge(VectorTime other)
        {
            var builder = this.Value.ToBuilder();
            foreach (var (processId, timeOther) in other.Value)
            {
                if (builder.TryGetValue(processId, out var timeThis))
                {
                    builder[processId] = Math.Max(timeThis, timeOther);
                }
                else builder[processId] = timeOther;
            }

            return new VectorTime(builder.ToImmutable());
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int? PartiallyCompareTo(VectorTime other) => Comparer.PartiallyCompare(this, other);

        public bool Equals(VectorTime other)
        {
            if (ReferenceEquals(this, other)) return true;
            if (ReferenceEquals(other, null)) return false;
            if (this.Value.Count != other.Value.Count) return false;

            foreach (var (k1, v1) in Value)
            {
                if (!other.Value.TryGetValue(k1, out var v2) || v2 != v1) return false;
            }

            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool Equals(object obj) => obj is VectorTime vtime && Equals(vtime);

        public override int GetHashCode()
        {
            unchecked
            {
                var hash = 0;
                foreach (var (k, v) in Value)
                {
                    hash ^= (397 * k.GetHashCode()) * v.GetHashCode();
                }

                return hash;
            }
        }

        public override string ToString()
        {
            var sb = new StringBuilder('{');
            foreach (var (processId, localTime) in this.Value)
            {
                sb.Append(processId).Append(':').Append(localTime).Append(',');
            }
            return sb.Append('}').ToString();
        }

        public static bool operator ==(VectorTime left, VectorTime right) => left is null ? right is null : left.Equals(right);
        public static bool operator !=(VectorTime left, VectorTime right) => !(left == right);
        public static bool operator <(VectorTime left, VectorTime right) => Comparer.PartiallyCompare(left, right) == -1;
        public static bool operator >(VectorTime left, VectorTime right) => Comparer.PartiallyCompare(left, right) == 1;
        public static bool operator <=(VectorTime left, VectorTime right) => Comparer.PartiallyCompare(left, right) <= 0;
        public static bool operator >=(VectorTime left, VectorTime right) => Comparer.PartiallyCompare(left, right) >= 0;
    }
}
