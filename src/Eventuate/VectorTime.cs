using System;
using System.Collections.Immutable;
using System.Linq;
using System.Runtime.CompilerServices;

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
                var allKeys = left.value.Keys.Union(right.value.Keys);
                foreach (var key in allKeys)
                {
                    var l = left.value.GetValueOrDefault(key, 0);
                    var r = right.value.GetValueOrDefault(key, 0);
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

        private readonly ImmutableDictionary<string, long> value;

        private VectorTime() : this(ImmutableDictionary<string, long>.Empty) { }

        public VectorTime(ImmutableDictionary<string, long> value)
        {
            this.value = value;
        }

        public VectorTime(params (string, long)[] values)
        {
            var builder = ImmutableDictionary.CreateBuilder<string, long>();
            foreach (var (k,v) in values)
            {
                builder[k] = v;
            }
            this.value = builder.ToImmutable();
        }

        /// <summary>
        /// Returns the local time of <paramref name="processId"/>.
        /// </summary>
        public long this[string processId] => value.GetValueOrDefault(processId, 0);

        public bool IsConcurrent(VectorTime other) => !Comparer.PartiallyCompare(this, other).HasValue;

        public VectorTime SetLocalTime(string processId, long localTime) => new VectorTime(this.value.SetItem(processId, localTime));

        public VectorTime LocalCopy(string processId) => new VectorTime((processId, value.GetValueOrDefault(processId, 0)));

        public VectorTime Increment(string processId)
        {
            if (value.TryGetValue(processId, out var time)) return new VectorTime(value.SetItem(processId, time + 1));
            else return new VectorTime((processId, 1));
        }

        public VectorTime Merge(VectorTime other)
        {
            var builder = this.value.ToBuilder();
            foreach (var (processId, timeOther) in other.value)
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
            if (this.value.Count != other.value.Count) return false;

            foreach (var (k1, v1) in value)
            {
                if (!other.value.TryGetValue(k1, out var v2) || v2 != v1) return false;
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
                foreach (var (k, v) in value)
                {
                    hash ^= (397 * k.GetHashCode()) * v.GetHashCode();
                }

                return hash;
            }
        }

        public static bool operator ==(VectorTime left, VectorTime right) => left is null ? right is null : left.Equals(right);
        public static bool operator !=(VectorTime left, VectorTime right) => !(left == right);
        public static bool operator <(VectorTime left, VectorTime right) => Comparer.PartiallyCompare(left, right) == -1;
        public static bool operator >(VectorTime left, VectorTime right) => Comparer.PartiallyCompare(left, right) == 1;
        public static bool operator <=(VectorTime left, VectorTime right) => Comparer.PartiallyCompare(left, right) <= 0;
        public static bool operator >=(VectorTime left, VectorTime right) => Comparer.PartiallyCompare(left, right) >= 0;
    }
}
