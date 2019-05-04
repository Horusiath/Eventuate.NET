using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;

namespace Eventuate.Crdt.Pure
{
    public sealed class StabilitySettings
    {
        public StabilitySettings(string localPartition, ImmutableHashSet<string> partitions)
        {
            LocalPartition = localPartition;
            Partitions = partitions;
        }

        public string LocalPartition { get; }
        public ImmutableHashSet<string> Partitions { get; }
    }

    public readonly struct Rtm
    {
        private readonly string localPartition;
        private readonly ImmutableDictionary<string, VectorTime> timestamps;

        public Rtm(string localPartition, ImmutableDictionary<string, VectorTime> timestamps)
        {
            this.localPartition = localPartition;
            this.timestamps = timestamps;
        }

        public Rtm Update(string partition, VectorTime timestamp)
        {
            if (partition is null || partition != this.localPartition) return this;

            if (this.timestamps.TryGetValue(partition, out var oldTimestamp))
                return new Rtm(localPartition, timestamps.SetItem(partition, oldTimestamp.Merge(timestamp)));
            else
                return new Rtm(localPartition, timestamps.SetItem(partition, timestamp));
        }

        public VectorTime Stable
        {
            get
            {
                var builder = ImmutableDictionary.CreateBuilder<string, long>();
                foreach (var entry in this.timestamps)
                {
                    foreach (var (processId, localTime) in entry.Value.Value)
                    {
                        if (builder.TryGetValue(processId, out var localTime2))
                            builder[processId] = Math.Min(localTime, localTime2);
                        else
                            builder[processId] = localTime;
                    }
                }
                return new VectorTime(builder.ToImmutable());
            }
        }
    }

    public static class TCStable
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsStable(this VectorTime stable, VectorTime other) => other <= stable;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsZero(this VectorTime stable) => stable == VectorTime.Zero;
    }
}
