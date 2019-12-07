using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Akka.TestKit;
using Eventuate.EventsourcingProtocol;
using Akka.Actor;
using Akka.TestKit.Xunit2;
using Eventuate.ReplicationProtocol;

namespace Eventuate.Rocks.Tests
{
    internal static class Utils
    {
        public static void Write(ReplicationTarget target, IEnumerable<string> events, string? aggregateId = null)
        {
            var system = target.Endpoint.System;
            var probe = new TestProbe(system, new XunitAssertions());
            target.Log.Tell(new Write(
                events.Select(e => new DurableEvent(e, target.LogId, emitterAggregateId: aggregateId)).ToArray(),
                system.DeadLetters,
                probe.Ref, 
                0,
                0));
            probe.ExpectMsg<WriteSuccess>();
        }

        public static async Task<IEnumerable<string>> Read(ReplicationTarget target)
        {
            var timeout = TimeSpan.FromSeconds(3);

            Task<ReplicationReadSuccess> ReadEvents() =>
                target.Log.Ask<ReplicationReadSuccess>(new ReplicationRead(1, int.MaxValue, int.MaxValue, null,
                    DurableEvent.UndefinedLogId, target.Endpoint.System.DeadLetters, VectorTime.Zero), timeout);

            var reading = await ReadEvents();
            return reading.Events.Select(e => (string) e.Payload);
        }

        public static async Task<int> Replicate(ReplicationTarget source, ReplicationTarget destination, int num = int.MaxValue)
        {
            var timeout = TimeSpan.FromSeconds(3);

            var rps = await destination.Log.Ask<GetReplicationProgressSuccess>(new GetReplicationProgress(source.LogId), timeout);
            var res = await source.Log.Ask<ReplicationReadSuccess>(
                new ReplicationRead(rps.StoredReplicationProgress + 1, num, int.MaxValue, null, destination.LogId,
                    destination.Endpoint.System.DeadLetters, rps.CurrentTargetVersionVector), timeout);
            var wes = await destination.Log.Ask<ReplicationWriteSuccess>(new ReplicationWrite(res.Events,
                ImmutableDictionary<string, ReplicationMetadata>.Empty.SetItem(source.LogId, new ReplicationMetadata(res.ReplicationProgress, VectorTime.Zero))), timeout);

            return wes.Events.Count;
        }
    }
    
    internal static class Dir
    {
        public static void Copy(string source, string destination)
        {
            var src = new DirectoryInfo(source);
            var dst = new DirectoryInfo(destination);
            
            Copy(src, dst);
        }

        private static void Copy(DirectoryInfo source, DirectoryInfo destination)
        {
            if (!destination.Exists)
            {
                destination.Create();
            }

            foreach (var srcFile in source.EnumerateFiles())
            {
                srcFile.CopyTo(Path.Combine(destination.FullName, srcFile.Name));
            }

            foreach (var subdir in source.EnumerateDirectories())
            {
                var dst = new DirectoryInfo(Path.Combine(destination.FullName, subdir.Name));
                Copy(subdir, dst);
            }
        }
    }
}