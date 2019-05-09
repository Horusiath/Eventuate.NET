using System;
using System.IO;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Event;

namespace Eventuate.Snapshots.Filesystem
{
    
    /// <summary> 
    /// A snapshot store that saves snapshots to the local filesystem. It keeps a configurable
    /// maximum number of snapshots per event-sourced actor, view, writer or processor. Snapshot
    /// loading falls back to older snapshots if newer snapshots cannot be loaded.
    /// </summary> 
    public class FilesystemSnapshotStore : ISnapshotStore
    {
        private readonly FilesystemSnapshotStoreSettings settings;
        private readonly string logId;
        private readonly ILoggingAdapter logger;
        private readonly DirectoryInfo rootDirectory;

        public FilesystemSnapshotStore(ActorSystem system, string logId)
        {
            this.settings = new FilesystemSnapshotStoreSettings(system);
            this.logId = logId;
            this.logger = system.Log;
            this.rootDirectory = new DirectoryInfo(Path.Combine(this.settings.RootDirectory, Uri.EscapeDataString(logId)));

            if (!this.rootDirectory.Exists)
            {
                this.rootDirectory.Create();
            }
        }

        public async Task Delete(long lowerSequenceNr)
        {
            var seqNrString = $"{lowerSequenceNr:D19}";
            foreach (var emitterDirectory in this.rootDirectory.EnumerateDirectories())
            {
                if (emitterDirectory.Exists)
                {
                    foreach (var snapshotFile in emitterDirectory.EnumerateFiles())
                    {
                        if (String.Compare(snapshotFile.Name, seqNrString, StringComparison.Ordinal) == -1)
                            snapshotFile.Delete();
                    }
                }
            }
        }

        public async Task Save(Snapshot snapshot)
        {
            var seqNrString = $"{snapshot.SequenceNr:D19}";
            var emitterDirectory = Uri.EscapeDataString(snapshot.EmitterId);
            var subdirs = rootDirectory.GetDirectories(emitterDirectory);
            var dir = subdirs.Length == 0 ? rootDirectory.CreateSubdirectory(emitterDirectory) : subdirs[0];
            var tmpSnapshotFile = new FileInfo(Path.Combine(dir.FullName, $"tmp-{seqNrString}"));
            if (!tmpSnapshotFile.Exists)
                tmpSnapshotFile.Create();

            using (var stream = tmpSnapshotFile.OpenWrite())
            {
                await Serialize(snapshot, stream);
            }
            
            tmpSnapshotFile.MoveTo(Path.Combine(dir.FullName, seqNrString));
            
            // do not keep more than the configured maximum number of snapshot files
            var files = dir.GetFiles();
            if (files.Length > settings.SnapshotsPerEmitterMax)
            {
                for (int i = 0; i < files.Length - settings.SnapshotsPerEmitterMax; i++)
                {
                    files[0].Delete();
                }
            }
        }

        public async Task<Snapshot> Load(string emitterId)
        {
            var emitterDirectory = Uri.EscapeDataString(emitterId);
            var subdirs = rootDirectory.GetDirectories(emitterDirectory);
            if (subdirs.Length == 0)
                return null;
            else
            {
                var dir = subdirs[0];
                var files = dir.GetFiles();
                for (int i = files.Length - 1; i >= 0; i++)
                {
                    var file = files[i];
                    if (!file.Name.StartsWith("tmp"))
                    {
                        try
                        {
                            using (var stream = file.OpenRead())
                            {
                                return await Deserialize(stream);
                            }
                        }
                        catch (Exception e)
                        {
                            logger.Warning("Failed to load snapshot from file", file.FullName);
                        }
                    }
                }
            }

            return null;
        }

        private async Task Serialize(Snapshot snapshot, FileStream stream)
        {
            var serializer = this.settings.Serialization.FindSerializerFor(snapshot, "json");
            var bytes = serializer.ToBinary(snapshot);

            await stream.WriteAsync(bytes, 0, bytes.Length);
        }
        
        private async Task<Snapshot> Deserialize(FileStream stream)
        {
            var buffer = new byte[stream.Length];
            await stream.ReadAsync(buffer, 0, buffer.Length);
            var serializer = this.settings.Serialization.FindSerializerForType(typeof(Snapshot));
            return (Snapshot) serializer.FromBinary(buffer, typeof(Snapshot));
        }
    }
}