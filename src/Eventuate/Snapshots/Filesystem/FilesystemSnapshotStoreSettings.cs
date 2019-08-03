#region copyright
// -----------------------------------------------------------------------
//  <copyright file="FilesystemSnapshotStoreSettings.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using Akka.Actor;
using Akka.Dispatch;
using Akka.Serialization;

namespace Eventuate.Snapshots.Filesystem
{
    /// <summary>
    /// <seealso cref="FilesystemSnapshotStore"/> configuration object.
    /// </summary>
    public sealed class FilesystemSnapshotStoreSettings
    {
        public FilesystemSnapshotStoreSettings(ActorSystem system)
        {
            this.ReadDispatcher = system.Dispatchers.Lookup("eventuate.snapshot.filesystem.read-dispatcher");
            this.WriteDispatcher = system.Dispatchers.Lookup("eventuate.snapshot.filesystem.write-dispatcher");
            this.Serialization = system.Serialization;

            var config = system.Settings.Config.GetConfig("eventuate.snapshot.filesystem");
            this.RootDirectory = config.GetString("dir");
            this.SnapshotsPerEmitterMax = config.GetInt("snapshots-per-emitter-max");
        }

        public int SnapshotsPerEmitterMax { get; set; }

        public string RootDirectory { get; set; }

        public Akka.Serialization.Serialization Serialization { get; set; }

        public MessageDispatcher WriteDispatcher { get; set; }

        public MessageDispatcher ReadDispatcher { get; set; }
    }
}