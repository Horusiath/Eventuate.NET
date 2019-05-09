using Akka.Actor;
using Akka.Configuration;
using Eventuate.EventsourcingProtocol;
using Eventuate.ReplicationProtocol;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Runtime.CompilerServices;

namespace Eventuate.EventLogs
{
    /// <summary>
    /// An event log wrapper that batches write commands. Batched <see cref="Write"/> commands are sent as
    /// <see cref="WriteMany"/> batch to the wrapped event log. Batched <see cref="ReplicationWrite"/>
    /// commands are sent as <see cref="ReplicationWriteMany"/> batch to the wrapped event event log.
    /// 
    /// Batch sizes dynamically increase to a configurable limit under increasing load. The batch size limit can be
    /// configured with `eventuate.log.write-batch-size`. If there is no current write operation in progress, a new
    /// <see cref="Write"/> or <see cref="ReplicationWrite"/> command is served immediately (as <see cref="WriteMany"/> 
    /// or <see cref="ReplicationWriteMany"/> batch of size 1, respectively), keeping latency at a minimum.
    /// </summary>
    public sealed class BatchingLayer : ActorBase
    {
        private readonly IActorRef eventLog;
        private readonly IActorRef defaultBatcher;
        private readonly IActorRef replicationBatcher;

        /// <summary>
        /// Creates a new actor instance of <see cref="BatchingLayer"/>.
        /// </summary>
        /// <param name="logProps">
        /// Configuration object of the wrapped event log actor. The wrapped event log actor is
        /// created as child actor of this wrapper.
        /// </param>
        public BatchingLayer(Props logProps)
        {
            this.eventLog = Context.Watch(Context.ActorOf(logProps));
            this.defaultBatcher = Context.ActorOf(Props.Create(() => new DefaultBatcher(this.eventLog)));
            this.replicationBatcher = Context.ActorOf(Props.Create(() => new ReplicationBatcher(this.eventLog)));
        }

        protected override bool Receive(object message)
        {
            switch (message)
            {
                case ReplicationWrite _:
                    replicationBatcher.Forward(message);
                    return true;
                case ReplicationRead _:
                    replicationBatcher.Forward(message);
                    return true;
                case ServiceEvent _:
                    Context.Parent.Tell(message);
                    return true;
                case Terminated _:
                    Context.Stop(Self);
                    return true;    
                default:
                    defaultBatcher.Forward(message);
                    return true;
            }
        }
    }

    public sealed class BatchingSettings
    {
        public BatchingSettings(Config config)
        {
            this.WriteBatchSize = config.GetInt("eventuate.log.write-batch-size");
        }

        public int WriteBatchSize { get; }
    }

    internal abstract class Batcher<T> : ActorBase
        where T : IDurableEventBatch
    {
        protected readonly BatchingSettings settings;
        protected ImmutableArray<T> batch;
        protected IActorRef eventLog;

        protected Batcher(IActorRef eventLog)
        {
            this.eventLog = eventLog;
            this.settings = new BatchingSettings(Context.System.Settings.Config);
            this.batch = ImmutableArray<T>.Empty;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected sealed override bool Receive(object message) => Idle(message);
        protected sealed override void Unhandled(object message) => eventLog.Forward(message);

        protected void WriteAll()
        {
            while (WriteBatch()) ;
        }

        protected bool WriteBatch()
        {
            if (this.batch.Length == 0) return false;

            var num = 0;
            var toWrite = new List<T>();

            var buff = ImmutableArray.CreateBuilder<T>(batch.Length);
            int i = 0;
            for (; i < batch.Length; i++)
            {
                var w = batch[i];
                num += w.Count;
                if (num <= settings.WriteBatchSize || num == w.Count)
                    toWrite.Add(w);
                else buff.Add(w);
            }

            batch = buff.ToImmutable();

            eventLog.Tell(WriteRequest(toWrite));
            return batch.Length != 0;
        }

        protected abstract object WriteRequest(IReadOnlyCollection<T> batches);
        protected abstract bool Idle(object message);
    }

    internal sealed class DefaultBatcher : Batcher<Write>
    {
        public DefaultBatcher(IActorRef eventLog) : base(eventLog)
        {
        }

        protected override bool Idle(object message)
        {
            if (message is Write write)
            {
                batch = batch.Add(write.WithReplyToDefault(Sender));
                WriteBatch();
                Context.Become(Writing);

                return true;
            }
            return false;
        }

        private bool Writing(object message)
        {
            switch (message)
            {
                case Write w:
                    batch = batch.Add(w.WithReplyToDefault(Sender));
                    return true;
                case WriteManyComplete _:
                    if (batch.IsEmpty)
                        Context.Become(Idle);
                    else
                        WriteBatch();
                    return true;
                case Replay r:
                    // ----------------------------------------------------------------------------
                    // Ensures that Replay commands are properly ordered relative to Write commands
                    // TODO: only force a writeAll() for replays that come from EventsourcedActors
                    // ----------------------------------------------------------------------------
                    WriteAll();
                    eventLog.Forward(r);
                    Context.Become(Idle);
                    return true;
                default: return false;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected override object WriteRequest(IReadOnlyCollection<Write> batches) => new WriteMany(batches);
    }

    internal sealed class ReplicationBatcher : Batcher<ReplicationWrite>
    {
        public ReplicationBatcher(IActorRef eventLog) : base(eventLog)
        {
        }

        protected override bool Idle(object message)
        {
            if (message is ReplicationWrite write)
            {
                batch = batch.Add(write.WithReplyToDefault(Sender));
                WriteBatch();
                Context.Become(Writing);

                return true;
            }
            else return false;
        }

        private bool Writing(object message)
        {
            switch (message)
            {
                case ReplicationWrite w:
                    batch = batch.Add(w.WithReplyToDefault(Sender));
                    return true;
                case ReplicationWriteManyComplete _:
                    if (batch.IsEmpty)
                        Context.Become(Idle);
                    else
                        WriteBatch();
                    return true;
                default: return false;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected override object WriteRequest(IReadOnlyCollection<ReplicationWrite> batches) =>
            new ReplicationWriteMany(batches);
    }
}
