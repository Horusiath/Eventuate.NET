using Akka;
using Akka.Actor;
using Akka.Configuration;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Stage;
using Eventuate.EventsourcingProtocol;
using Eventuate.ReplicationProtocol;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace Eventuate.Streams
{
    internal sealed class DurableEventWriterSettings
    {
        public DurableEventWriterSettings(Config config)
        {
            this.WriteBatchSize = config.GetInt("eventuate.log.write-batch-size");
            this.WriteTimeout = config.GetTimeSpan("eventuate.log.write-timeout");
        }

        public int WriteBatchSize { get; }
        public TimeSpan WriteTimeout { get; }
    }

    /// <summary>
    /// Stream-based alternative to <see cref="EventsourcedActor"/>.
    /// </summary>
    public static class DurableEventWriter
    {
        internal static readonly TimeSpan DefaultWriteTimeout = TimeSpan.FromSeconds(10);

        /// <summary>
        /// Creates an Akka.NET Streams stage that writes input <see cref="DurableEvent"/>s
        /// to <paramref name="eventLog"/> and emits the written <see cref="DurableEvent"/>s. The input <see cref="DurableEvent"/>'s `emitterId`
        /// is set to this writer's <paramref name="id"/>. The `processId`, `localLogId`, `localSequenceNr` and `systemTimestamp` are set by
        /// `eventLog`. The event log also updates the local time of `vectorTimestamp`. All other input <see cref="DurableEvent"/>
        /// fields are written to the event log without modification. Behavior of the writer can be configured with:
        /// 
        ///  - `eventuate.log.write-batch-size`. Maximum size of <see cref="DurableEvent"/> batches written to the event log. Events
        ///  are batched (with [[Flow.batch]]) if they are produced faster than this writer can write events.
        ///  - `eventuate.log.write-timeout`. Timeout for writing events to the event log. A write timeout or another write
        ///  failure causes this stage to fail.
        /// </summary>
        /// <param name="id">A global unique writer id.</param>
        /// <param name="eventLog">Target event log.</param>
        public static Flow<DurableEvent, DurableEvent, NotUsed> Create(string id, IActorRef eventLog, int writeBatchSize = 64, TimeSpan? writeTimeout = null)
        {
            var timeout = writeTimeout ?? DefaultWriteTimeout;
            return EmissionWriter(id, eventLog, writeBatchSize, timeout);
        }

        private static BatchWriter EmissionBatchWriter(string id, IActorRef eventLog, TimeSpan timeout) => async (events) =>
        {
            var updated = new List<DurableEvent>();
            foreach (var e in events)
            {
                var e2 = new DurableEvent(e.Payload, id, e.EmitterAggregateId, e.CustomDestinationAggregateIds, e.SystemTimestamp, e.VectorTimestamp, DurableEvent.UndefinedLogId, e.LocalLogId, e.LocalSequenceNr, e.DeliveryId, e.PersistOnEventSequenceNr, e.PersistOnEventId);
                updated.Add(e2);
            }
            var response = await eventLog.Ask(new Write(updated, null, null, 0, 0), timeout);
            switch (response)
            {
                case WriteSuccess s: return s.Events;
                case WriteFailure f: throw f.Cause;
                default: throw new NotSupportedException($"Response of type [{response.GetType().FullName}] not supported");
            }
        };

        internal static BatchWriter ReplicationBatchWriter(string id, IActorRef eventLog, TimeSpan timeout) => async (events) =>
        {
            var updated = new List<DurableEvent>();
            var metadataBuilder = ImmutableDictionary.CreateBuilder<string, ReplicationMetadata>();
            foreach (var e in events)
            {
                var e2 = new DurableEvent(e.Payload, id, e.EmitterAggregateId, e.CustomDestinationAggregateIds, e.SystemTimestamp, e.VectorTimestamp, DurableEvent.UndefinedLogId, e.LocalLogId, e.LocalSequenceNr, e.DeliveryId, e.PersistOnEventSequenceNr, e.PersistOnEventId);
                updated.Add(e2);
                metadataBuilder[e2.LocalLogId] = new ReplicationMetadata(e2.LocalSequenceNr, VectorTime.Zero);
            }
            var response = await eventLog.Ask(new ReplicationWrite(updated, metadataBuilder.ToImmutable()), timeout);
            switch (response)
            {
                case ReplicationWriteSuccess s: return s.Events;
                case ReplicationWriteFailure f: throw f.Cause;
                default: throw new NotSupportedException($"Response of type [{response.GetType().FullName}] not supported");
            }
        };

        private static Flow<DurableEvent, DurableEvent, NotUsed> EmissionWriter(string id, IActorRef eventLog, int batchSize, TimeSpan timeout) =>
            Flow.Create<DurableEvent>()
                .Batch(batchSize, e => new List<DurableEvent>(batchSize) { e }, (s, e) => { s.Add(e); return s; })
                .Via(new BatchWriteStage(EmissionBatchWriter(id, eventLog, timeout)))
                .SelectMany(events => events);

        //internal static Flow<List<DurableEvent>, DurableEvent, NotUsed> ReplicationWriter(string id, IActorRef eventLog, int batchSize, TimeSpan timeout) =>
        //    Flow.Create<List<DurableEvent>>()
        //        .BatchWeighted(batchSize, e => e.Count, e => e, (s, e) => { s.AddRange(e); return s; })
        //        .Via(new BatchWriteStage(ReplicationBatchWriter(id, eventLog, timeout)))
        //        .SelectMany(events => events);
    }

    internal delegate Task<IEnumerable<DurableEvent>> BatchWriter(IEnumerable<DurableEvent> events);

    internal sealed class BatchWriteStage : GraphStage<FlowShape<List<DurableEvent>, IEnumerable<DurableEvent>>>
    {
        private readonly Inlet<List<DurableEvent>> inlet = new Inlet<List<DurableEvent>>("batchWrite.in");
        private readonly Outlet<IEnumerable<DurableEvent>> outlet = new Outlet<IEnumerable<DurableEvent>>("batchWrite.out");
        private readonly BatchWriter batchWriter;

        public BatchWriteStage(BatchWriter batchWriter)
        {
            this.Shape = new FlowShape<List<DurableEvent>, IEnumerable<DurableEvent>>(this.inlet, this.outlet);
            this.batchWriter = batchWriter;
        }

        public override FlowShape<List<DurableEvent>, IEnumerable<DurableEvent>> Shape { get; }

        protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes) => new Logic(this);

        public sealed class Logic : InAndOutGraphStageLogic
        {
            private readonly Inlet<List<DurableEvent>> inlet;
            private readonly Outlet<IEnumerable<DurableEvent>> outlet;
            private readonly BatchWriter batchWriter;
            private readonly Action<Try<IEnumerable<DurableEvent>>> callback;
            private bool writing = false;
            private bool finished = false;

            public Logic(BatchWriteStage stage) : base(stage.Shape)
            {
                this.inlet = stage.inlet;
                this.outlet = stage.outlet;
                this.batchWriter = stage.batchWriter;
                this.callback = GetAsyncCallback<Try<IEnumerable<DurableEvent>>>(attempt =>
                {
                    if (attempt.TryGetValue(out var events))
                    {
                        writing = false;
                        Push(outlet, events);
                        if (finished)
                            CompleteStage();
                    }
                    else FailStage(attempt.Exception);
                });

                SetHandler(inlet, this);
                SetHandler(outlet, this);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public override void OnPull() => Pull(inlet);

            public override void OnPush()
            {
                var events = Grab(inlet);
                batchWriter(events).ContinueWith(t => callback(t.AsTry()));
                writing = true;
            }

            public override void OnUpstreamFinish()
            {
                if (writing)
                {
                    // defer stage completion
                    finished = true;
                }
                else base.OnUpstreamFinish();
            }
        }
    }
}
