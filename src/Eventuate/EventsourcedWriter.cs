using Akka.Actor;
using Akka.Configuration;
using Eventuate.EventsourcingProtocol;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Eventuate.EventsourcedWriter;

namespace Eventuate
{
    internal sealed class EventsourcedWriterSettings
    {
        public EventsourcedWriterSettings(Config config)
        {
            this.ReplayRetryMax = config.GetInt("eventuate.log.replay-retry-max");
        }

        public int ReplayRetryMax { get; }
    }

    /// <summary>
    /// Thrown by <see cref="EventsourcedWriter{TRead, TWrite}"/> to indicate that a read operation from an external database failed.
    /// </summary>
    public class EventsourcedWriterReadException : Exception
    {
        public EventsourcedWriterReadException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Thrown by <see cref="EventsourcedWriter{TRead, TWrite}"/> to indicate that a write operation to the external database failed.
    /// </summary>
    public class EventsourcedWriterWriteException : Exception
    {
        public EventsourcedWriterWriteException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }

    public static class EventsourcedWriter
    {

        internal readonly struct ReadSuccess<TRead>
        {
            public ReadSuccess(TRead result, int instanceId)
            {
                Result = result;
                InstanceId = instanceId;
            }

            public TRead Result { get; }
            public int InstanceId { get; }
        }
        internal readonly struct ReadFailure
        {
            public ReadFailure(Exception cause, int instanceId)
            {
                Cause = cause;
                InstanceId = instanceId;
            }

            public Exception Cause { get; }
            public int InstanceId { get; }
        }

        internal readonly struct WriteSuccess<TWrite>
        {
            public WriteSuccess(TWrite result, int instanceId)
            {
                Result = result;
                InstanceId = instanceId;
            }

            public TWrite Result { get; }
            public int InstanceId { get; }
        }
        internal readonly struct WriteFailure
        {
            public WriteFailure(Exception cause, int instanceId)
            {
                Cause = cause;
                InstanceId = instanceId;
            }

            public Exception Cause { get; }
            public int InstanceId { get; }
        }

    }

    /// <summary>
    /// An <see cref="EventsourcedView"/> designed to update external databases from events stored in its event log. It
    /// supports event processing patterns optimized for batch-updating external databases to create persistent
    /// views or read models:
    /// 
    ///  - During initialization, a concrete <see cref="EventsourcedWriter{TRead, TWrite}"/> asynchronously `read`s data from the external
    ///    database to obtain information about the actual event processing progress. For example, if the last
    ///    processed event sequence number is written with every batch update to the database, it can be read
    ///    during initialization and used by the writer to detect duplicates during further event processing so
    ///    that event processing can be made idempotent.
    /// 
    ///  - During event processing in its `onEvent` handler, a concrete writer usually builds a database-specific
    ///    write-batch (representing an incremental update). After a configurable number of events, `EventsourcedWriter`
    ///    calls `write` to asynchronously write the prepared batch to the database.
    /// 
    /// An <see cref="EventsourcedWriter{TRead, TWrite}"/> may also implement an `onCommand` handler to process commands and save snapshots of
    /// internal state. Internal state is recovered by replaying events from the event log, optionally starting from
    /// a saved snapshot (see [[EventsourcedView]] for details). If a writer doesn't require full internal state
    /// recovery, it may define a custom starting position in the event log by returning a sequence number from
    /// `readSuccess`. If full internal state recovery is required instead, `readSuccess` should return `None`
    /// (which is the default).
    /// 
    /// Implementation notes:
    /// 
    ///  - After having started an asynchronous write but before returning from `write`, a writer should clear the
    ///    prepared write batch so that further events can be processed while the asynchronous write operation is
    ///    in progress.
    ///  - Event processing during replay (either starting from a default or application-defined position) is subject
    ///    to backpressure. After a configurable number of events (see `eventuate.log.replay-batch-size` configuration
    ///    parameter), replay is suspended until these events have been written to the target database and then resumed
    ///    again. There's no backpressure mechanism for live event processing yet (but will come in future releases).
    /// </summary>
    /// <seealso cref="EventsourcedProcessor"/>
    /// <seealso cref="StatefulProcessor"/>
    /// <typeparam name="TRead">Result type of the asynchronous read operation.</typeparam>
    /// <typeparam name="TWrite">Result type of the asynchronous write operations.</typeparam>
    public abstract class EventsourcedWriter<TRead, TWrite> : EventsourcedView
    {
        private readonly EventsourcedWriterSettings settings;
        private int numPending = 0;

        protected EventsourcedWriter()
        {
            this.settings = new EventsourcedWriterSettings(Context.System.Settings.Config);
        }

        /// <summary>
        /// Disallow for <see cref="EventsourcedWriter{TRead, TWrite}"/> and subclasses as event processing progress is determined by 
        /// <see cref="Read"/> and `readSuccess`.
        /// </summary>
        protected override long? ReplayFromSequenceNr => null;

        /// <summary>
        /// Asynchronously reads an initial value from the target database, usually to obtain information about
        /// event processing progress. This method is called during initialization.
        /// </summary>
        public abstract Task<TRead> Read();

        /// <summary>
        /// Asynchronously writes an incremental update to the target database. Incremental updates are prepared
        /// during event processing by a concrete <see cref="EventsourcedView.OnEvent(object)"/> handler.
        /// 
        /// During event replay, this method is called latest after having replayed `eventuate.log.replay-batch-size`
        /// events and immediately after replay completes. During live processing, <see cref="Write"/> is called immediately if
        /// no write operation is in progress and an event has been handled by <see cref="EventsourcedView.OnEvent(object)"/>. 
        /// If a write operation is in progress, further event handling may run concurrently to that operation. If events 
        /// are handled while a write operation is in progress, another write will follow immediately after the previous 
        /// write operation completes.
        /// </summary>
        public abstract Task<TWrite> Write();

        /// <summary>
        /// Called with a read result after a <see cref="Read"/> operation successfully completes. This method may update
        /// internal actor state. If null is returned, the writer continues with state recovery by replaying
        /// events, optionally starting from a snapshot. If the return value is defined, replay starts from the
        /// returned sequence number without ever loading a snapshot. Does nothing and returns null by default
        /// and can be overridden.
        /// </summary>
        public virtual long? ReadSuccess(TRead result) => ReplayFromSequenceNr;

        /// <summary>
        /// Called with a write result after a <see cref="Write"/> operation successfully completes. This method may update
        /// internal actor state. Does nothing by default and can be overridden.
        /// </summary>
        public virtual void WriteSuccess(TWrite write) { }

        /// <summary>
        /// Called with failure details after a <see cref="Read"/> operation failed. Throws <see cref="EventsourcedWriterReadException"/>
        /// by default (causing the writer to restart) and can be overridden.
        /// </summary>
        public virtual void ReadFailure(Exception cause) => throw new EventsourcedWriterReadException("Read failed", cause);

        /// <summary>
        /// Called with failure details after a <see cref="Write"/> operation failed. Throws <see cref="EventsourcedWriterWriteException"/>
        /// by default (causing the writer to restart) and can be overridden.
        /// </summary>
        public virtual void WriteFailure(Exception cause) => throw new EventsourcedWriterWriteException("Write failed", cause);

        internal override void ReceiveEventInternal(DurableEvent e)
        {
            base.ReceiveEventInternal(e);
            numPending++;
        }

        internal override void Initialize()
        {
            Read().PipeTo(Self,
                success: r => new ReadSuccess<TRead>(r, InstanceId),
                failure: e => new ReadFailure(e, InstanceId));
        }

        internal override Receive Initiating(int replayAttempts) => message =>
        {
            switch (message)
            {
                case EventsourcedWriter.ReadSuccess<TRead> s:
                    if (s.InstanceId == InstanceId)
                    {
                        var result = ReadSuccess(s.Result);
                        if (result.HasValue) Replay(result.Value, subscribe: true);
                        else Load();
                    }
                    return true;

                case EventsourcedWriter.ReadFailure f:
                    if (f.InstanceId == InstanceId)
                        ReadFailure(f.Cause);
                    return true;

                case ReplaySuccess rs:
                    if (rs.InstanceId == InstanceId)
                    {
                        var i = 0;
                        foreach (var e in rs.Events)
                        {
                            ReceiveEvent(e);
                            i++;
                        }

                        if (i == 0)
                        {
                            Context.Become(Initiated);
                            VersionChanged(CurrentVersion);
                            Recovered();
                            UnstashAll();
                        }
                        else
                        {
                            if (numPending > 0)
                            {
                                var l = InitiatingWrite(rs.ReplayProgress);
                                var r = Initiating(settings.ReplayRetryMax);
                                Context.Become(m => l(m) || r(m));
                                Write(InstanceId);
                            }
                            else
                            {
                                Replay(rs.ReplayProgress + 1);
                            }
                        }
                    }
                    return true;

                default: return base.Initiating(replayAttempts)(message);
            }
        };

        internal override bool Initiated(object message)
        {
            if (message is Written w)
            {
                ReceiveEvent(w.Event);
                if (numPending > 0)
                {
                    Context.Become(m => InitiatedWrite(m) || Initiated(m));
                    Write(InstanceId);
                }
                return true;
            }
            else return base.Initiated(message);
        }

        private Receive InitiatingWrite(long progress) => message =>
        {
            switch (message)
            {
                case EventsourcedWriter.WriteSuccess<TWrite> s:
                    if (s.InstanceId == InstanceId)
                    {
                        WriteSuccess(s.Result);
                        Context.Become(Initiating(settings.ReplayRetryMax));
                        Replay(progress + 1);
                    }
                    return true;

                case EventsourcedWriter.WriteFailure f:
                    if (f.InstanceId == InstanceId)
                        WriteFailure(f.Cause);
                    return true;

                default: return false;
            }
        };

        private bool InitiatedWrite(object message)
        {
            switch (message)
            {
                case Written w:
                    if (w.Event.LocalSequenceNr > LastSequenceNr)
                        ReceiveEvent(w.Event);
                    return true;

                case EventsourcedWriter.WriteSuccess<TWrite> s:
                    if (s.InstanceId == InstanceId)
                    {
                        WriteSuccess(s.Result);
                        if (numPending > 0)
                            Write(InstanceId);
                        else
                            Context.Become(Initiated);
                    }
                    return true;

                case EventsourcedWriter.WriteFailure f:
                    if (f.InstanceId == InstanceId)
                        WriteFailure(f.Cause);
                    return true;

                default: return false;
            }
        }

        private void Write(int instanceId)
        {
            Write().PipeTo(Self,
                success: r => new WriteSuccess<TWrite>(r, instanceId),
                failure: e => new EventsourcedWriter.WriteFailure(e, instanceId));

            numPending = 0;
        }
    }
}
