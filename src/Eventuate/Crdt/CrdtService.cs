using System;
using System.Collections.Immutable;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;

namespace Eventuate.Crdt
{
    /// <summary>
    /// Typeclass to be implemented by CRDTs if they shall be managed by <see cref="CrdtService{TCrdt, TValue}"/>
    /// </summary>
    /// <typeparam name="TCrdt"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public interface ICrdtOperations<TCrdt, TValue> 
    {
        /// <summary>
        /// Default <typeparamref name="TCrdt"/> instance.
        /// </summary>
        TCrdt Zero { get; }

        /// <summary>
        /// Returns the CRDT value (for example, the entries of an OR-Set)
        /// </summary>
        TValue Value(TCrdt crdt);

        /// <summary>
        /// Must return `true` if CRDT checks preconditions. Should be overridden to return
        /// `false` if CRDT does not check preconditions, as this will significantly increase
        /// write throughput.
        /// </summary>
        bool Precondition { get; }

        /// <summary>
        /// Update phase 1 ("atSource"). Prepares an operation for phase 2.
        /// </summary>
        object Prepare(TCrdt crdt, object operation);

        /// <summary>
        /// Update phase 2 ("downstream").
        /// </summary>
        TCrdt Effect(TCrdt crdt, object operation, DurableEvent e);
    }

    /// <summary>
    /// Persistent event with update operation.
    /// </summary>
    public readonly struct ValueUpdated
    {
        public ValueUpdated(object operation)
        {
            Operation = operation;
        }

        /// <summary>
        /// An update operation.
        /// </summary>
        public object Operation { get; }
    }

    internal sealed class CrdtServiceSettings
    {
        public CrdtServiceSettings(Config config)
        {
            this.OperationTimeout = config.GetTimeSpan("eventuate.crdt.operation-timeout");
        }

        public TimeSpan OperationTimeout { get; }
    }

    internal interface IIdentified
    {
        string Id { get; }
    }

    internal readonly struct Get : IIdentified
    {
        public Get(string id)
        {
            Id = id;
        }

        public string Id { get; }
    }

    internal readonly struct GetReply<TValue> : IIdentified
    {
        public GetReply(string id, TValue value)
        {
            Id = id;
            Value = value;
        }

        public string Id { get; }
        public TValue Value { get; }
    }

    internal readonly struct Update : IIdentified
    {
        public Update(string id, object operation)
        {
            Id = id;
            Operation = operation;
        }

        public string Id { get; }
        public object Operation { get; }
    }

    internal readonly struct UpdateReply<T> : IIdentified
    {
        public UpdateReply(string id, T value)
        {
            Id = id;
            Value = value;
        }

        public string Id { get; }
        public T Value { get; }
    }

    internal readonly struct Save : IIdentified
    {
        public Save(string id)
        {
            Id = id;
        }

        public string Id { get; }
    }

    internal readonly struct SaveReply : IIdentified
    {
        public SaveReply(string id, SnapshotMetadata metadata)
        {
            Id = id;
            Metadata = metadata;
        }

        public string Id { get; }
        public SnapshotMetadata Metadata { get; }
    }

    internal readonly struct OnChange<T>
    {
        public OnChange(T crdt, object operation)
        {
            Crdt = crdt;
            Operation = operation;
        }

        public T Crdt { get; }
        public object Operation { get; }
    }

    public class ServiceNotStartedException : Exception
    {
        public ServiceNotStartedException(string serviceId) : base($"CRDT service [{serviceId}] has not been started.") { }
    }

    /// <summary>
    /// A generic, replicated CRDT service that manages a map of CRDTs identified by name.
    /// Replication is based on the replicated event `log` that preserves causal ordering
    /// of events.
    /// </summary>
    public abstract class CrdtService<TCrdt, TValue, TOperations> : IDisposable
        where TOperations : struct, ICrdtOperations<TCrdt, TValue>
    {
        private IActorRef manager = null;
        private readonly Lazy<CrdtServiceSettings> settings;

        protected CrdtService()
        {
            this.settings = new Lazy<CrdtServiceSettings>(() => new CrdtServiceSettings(System.Settings.Config));
        }

        /// <summary>
        /// This service's actor system.
        /// </summary>
        public abstract ActorSystem System { get; }

        /// <summary>
        /// CRDT service id.
        /// </summary>
        public abstract string ServiceId { get; }

        /// <summary>
        /// Event log.
        /// </summary>
        public abstract IActorRef EventLog { get; }
        
        /// <summary>
        /// Starts the CRDT service.
        /// </summary>
        public void Start()
        {
            if (manager is null)
                manager = System.ActorOf(Props.Create(() => new CrdtManager(this.ServiceId, this.EventLog)));
        }

        public void Dispose()
        {
            if (!(manager is null))
            {
                System.Stop(manager);
                manager = null;
            }
        }

        /// <summary>
        /// Returns the current value of the CRDT identified by <paramref name="id"/>.
        /// </summary>
        public async Task<TValue> GetValue(string id)
        {
            ThrowIfNotStarted();
            var response = await manager.Ask<GetReply<TValue>>(new Get(id), timeout: settings.Value.OperationTimeout);
            return response.Value;
        }

        /// <summary>
        /// Saves a snapshot of the CRDT identified by <paramref name="id"/>.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public async Task<SnapshotMetadata> Save(string id)
        {
            ThrowIfNotStarted();
            var response = await manager.Ask<SaveReply>(new Save(id), timeout: settings.Value.OperationTimeout);
            return response.Metadata;
        }

        /// <summary>
        /// Updates the CRDT identified by <paramref name="id"/> with given <paramref name="operation"/>.
        /// Returns the updated value of the CRDT.
        /// </summary>
        public async Task<TValue> Update(string id, object operation)
        {
            ThrowIfNotStarted();
            var response = await manager.Ask<UpdateReply<TValue>>(new Update(id, operation), timeout: settings.Value.OperationTimeout);
            return response.Value;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void ThrowIfNotStarted()
        {
            if (manager is null) throw new ServiceNotStartedException(this.ServiceId);
        }

        #region actors

        private sealed class CrdtActor : EventsourcedActor
        {
            private readonly string crdtId;
            private TCrdt crdt;

            public CrdtActor(string serviceId, string crdtId, IActorRef eventLog)
            {
                Id = serviceId + "_" + crdtId;
                EventLog = eventLog;
                this.crdtId = crdtId;
                this.crdt = default(TOperations).Zero;
            }

            public override bool StateSync => default(TOperations).Precondition;
            public override string Id { get; }
            public override IActorRef EventLog { get; }

            protected override bool OnCommand(object message)
            {
                var ops = default(TOperations);
                switch (message)
                {
                    case Get g when g.Id == this.crdtId:
                        Sender.Tell(new GetReply<TValue>(g.Id, ops.Value(crdt)));
                        return true;

                    case Update u when u.Id == this.crdtId:
                        try
                        {
                            var op = ops.Prepare(crdt, u.Operation);
                            if (op is null)
                                Sender.Tell(new UpdateReply<TValue>(crdtId, ops.Value(crdt)));
                            else
                                Persist(new ValueUpdated(op), attempt =>
                                {
                                    if (attempt.IsSuccess) Sender.Tell(new UpdateReply<TValue>(crdtId, ops.Value(crdt)));
                                    else Sender.Tell(new Status.Failure(attempt.Exception));
                                });
                        }
                        catch (Exception err)
                        {
                            Sender.Tell(new Status.Failure(err));
                        }
                        return true;

                    case Save s when s.Id == this.crdtId:
                        Save(crdt, attempt =>
                        {
                            if (attempt.TryGetValue(out var metadata))
                                Sender.Tell(new SaveReply(crdtId, metadata));
                            else
                                Sender.Tell(new Status.Failure(attempt.Exception));
                        });
                        return true;

                    default: return false;
                }
            }

            protected override bool OnEvent(object message)
            {
                if (message is ValueUpdated e)
                {
                    crdt = default(TOperations).Effect(crdt, e.Operation, LastHandledEvent);
                    Context.Parent.Tell(new OnChange<TCrdt>(crdt, e.Operation));

                    return true;
                }
                else return false;
            }

            protected override bool OnSnapshot(object snapshot)
            {
                this.crdt = (TCrdt)snapshot;
                Context.Parent.Tell(new OnChange<TCrdt>(crdt, null));
                return true;
            }
        }

        private sealed class CrdtManager : ReceiveActor
        {
            private readonly string serviceId;
            private readonly IActorRef eventLog;

            public CrdtManager(string serviceId, IActorRef eventLog)
            {
                this.serviceId = serviceId;
                this.eventLog = eventLog;
                Receive<IIdentified>(i => Crdt(i.Id).Forward(i));
            }

            private IActorRef Crdt(string id)
            {
                var name = Uri.EscapeDataString(id);
                var child = Context.Child(name);
                if (Equals(child, ActorRefs.Nobody))
                {
                    child = Context.ActorOf(Props.Create(() => new CrdtActor(this.serviceId, id, this.eventLog)), name: name);
                }

                return child;
            }
        }

        #endregion
    }
}
