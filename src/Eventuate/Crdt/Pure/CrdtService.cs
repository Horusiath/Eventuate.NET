using Akka.Actor;
using Akka.Configuration;
using System;
using System.Collections.Immutable;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace Eventuate.Crdt.Pure
{
    /// <summary>
    /// Typeclass to be implemented by CRDTs if they shall be managed by <see cref="CrdtService{TCrdt, TValue, TOperations}"/>
    /// </summary>
    /// <typeparam name="TCrdt">CRDT type.</typeparam>
    /// <typeparam name="TValue">CRDT returned value type.</typeparam>
    public interface IPureCrdtOperations<TCrdt, TValue> : ICrdtOperations<TCrdt, TValue>
    {
        /// <summary>
        /// This mechanism allows to discard stable operations, not only timestamps, if they have no
        /// impact on the semantics of the data type. For some data types like RWSet
        /// some operations become useless once other future operations become stable"
        /// </summary>
        /// <param name="crdt">A pure Conflict-free Replicated Data Type.</param>
        /// <param name="stable">The <see cref="TCStable"/> vector time delivered by TCSB middleware.</param>
        /// <returns>The crdt after being applied causal stabilization. By default it returns the same crdt unmodified.</returns>
        TCrdt Stable(TCrdt crdt, VectorTime stable);
    }

    internal sealed class PureCrdtServiceSettings
    {
        public PureCrdtServiceSettings(Config config)
        {
            this.OperationTimeout = config.GetTimeSpan("eventuate.crdt.pure.operation-timeout");
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

    internal readonly struct OnStable<T>
    {
        public OnStable(T crdt, VectorTime stable)
        {
            Crdt = crdt;
            Stable = stable;
        }

        public T Crdt { get; }
        public VectorTime Stable { get; }
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
    /// <typeparam name="TCrdt">CRDT Type</typeparam>
    /// <typeparam name="TValue">CRDT value type</typeparam>
    public abstract class PureCrdtService<TCrdt, TValue, TOperations>
        where TOperations : struct, IPureCrdtOperations<TCrdt, TValue>
    {
        private IActorRef manager = null;
        private readonly Lazy<PureCrdtServiceSettings> settings;

        protected PureCrdtService()
        {
            this.settings = new Lazy<PureCrdtServiceSettings>(() => new PureCrdtServiceSettings(System.Settings.Config));
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
            var config = System.Settings.Config.GetConfig($"eventuate.crdt.stability.{this.ServiceId}");

            var localPartition = config.GetString("local-partition");
            var partitions = config.GetStringList("partitions");
            var stabilitySettings =
                (!string.IsNullOrEmpty(localPartition) && !(partitions is null))
                ? new StabilitySettings(localPartition, partitions.ToImmutableHashSet())
                : null;

            if (manager is null)
                manager = System.ActorOf(Props.Create(() => new PureCrdtManager(this.ServiceId, this.EventLog, stabilitySettings)));
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

        private sealed class PureCrdtActor : EventsourcedActor
        {
            private readonly string crdtId;
            private readonly StabilitySettings stabilitySettings;
            private TCrdt crdt;
            private VectorTime lastStable = null;
            private Rtm? rtm;

            public PureCrdtActor(string serviceId, string crdtId, IActorRef eventLog, StabilitySettings stabilitySettings = null)
            {
                Id = serviceId + "_" + crdtId;
                AggregateId = typeof(TCrdt).Name + "_" + crdt;
                EventLog = eventLog;
                this.stabilitySettings = stabilitySettings;
                this.crdtId = crdtId;
                this.crdt = default(TOperations).Zero;
                this.rtm = !(stabilitySettings is null) ? new Rtm(stabilitySettings) : default;
            }

            public override bool StateSync => default(TOperations).Precondition;
            public override string Id { get; }
            public override string AggregateId { get; }
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
                    UpdateStability(LastProcessId, LastVectorTimestamp);
                    Context.Parent.Tell(new OnChange<TCrdt>(crdt, e.Operation));

                    return true;
                }
                else return false;
            }

            private void UpdateStability(string processId, VectorTime vectorTimestamp)
            {
                if (rtm.HasValue)
                {
                    var rtm = this.rtm.Value.Update(processId, vectorTimestamp);
                    this.rtm = rtm;
                    var stable = rtm.Stable;
                    if (this.lastStable != stable)
                    {
                        this.lastStable = stable;
                        this.crdt = default(TOperations).Stable(crdt, stable);
                        Context.Parent.Tell(new OnStable<TCrdt>(crdt, stable));
                    }
                }

            }

            protected override bool OnSnapshot(object snapshot)
            {
                this.crdt = (TCrdt)snapshot;
                Context.Parent.Tell(new OnChange<TCrdt>(crdt, null));
                return true;
            }
        }

        private sealed class PureCrdtManager : ReceiveActor
        {
            private readonly string serviceId;
            private readonly IActorRef eventLog;
            private readonly StabilitySettings stabilitySettings;

            public PureCrdtManager(string serviceId, IActorRef eventLog, StabilitySettings stabilitySettings)
            {
                this.serviceId = serviceId;
                this.eventLog = eventLog;
                this.stabilitySettings = stabilitySettings;
                Receive<IIdentified>(i => Crdt(i.Id).Forward(i));
            }

            private IActorRef Crdt(string id)
            {
                var name = Uri.EscapeDataString(id);
                var child = Context.Child(name);
                if (Equals(child, ActorRefs.Nobody))
                {
                    child = Context.ActorOf(Props.Create(() => new PureCrdtActor(this.serviceId, id, this.eventLog, this.stabilitySettings)), name: name);
                }

                return child;
            }
        }

        #endregion
    }
}
