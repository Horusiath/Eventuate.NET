using Akka.Actor;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace Eventuate.Crdt
{
    /// <summary>
    /// Operation-based Counter CRDT.
    /// </summary>
    public readonly struct Counter : ISerializable
    {
        public Counter(long value)
        {
            Value = value;
        }

        public long Value { get; }

        public Counter Update(long delta) => new Counter(Value + delta);

        public readonly struct UpdateOp : ISerializable
        {
            public UpdateOp(object delta)
            {
                Delta = delta;
            }

            public object Delta { get; }
        }

        public readonly struct Operations : ICrdtOperations<Counter, long>
        {
            public Counter Zero
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => default;
            }

            public bool Precondition
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => false;
            }

            public Counter Effect(Counter crdt, object operation, DurableEvent e)
            {
                var op = (UpdateOp)operation;
                return crdt.Update((long)op.Delta);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public object Prepare(Counter crdt, object operation) => operation;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public long Value(Counter crdt) => crdt.Value;
        }
    }

    public sealed class CounterService : CrdtService<Counter, long, Counter.Operations>
    {
        public CounterService(ActorSystem system, string serviceId, IActorRef eventLog)
        {
            System = system;
            ServiceId = serviceId;
            EventLog = eventLog;

            this.Start();
        }

        public override ActorSystem System { get; }

        public override string ServiceId { get; }

        public override IActorRef EventLog { get; }

        public Task<long> Update(string id, long delta) => Update(id, new Counter.UpdateOp(delta));
    }
}
