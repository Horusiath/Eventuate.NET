using Akka.Configuration;
using System;
using System.Collections.Generic;
using System.Text;

namespace Eventuate.Crdt.Pure
{
    /// <summary>
    /// Typeclass to be implemented by CRDTs if they shall be managed by <see cref="CrdtService{TCrdt, TValue, TOperations}"/>
    /// </summary>
    /// <typeparam name="TCrdt">CRDT type.</typeparam>
    /// <typeparam name="TValue">CRDT returned value type.</typeparam>
    public interface ICrdtOperations<TCrdt, TValue>
    {
        /// <summary>
        /// Default CRDT instance.
        /// </summary>
        TCrdt Zero { get; }

        /// <summary>
        /// Must return `true` if CRDT checks preconditions. Should be overridden to return
        /// `false` if CRDT does not check preconditions, as this will significantly increase
        /// write throughput.
        /// </summary>
        bool Precondition { get; }

        /// <summary>
        /// Returns the CRDT value (for example, the entries of an OR-Set)
        /// </summary>
        TValue Value(TCrdt crdt);

        /// <summary>
        /// Update phase 1 ("atSource"). Prepares an operation for phase 2.
        /// </summary>
        object Prepare(TCrdt crdt, object operation);

        /// <summary>
        /// Update phase 2 ("downstream").
        /// </summary>
        TCrdt Effect(TCrdt crdt, object operation, VectorTime vectorTimestamp, DateTime? systemTimestamp = null, string creator = null);

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

    internal sealed class CrdtServiceSettings
    {
        public CrdtServiceSettings(Config config)
        {
            this.OperationTimeout = config.GetTimeSpan("eventuate.crdt.pure.operation-timeout");
        }

        public TimeSpan OperationTimeout { get; }
    }

    /// <summary>
    /// A generic, replicated CRDT service that manages a map of CRDTs identified by name.
    /// Replication is based on the replicated event `log` that preserves causal ordering
    /// of events.
    /// </summary>
    /// <typeparam name="TCrdt">CRDT Type</typeparam>
    /// <typeparam name="TValue">CRDT value type</typeparam>
    public abstract class CrdtService<TCrdt, TValue, TOperations> 
        where TOperations: struct, ICrdtOperations<TCrdt, TValue>
    {
    }
}
