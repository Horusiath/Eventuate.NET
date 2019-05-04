using System;

namespace Eventuate.Crdt.Pure
{
    /// <summary>
    /// Returns true if the operation is itself redundant or it is redundant by the operations
    /// currently on the POLog.
    /// "In most cases this can be decided by looking only at the delivered operation itself"
    /// </summary>
    public delegate bool LogRedundancy(Versioned<object> versioned, PartiallyOrderedLog log);

    /// <summary>
    /// Returns true if the second operation (the operation currently in the POLog is made redundant by
    /// the first one (the new delivered one)
    /// </summary>
    public delegate bool Redundancy(Versioned<object> a, Versioned<object> b);

    /// <summary>
    /// A triple containing the data type specific relations R, R0 and R1 used by a CRDT for causal redundancy.
    /// </summary>
    public readonly struct CausalRedundancy
    {
        public CausalRedundancy(LogRedundancy r, Redundancy r0, Redundancy r1 = null)
        {
            R = r;
            R0 = r0;
            R1 = r1 ?? r0;
        }

        /// <summary>
        /// Defines whether the delivered operation is itself redundant and does not need to be added itself to the PO-Log.
        /// </summary>
        public LogRedundancy R { get; }

        /// <summary>
        /// Is used when the new delivered operation is discarded being redundant.
        /// </summary>
        public Redundancy R0 { get; }

        /// <summary>
        /// Is used if the new delivered operation is added to the PO-Log.
        /// </summary>
        public Redundancy R1 { get; }

        /// <summary>
        /// Returns the <see cref="Redundancy"/> relation that should be used to prune the POLog and the stable state, depending on wether the newly delivered operation was redundant or not.
        /// </summary>
        /// <param name="redundant">
        /// A flag that indicates if the newly delivered operation was marked redundant by the 
        /// <see cref="R"/> relation (hence not added to the POLog)
        /// </param>
        public Redundancy RedundancyFilter(bool redundant) => redundant ? R0 : R1;
    }

    /// <summary>
    /// A pure op-based CRDT wich state is splitted in two componentes. A map of timestamps to operations (the POLog) and
    /// a plain set of stable operations or a specialized implementation (the state)
    /// P(O) × (T ֒→ O)
    /// </summary>
    public readonly struct CRDT<TState> : ISerializable
    {
        public CRDT(TState state) : this(PartiallyOrderedLog.Empty, state) { }

        public CRDT(PartiallyOrderedLog log, TState state)
        {
            Log = log;
            State = state;
        }

        /// <summary>
        /// The POLog contains only the set of timestamped operations.
        /// </summary>
        public PartiallyOrderedLog Log { get; }

        /// <summary>
        /// .The state of the CRDT that contains stable operations (non-timestamped) or a "specialized
        /// implementation according to the domain" e.g., a bitmap for dense sets of integers in an AWSet
        /// </summary>
        public TState State { get; }
    }
}
