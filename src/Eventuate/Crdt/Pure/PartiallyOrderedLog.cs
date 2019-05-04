#region copyright
// -----------------------------------------------------------------------
// <copyright file="PartiallyOrderedLog.cs" company="Bartosz Sypytkowski">
//     Copyright 2015 - 2016 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//     Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
// </copyright>
// -----------------------------------------------------------------------
#endregion

using System.Collections.Generic;
using System.Collections.Immutable;

namespace Eventuate.Crdt.Pure
{
    /// <summary>
    /// A Partial Ordered Log which retains all invoked operations together with their timestamps.
    /// </summary>
    public sealed class PartiallyOrderedLog : ISerializable
    {
        public static readonly PartiallyOrderedLog Empty = new PartiallyOrderedLog();

        public PartiallyOrderedLog() : this(ImmutableHashSet<Versioned<object>>.Empty) { }

        public PartiallyOrderedLog(ImmutableHashSet<Versioned<object>> log)
        {
            Log = log;
        }

        /// <summary>
        /// The set of operations with its timestamp and optional metadata (i.e. systemTimestamp, creator).
        /// </summary>
        public ImmutableHashSet<Versioned<object>> Log { get; }

        /// <summary>
        /// "Prunes the PO-Log once an operation is causally delivered in the effect. The aim is to
        /// keep the smallest number of PO-Log operations such that all queries return
        /// the same result as if the full PO-Log was present. In particular, this method discards
        /// operations from the PO-Log if they can be removed without impacting the output of query
        /// operations"
        /// These is called causal redundancy and it is one of the two mechanisms that conforms the semantic
        /// compaction used by the framework to reduce the size of pure op-based CRDTs. The other one is causal
        /// stabilization through <see cref="Stable(VectorTime)"/>.
        /// </summary>
        /// <param name="ops">The set of operations that conform the POLog.</param>
        /// <param name="newOperation">The new delivered operation.</param>
        /// <param name="r">A function that receives a new operation o and returns a filter that returns true if an operation o' is redundant by o.</param>
        /// <returns>The set of operations that conform the POLog and are not redundant by <paramref name="newOperation"/>.</returns>
        public ImmutableHashSet<Versioned<object>> Prune(ImmutableHashSet<Versioned<object>> ops, Versioned<object> newOperation, Redundancy r)
        {
            var builder = ops.ToBuilder();
            foreach (var op in ops)
            {
                if (r(newOperation, op))
                    builder.Remove(op);
            }
            return builder.ToImmutable();
        }

        /// <summary>
        /// "A newly delivered operation (t, o) is added to the PO-Log if it is not redundant
        /// by the PO-Log operations [...]. An existing operation x in the PO-Log is removed
        /// if it is made redundant by (t, o)"
        /// </summary>
        /// <see cref="CvRDTPureOp.UpdateState"/>
        /// <param name="operation">The operation to add.</param>
        /// <param name="redundancy">The data type specific relations for causal redundancy.</param>
        /// <returns>
        /// A pair conformed by
        /// - a boolean indicating if the operation was added to the POLog (i.e. it wasn't redundant). This is used after in [[CvRDTPureOpSimple.updateState]] to know wich [[Redundancy_]] relation must use for update the state.
        /// - the resulting POLog after adding and pruning.
        /// Note that the operation received may not be present in the returned POLog if it was redundant
        /// </returns>
        public (PartiallyOrderedLog, bool) Add(Versioned<object> operation, in CausalRedundancy redundancy)
        {
            var redundant = redundancy.R(operation, this);
            var updatedLog = redundant ? Log : Log.Add(operation);
            var r = redundancy.RedundancyFilter(redundant);
            return (new PartiallyOrderedLog(Prune(updatedLog, operation, r)), redundant);
        }

        /// <summary>
        /// Discards all the operations from the POLog that are less or equal than the received <paramref name="stable"/>
        /// and returns a pair with the updated POLog and the discarded (stable) operations.
        /// </summary>
        /// <param name="stable">The stable version delivered by the TCSB middleware.</param>
        /// <returns>
        /// A pair conformed by the <see cref="PartiallyOrderedLog"/> with only the operations that are not stable
        /// at the received <see cref="TCStable"/>, and the set of operations that are stable
        /// at the received <see cref="TCStable"/>.
        /// </returns>
        public (PartiallyOrderedLog, ImmutableHashSet<object>) Stable(VectorTime stable)
        {
            var stableOps = ImmutableHashSet.CreateBuilder<object>();
            var nonStableOps = ImmutableHashSet.CreateBuilder<Versioned<object>>();
            foreach (var op in Log)
            {
                if (stable.IsStable(op.VectorTimestamp))
                    stableOps.Add(op.Value);
                else
                    nonStableOps.Add(op);
            }
            return (new PartiallyOrderedLog(nonStableOps.ToImmutable()), stableOps.ToImmutable());
        }
    }
}