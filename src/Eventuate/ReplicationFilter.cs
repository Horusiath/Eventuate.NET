#region copyright
// -----------------------------------------------------------------------
//  <copyright file="ReplicationFilter.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;

namespace Eventuate
{
    /// <summary>
    /// Serializable and composable replication filter.
    /// </summary>
    public abstract class ReplicationFilter : ISerializable
    {
        /// <summary>
        /// Evaluates this filter on the given <paramref name="durableEvent"/>.
        /// </summary>
        public abstract bool Invoke(DurableEvent durableEvent);

        /// <summary>
        /// Returns a composed replication filter that represents a logical AND of
        /// this filter and the given <paramref name="filter"/>.
        /// </summary>
        public virtual ReplicationFilter And(ReplicationFilter filter)
        {
            if (this is AndFilter f) return new AndFilter(f.Filters.Union(new[] { filter }));
            else return new AndFilter(new[] { this, filter });
        }

        /// <summary>
        /// Returns a composed replication filter that represents a logical OR of
        /// this filter and the given <paramref name="filter"/>.
        /// </summary>
        public virtual ReplicationFilter Or(ReplicationFilter filter)
        {
            if (this is OrFilter f) return new OrFilter(f.Filters.Union(new[] { filter }));
            else return new OrFilter(new[] { this, filter });
        }
    }

    /// <summary>
    /// Serializable logical AND of given `filters`.
    /// </summary>
    internal sealed class AndFilter : ReplicationFilter
    {
        public IEnumerable<ReplicationFilter> Filters { get; }

        public AndFilter(IEnumerable<ReplicationFilter> filters) {
            this.Filters = filters;
        }

        /// <summary>
        /// Evaluates to `true` if all `filters` evaluate to `true`, `false` otherwise.
        /// </summary>
        public override bool Invoke(DurableEvent durableEvent)
        {
            foreach (var filter in Filters)
            {
                if (!filter.Invoke(durableEvent)) return false;
            }
            return true;
        }
    }

    /// <summary>
    /// Serializable logical OR of given `filters`.
    /// </summary>
    internal sealed class OrFilter : ReplicationFilter
    {
        public IEnumerable<ReplicationFilter> Filters { get; }

        public OrFilter(IEnumerable<ReplicationFilter> filters)
        {
            this.Filters = filters;
        }

        public override bool Invoke(DurableEvent durableEvent)
        {
            foreach (var filter in Filters)
            {
                if (filter.Invoke(durableEvent)) return true;
            }
            return false;
        }
    }

    /// <summary>
    /// Replication filter that evaluates to `true` for all events.
    /// </summary>
    internal sealed class NoFilter : ReplicationFilter
    {
        public static readonly NoFilter Instance = new NoFilter();
        private NoFilter() { }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool Invoke(DurableEvent durableEvent) => true;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override ReplicationFilter And(ReplicationFilter filter) => filter;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override ReplicationFilter Or(ReplicationFilter filter) => this;
    }
}
