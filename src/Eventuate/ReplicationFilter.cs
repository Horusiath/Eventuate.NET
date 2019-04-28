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
    public interface IReplicationFilter : ISerializable
    {
        /// <summary>
        /// Evaluates this filter on the given <paramref name="durableEvent"/>.
        /// </summary>
        bool Invoke(DurableEvent durableEvent);

        /// <summary>
        /// Returns a composed replication filter that represents a logical AND of
        /// this filter and the given <paramref name="filter"/>.
        /// </summary>
        IReplicationFilter And(IReplicationFilter filter);

        /// <summary>
        /// Returns a composed replication filter that represents a logical OR of
        /// this filter and the given <paramref name="filter"/>.
        /// </summary>
        IReplicationFilter Or(IReplicationFilter filter);
    }

    /// <summary>
    /// Serializable logical AND of given `filters`.
    /// </summary>
    internal sealed class AndFilter : IReplicationFilter
    {
        private readonly IEnumerable<IReplicationFilter> filters;

        public AndFilter(IEnumerable<IReplicationFilter> filters) {
            this.filters = filters;
        }

        /// <summary>
        /// Evaluates to `true` if all `filters` evaluate to `true`, `false` otherwise.
        /// </summary>
        public bool Invoke(DurableEvent durableEvent)
        {
            foreach (var filter in filters)
            {
                if (!filter.Invoke(durableEvent)) return false;
            }
            return true;
        }

        public IReplicationFilter And(IReplicationFilter filter) => new AndFilter(filters.Union(new[] { filter }));

        public IReplicationFilter Or(IReplicationFilter filter) => new OrFilter(new[] { this, filter });
    }

    /// <summary>
    /// Serializable logical OR of given `filters`.
    /// </summary>
    internal sealed class OrFilter : IReplicationFilter
    {
        private readonly IEnumerable<IReplicationFilter> filters;

        public OrFilter(IEnumerable<IReplicationFilter> filters)
        {
            this.filters = filters;
        }

        public bool Invoke(DurableEvent durableEvent)
        {
            foreach (var filter in filters)
            {
                if (filter.Invoke(durableEvent)) return true;
            }
            return false;
        }

        public IReplicationFilter And(IReplicationFilter filter) => new AndFilter(new[] { this, filter });

        public IReplicationFilter Or(IReplicationFilter filter) => new OrFilter(filters.Union(new[] { filter }));
    }

    /// <summary>
    /// Replication filter that evaluates to `true` for all events.
    /// </summary>
    internal sealed class NoFilter : IReplicationFilter
    {
        public static readonly NoFilter Instance = new NoFilter();
        private NoFilter() { }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Invoke(DurableEvent durableEvent) => true;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IReplicationFilter And(IReplicationFilter filter) => filter;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IReplicationFilter Or(IReplicationFilter filter) => this;
    }
}
