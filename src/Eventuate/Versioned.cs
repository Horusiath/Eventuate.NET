using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;

namespace Eventuate
{
    /// <summary>
    /// A versioned value.
    /// </summary>
    public readonly struct Versioned<T>
    {
        public Versioned(T value, VectorTime vectorTimestamp, DateTime? systemTimestamp = null, string creator = null)
        {
            Value = value;
            VectorTimestamp = vectorTimestamp;
            SystemTimestamp = systemTimestamp ?? DateTime.MinValue;
            Creator = creator ?? string.Empty;
        }

        /// <summary>
        /// The value to be versioned.
        /// </summary>
        public T Value { get; }

        /// <summary>
        /// Update vector timestamp of the event that caused this version.
        /// </summary>
        public VectorTime VectorTimestamp { get; }

        /// <summary>
        /// Update system timestamp of the event that caused this version.
        /// </summary>
        public DateTime SystemTimestamp { get; }

        /// <summary>
        /// Creator of the event that caused this version.
        /// </summary>
        public string Creator { get; }
    }

    /// <summary>
    /// Tracks concurrent <see cref="Versioned{T}"/> values which arise from concurrent updates.
    /// </summary>
    /// <typeparam name="TValue">Versioned value type.</typeparam>
    /// <typeparam name="TUpdate">Update type.</typeparam>
    public interface IConcurrentVersions<TValue, TUpdate> : ISerializable
    {
        /// <summary>
        /// Updates that <see cref="Versioned"/> value with <paramref name="update"/> that is a predecessor of <paramref name="vectorTimestamp"/>. If
        /// there is no such predecessor, a new concurrent version is created 
        /// (optionally derived from an older entry in the version history, in case of incremental updates).
        /// </summary>
        IConcurrentVersions<TValue, TUpdate> Update(TUpdate update, VectorTime vectorTimestamp, DateTime? systemTimestamp = null, string creator = null);

        /// <summary>
        /// Resolves multiple concurrent versions to a single version. For the resolution to be successful,
        /// one of the concurrent versions must have a <paramref name="vectorTimestamp"/> that is equal to <paramref name="selectedTimestamp"/>.
        /// Only those concurrent versions with a `vectorTimestamp` less than the given `vectorTimestamp`
        /// participate in the resolution process (which allows for resolutions to be concurrent to other
        /// updates).
        /// </summary>
        IConcurrentVersions<TValue, TUpdate> Resolve(VectorTime selectedTimestamp, VectorTime vectorTimestamp, DateTime? systemTimestamp = null);

        /// <summary>
        /// Returns all (un-resolved) concurrent versions.
        /// </summary>
        IEnumerable<Versioned<TValue>> All { get; }

        /// <summary>
        /// Returns `true` if there is more than one version available i.e. if there are multiple concurrent(= conflicting) versions.
        /// </summary>
        bool HasConflict { get; }

        /// <summary>
        /// Owner of versioned values.
        /// </summary>
        string Owner { get; }

        /// <summary>
        /// Updates the owner.
        /// </summary>
        IConcurrentVersions<TValue, TUpdate> WithOwner(string owner);
    }

    /// <summary>
    /// A <see cref="IConcurrentVersions{TValue, TUpdate}"/> implementation that shall be used if updates replace current
    /// versioned values (= full updates). <see cref="ConcurrentVersionsList{T}"/> is an immutable data structure.
    /// </summary>
    public sealed class ConcurrentVersionsList<T> : IConcurrentVersions<T, T>
    {
        private readonly ImmutableList<Versioned<T>> versions;

        public ConcurrentVersionsList(ImmutableList<Versioned<T>> versions = null, string owner = null)
        {
            this.versions = versions ?? ImmutableList<Versioned<T>>.Empty;
            this.Owner = owner ?? string.Empty;
        }

        public IEnumerable<Versioned<T>> All => versions;

        public bool HasConflict => versions.Count > 1;

        public string Owner { get; }

        public IConcurrentVersions<T, T> Resolve(VectorTime selectedTimestamp, VectorTime vectorTimestamp, DateTime? systemTimestamp = null)
        {
            var builder = ImmutableList.CreateBuilder<Versioned<T>>();
            foreach (var version in this.versions)
            {
                var cmp = version.VectorTimestamp.PartiallyCompareTo(selectedTimestamp);
                if (!cmp.HasValue) builder.Add(version);
                else if (cmp.Value == 0) builder.Add(new Versioned<T>(version.Value, vectorTimestamp, systemTimestamp));
            }

            return new ConcurrentVersionsList<T>(builder.ToImmutable(), this.Owner);
        }

        public IConcurrentVersions<T, T> Update(T update, VectorTime vectorTimestamp, DateTime? systemTimestamp = null, string creator = null)
        {
            var builder = ImmutableList.CreateBuilder<Versioned<T>>();
            var conflictResolved = false;
            foreach (var version in this.versions)
            {
                if (conflictResolved) builder.Add(version);
                else
                {
                    var cmp = version.VectorTimestamp.PartiallyCompareTo(vectorTimestamp);
                    if (cmp > 0)
                    {
                        // regular update on that version
                        builder.Add(new Versioned<T>(update, vectorTimestamp, systemTimestamp, creator));
                    }
                    else if (cmp < 0)
                    {
                        // conflict already resolved, ignore
                        builder.Add(version);
                        conflictResolved = false;
                    }
                    else
                    {
                        // conflicting update, try next
                        builder.Add(version);
                        conflictResolved = false;
                    }
                }

            }

            return new ConcurrentVersionsList<T>(builder.ToImmutable(), this.Owner);
        }

        public IConcurrentVersions<T, T> WithOwner(string owner) => new ConcurrentVersionsList<T>(this.versions, owner);
    }

    /// <summary>
    /// A <see cref="IConcurrentVersions{TValue, TUpdate}"/> implementation that shall be used if updates are incremental.
    /// `ConcurrentVersionsTree` is a mutable data structure. Therefore, it is recommended not
    /// to share instances of <see cref="ConcurrentVersionsTree{TValue, TUpdate}"/> directly but rather the <see cref="Versioned{T}"/>
    /// sequence returned by <see cref="ConcurrentVersionsTree{TValue, TUpdate}.All"/>. Later releases will be based on
    /// an immutable data structure.
    /// 
    /// '''Please note:''' This implementation does not purge old versions at the moment (which
    /// shouldn't be a problem if the number of incremental updates to a versioned aggregate is
    /// rather small). In later releases, manual and automated purging of old versions will be
    /// supported.
    /// </summary>
    public sealed class ConcurrentVersionsTree<TValue, TUpdate> : IConcurrentVersions<TValue, TUpdate>, ICloneable
    {
        private static Func<TValue, TUpdate, TValue> Ignore = (v, _) => v;

        internal sealed class Node : ISerializable, ICloneable
        {
            internal bool rejected = false;
            internal ImmutableArray<Node> children = ImmutableArray<Node>.Empty;
            internal Node parent;

            public Node(Versioned<TValue> versioned)
            {
                this.parent = this;
                Versioned = versioned;
            }

            public Versioned<TValue> Versioned { get; private set; }

            public bool IsLeaf
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => children.IsEmpty;
            }

            public bool IsRoot
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => ReferenceEquals(this, parent);
            }

            public bool IsOnlyChild
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => this.parent.children.Length == 1;
            }

            public object Clone()
            {
                var childrenCopies = ImmutableArray.CreateBuilder<Node>(this.children.Length);
                foreach (var child in this.children)
                {
                    childrenCopies.Add((Node)child.Clone());
                }
                return new Node(this.Versioned)
                {
                    children = childrenCopies.ToImmutable(),
                    rejected = this.rejected
                };
            }

            public void AddChild(Node node)
            {
                node.parent = this;
                this.children = this.children.Add(node);
            }

            public void Reject()
            {
                this.rejected = true;
                if (IsOnlyChild) parent.Reject();
            }

            public void Stamp(VectorTime vt, DateTime st)
            {
                var v = this.Versioned;
                this.Versioned = new Versioned<TValue>(v.Value, vt, st, v.Creator);
            }
        }

        private readonly Node root;
        private readonly Func<TValue, TUpdate, TValue> projection;

        internal ConcurrentVersionsTree(Node root, string owner = null, Func<TValue, TUpdate, TValue> projection = null)
        {
            this.Owner = owner ?? string.Empty;
            this.root = root;
            this.projection = projection ?? Ignore;
        }

        private T Aggregate<T>(Node node, T seed, Func<T, Node, T> fold)
        {
            var result = fold(seed, node);
            if (node.IsLeaf) return result;
            else return node.children.Aggregate(result, (acc, n) => Aggregate(n, acc, fold));
        }

        private IEnumerable<Node> Leaves() => Aggregate(this.root, ImmutableList<Node>.Empty, (acc, n) =>  n.IsLeaf ? acc.Add(n) : acc);

        private IEnumerable<Node> Nodes() => Aggregate(this.root, ImmutableList<Node>.Empty, (acc, n) => acc.Add(n));

        private Node Predecessor(VectorTime timestamp) => Aggregate(this.root, this.root, (candidate, n) =>
            (timestamp > n.Versioned.VectorTimestamp && n.Versioned.VectorTimestamp > candidate.Versioned.VectorTimestamp) ? n : candidate);

        public IEnumerable<Versioned<TValue>> All
        {
            get
            {
                foreach (var n in Leaves())
                {
                    if (!n.rejected)
                        yield return n.Versioned;
                }
            }
        }

        public bool HasConflict => !this.root.IsLeaf;

        public string Owner { get; }

        public IConcurrentVersions<TValue, TUpdate> Resolve(VectorTime selectedTimestamp, VectorTime vectorTimestamp, DateTime? systemTimestamp = null)
        {
            foreach (var n in this.Leaves())
            {
                if (n.rejected) { } // ignore
                else if (n.Versioned.VectorTimestamp.IsConcurrent(vectorTimestamp)) { } // ignore
                else if (n.Versioned.VectorTimestamp == selectedTimestamp) n.Stamp(vectorTimestamp, systemTimestamp ?? DateTime.MinValue);
                else n.Reject();
            }
            return this;
        }

        public IConcurrentVersions<TValue, TUpdate> Update(TUpdate update, VectorTime vectorTimestamp, DateTime? systemTimestamp = null, string creator = null)
        {
            var p = Predecessor(vectorTimestamp);
            p.AddChild(new Node(new Versioned<TValue>(this.projection(p.Versioned.Value, update), vectorTimestamp, systemTimestamp, creator)));
            return this;
        }

        public IConcurrentVersions<TValue, TUpdate> WithOwner(string owner) =>
            new ConcurrentVersionsTree<TValue, TUpdate>((Node)this.root.Clone(), owner, this.projection);

        public IConcurrentVersions<TValue, TUpdate> WithProjection(Func<TValue, TUpdate, TValue> projection) =>
            new ConcurrentVersionsTree<TValue, TUpdate>((Node)this.root.Clone(), this.Owner, projection);

        public object Clone() => new ConcurrentVersionsTree<TValue, TUpdate>((Node)this.root.Clone(), this.Owner, this.projection);
    }

    public static class ConcurrentVersionsTree
    {
        /// <summary>
        /// Creates a new <see cref="ConcurrentVersionsTree{TValue, TUpdate}"/> that uses projection function to compute
        /// new (potentially concurrent) versions from a parent version.
        /// </summary>
        /// <param name="initial">Value of the initial version.</param>
        /// <param name="projection">Projection function for updates.</param>
        public static ConcurrentVersionsTree<TValue, TUpdate> Create<TValue, TUpdate>(TValue initial, Func<TValue, TUpdate, TValue> projection) =>
            new ConcurrentVersionsTree<TValue, TUpdate>(new ConcurrentVersionsTree<TValue, TUpdate>.Node(new Versioned<TValue>(initial, VectorTime.Zero)), projection: projection);

        /// <summary>
        /// Creates a new <see cref="ConcurrentVersionsTree{TValue, TUpdate}"/> that uses projection function to compute
        /// new (potentially concurrent) versions from a parent version.
        /// </summary>
        /// <param name="projection">Projection function for updates.</param>
        public static ConcurrentVersionsTree<TValue, TUpdate> Create<TValue, TUpdate>(Func<TValue, TUpdate, TValue> projection) where TValue : new() =>
            Create(new TValue(), projection);
    }

    public static class ConcurrentVersionsExtensions
    {
        public static IConcurrentVersions<T1, T2> Resolve<T1, T2>(this IConcurrentVersions<T1, T2> versions, VectorTime selectedTimestamp)
        {
            var vectorTime = VectorTime.Zero;
            DateTime systemTime = default;
            foreach (var versioned in versions.All)
            {
                vectorTime = vectorTime.Merge(versioned.VectorTimestamp);
                systemTime = versioned.SystemTimestamp > systemTime ? versioned.SystemTimestamp : systemTime;
            }

            return versions.Resolve(selectedTimestamp, vectorTime, systemTime);
        }
    }
}
