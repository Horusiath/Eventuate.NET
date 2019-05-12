#region copyright
// -----------------------------------------------------------------------
//  <copyright file="ConcurrentVersionsSpec.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Linq;
using FluentAssertions;
using Xunit;

namespace Eventuate.Tests
{
    public abstract class ConcurrentVersionsSpec
    {
        protected IConcurrentVersions<string, string> versions;
        
        protected ConcurrentVersionsSpec()
        {
            this.versions = Create();
        }

        protected abstract IConcurrentVersions<string, string> Create();
        protected VectorTime VectorTime(int t1, int t2, int t3) =>
            new VectorTime(("p1", t1), ("p2", t2), ("p3", t3));

        [Fact]
        public void ConcurrentVersions_instance_must_track_causal_updates()
        {
            var result = versions
                .Update("a", VectorTime(1, 0, 0))
                .Update("b", VectorTime(2, 0, 0));

            result.HasConflict.Should().Be(false);
            var all = result.All.ToArray();
            all.First().Should().Be(new Versioned<string>("b", VectorTime(2, 0, 0)));
        }
        
        [Fact]
        public void ConcurrentVersions_instance_must_track_concurrent_updates()
        {
            var result = versions
                .Update("a", VectorTime(1, 0, 0))
                .Update("b", VectorTime(0, 1, 0));

            result.HasConflict.Should().Be(true);
            var all = result.All.ToArray();
            all[0].Should().Be(new Versioned<string>("a", VectorTime(1, 0, 0)));
            all[1].Should().Be(new Versioned<string>("b", VectorTime(0, 1, 0)));
        }
        
        [Fact]
        public void ConcurrentVersions_instance_must_resolve_concurrent_updates()
        {
            var result = versions
                .Update("a", VectorTime(1, 0, 0))
                .Update("b", VectorTime(0, 1, 0))
                .Resolve(VectorTime(1,0,0), VectorTime(2, 1, 0));

            result.HasConflict.Should().Be(false);
            var all = result.All.ToArray();
            all[0].Should().Be(new Versioned<string>("a", VectorTime(2, 1, 0)));
        }
        
        [Fact]
        public void ConcurrentVersions_instance_must_resolve_concurrent_updates_with_implicit_event_timestamp()
        {
            var result = versions
                .Update("a", VectorTime(1, 0, 0))
                .Update("b", VectorTime(0, 1, 0))
                .Resolve(VectorTime(1,0,0));

            result.HasConflict.Should().Be(false);
            var all = result.All.ToArray();
            all[0].Should().Be(new Versioned<string>("a", VectorTime(1, 1, 0)));
        }
        
        [Fact]
        public void ConcurrentVersions_instance_must_resolve_concurrent_updates_advanced()
        {
            var updated = versions
                .Update("a", VectorTime(1, 0, 0))
                .Update("b", VectorTime(0, 1, 0))
                .Update("c", VectorTime(0, 1, 4))
                .Update("d", VectorTime(0, 3, 0))
                .Update("e", VectorTime(0, 1, 5));

            var all = updated.All.ToArray();
            all.Length.Should().Be(3);
            all[0].Should().Be(new Versioned<string>("a", VectorTime(1, 0, 0)));
            all[1].Should().Be(new Versioned<string>("e", VectorTime(0, 1, 5)));
            all[2].Should().Be(new Versioned<string>("d", VectorTime(0, 3, 0)));

            var result = updated.Resolve(VectorTime(0, 3, 0), VectorTime(3, 4, 8));
            result.HasConflict.Should().Be(false);
            result.All.First().Should().Be(new Versioned<string>("d", VectorTime(3, 4, 8)));
        }
        
        [Fact]
        public void ConcurrentVersions_instance_must_only_resolve_concurrent_updates_that_happened_before_resolve()
        {
            var result = versions
                .Update("a", VectorTime(1, 0, 0))
                .Update("b", VectorTime(0, 1, 0))
                .Update("c", VectorTime(0, 0, 1))
                .Resolve(VectorTime(1,0,0), VectorTime(2, 1, 0));

            var all = result.All.ToArray();
            all.Length.Should().Be(2);
            all[0].Should().Be(new Versioned<string>("a", VectorTime(2, 1, 0)));
            all[1].Should().Be(new Versioned<string>("c", VectorTime(0, 0, 1)));
        }
    }

    public class ConcurrentVersionsListSpec : ConcurrentVersionsSpec
    {
        protected override IConcurrentVersions<string, string> Create() => new ConcurrentVersionsList<string>();
    }

    public class ConcurrentVersionsTreeSpec : ConcurrentVersionsSpec
    {
        private static readonly Func<string, string, string> Replace = (a, b) => b;
        private static readonly Func<string, string, string> Append = (a, b) => $"{a}{b}";
        
        protected override IConcurrentVersions<string, string> Create() => 
            ConcurrentVersionsTree.Create<string, string>(null, Replace);

        [Fact]
        public void ConcurrentVersionsTree_instance_must_support_updates_on_rejected_versions_append_to_leaf()
        {
            var result = ConcurrentVersionsTree.Create(null, Append)
                .Update("a", VectorTime(1, 0, 0))
                .Update("b", VectorTime(1, 1, 0))
                .Update("c", VectorTime(1, 0, 1))
                .Resolve(VectorTime(1, 0, 1), VectorTime(1, 2, 1))
                .Update("d", VectorTime(2, 1, 0))
                .Update("e", VectorTime(3, 1, 0));

            var all = result.All.ToArray();
            all.Length.Should().Be(2);
            all[0].Should().Be(new Versioned<string>("abde", VectorTime(3, 1, 0)));
            all[1].Should().Be(new Versioned<string>("ac", VectorTime(1, 2, 1)));
        }
        
        [Fact]
        public void ConcurrentVersionsTree_instance_must_support_updates_on_rejected_versions_append_to_non_leaf()
        {
            var result = ConcurrentVersionsTree.Create(null, Append)
                .Update("a", VectorTime(1, 0, 0))
                .Update("b", VectorTime(1, 1, 0))
                .Update("x", VectorTime(1, 2, 0))
                .Update("c", VectorTime(1, 0, 1))
                .Resolve(VectorTime(1, 0, 1), VectorTime(1, 3, 1))
                .Update("d", VectorTime(2, 1, 0))
                .Update("e", VectorTime(3, 1, 0));

            var all = result.All.ToArray();
            all.Length.Should().Be(2);
            all[0].Should().Be(new Versioned<string>("abde", VectorTime(3, 1, 0)));
            all[1].Should().Be(new Versioned<string>("ac", VectorTime(1, 3, 1)));
        }
        
        [Fact]
        public void ConcurrentVersionsTree_instance_must_append_updates_to_the_closest_predecessor()
        {
            var result = ConcurrentVersionsTree.Create(null, Append)
                .Update("a", VectorTime(1, 0, 0))
                .Update("b", VectorTime(2, 0, 0))
                .Update("c", VectorTime(1, 1, 0))
                .Resolve(VectorTime(2, 0, 0), VectorTime(2, 2, 0))
                .Update("d", VectorTime(3, 2, 0));

            result.HasConflict.Should().Be(false);
            var all = result.All.ToArray();
            all[0].Should().Be(new Versioned<string>("abd", VectorTime(3, 2, 0)));
        }
        
        [Fact]
        public void ConcurrentVersionsTree_instance_must_create_deep_copy_of_itself()
        {
            var tree = ConcurrentVersionsTree.Create(null, Append)
                .Update("a", VectorTime(1, 0, 0))
                .Update("b", VectorTime(2, 0, 0))
                .Update("c", VectorTime(1, 1, 0)) as ConcurrentVersionsTree<string, string>;

            var upd1 = ((ConcurrentVersionsTree<string, string>) tree.Clone())
                .Resolve(VectorTime(2, 0, 0), VectorTime(2, 2, 0));
            
            var upd2 = ((ConcurrentVersionsTree<string, string>) tree.Clone())
                .Resolve(VectorTime(1, 1, 0), VectorTime(2, 2, 0));

            tree.HasConflict.Should().Be(true);
            upd1.HasConflict.Should().Be(false);
            upd2.HasConflict.Should().Be(false);
            
            tree.All.Should().BeEquivalentTo(
                new Versioned<string>("ab", VectorTime(2, 0, 0)),
                new Versioned<string>("ac", VectorTime(1, 1, 0)));

            upd1.All.Should().BeEquivalentTo(new Versioned<string>("ab", VectorTime(2, 2, 0)));
            upd2.All.Should().BeEquivalentTo(new Versioned<string>("ac", VectorTime(2, 2, 0)));
        }
    }
}