#region copyright
// -----------------------------------------------------------------------
//  <copyright file="ORSetSpec.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using Eventuate.Crdt;
using FluentAssertions;
using Xunit;

namespace Eventuate.Tests.Crdt
{
    public class ORSetSpec
    {
        private readonly ORSet<int> target = ORSet<int>.Empty;

        private static VectorTime VectorTime(int a, int b) => new VectorTime(("p1", a), ("p2", b));

        [Fact]
        public void ORSet_must_be_empty_by_default()
        {
            target.Value.Should().BeEmpty();
        }
        
        [Fact]
        public void ORSet_must_add_entry()
        {
            target
                .Add(1, VectorTime(1, 0))
                .Value.Should().BeEquivalentTo(1);
        }
        
        [Fact]
        public void ORSet_must_mask_sequential_duplicates()
        {
            target
                .Add(1, VectorTime(1, 0))
                .Add(1, VectorTime(2, 0))
                .Value.Should().BeEquivalentTo(1);
        }
        
        [Fact]
        public void ORSet_must_mask_concurrent_duplicates()
        {
            target
                .Add(1, VectorTime(1, 0))
                .Add(1, VectorTime(0, 1))
                .Value.Should().BeEquivalentTo(1);
        }
        
        [Fact]
        public void ORSet_must_remove_a_pair()
        {
            target
                .Add(1, VectorTime(1, 0))
                .Remove(VectorTime(1, 0))
                .Value.Should().BeEmpty();
        }
        
        [Fact]
        public void ORSet_must_remove_an_entry_by_removing_all_pairs()
        {
            var tmp =
                target
                    .Add(1, VectorTime(1, 0))
                    .Add(1, VectorTime(2, 0));

            tmp
                .Remove(tmp.PrepareRemove(1))
                .Value.Should().BeEmpty();
        }
        
        [Fact]
        public void ORSet_must_keep_an_entry_if_not_all_pairs_are_removed()
        {
            target
                .Add(1, VectorTime(1, 0))
                .Add(1, VectorTime(2, 0))
                .Remove(VectorTime(1, 0))
                .Value.Should().BeEquivalentTo(1);
        }
        
        [Fact]
        public void ORSet_must_add_an_entry_if_concurrent_to_remove()
        {
            target
                .Add(1, VectorTime(1, 0))
                .Remove(VectorTime(1, 0))
                .Add(1, VectorTime(0, 1))
                .Value.Should().BeEquivalentTo(1);
        }
        
        [Fact]
        public void ORSet_must_prepare_remove_set_if_it_contains_the_given_entry()
        {
            target
                .Add(1, VectorTime(1, 0))
                .PrepareRemove(1).Should().BeEquivalentTo(VectorTime(1, 0));
        }
        
        [Fact]
        public void ORSet_must_not_prepare_remove_set_if_does_not_contains_the_given_entry()
        {
            target.PrepareRemove(1).Should().BeEmpty();
        }
    }
}