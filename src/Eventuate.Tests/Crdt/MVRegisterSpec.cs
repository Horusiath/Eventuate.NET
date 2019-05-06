using Eventuate.Crdt;
using System;
using System.Collections.Generic;
using System.Text;
using FluentAssertions;
using Xunit;

namespace Eventuate.Tests.Crdt
{
    public class MVRegisterSpec
    {
        private readonly MVRegister<int> target = MVRegister<int>.Empty;

        private static VectorTime VectorTime(int a, int b) => new VectorTime(("p1", a), ("p2", b));

        [Fact]
        public void MVRegister_must_not_have_value_set_by_default()
        {
            target.Value.Should().BeEmpty();
        }
        
        [Fact]
        public void MVRegister_must_store_single_value()
        {
            target
                .Assign(1, VectorTime(1, 0))
                .Value.Should().BeEquivalentTo(1);
        }
        
        [Fact]
        public void MVRegister_must_store_multiple_values_in_case_of_concurrent_updates()
        {
            target
                .Assign(1, VectorTime(1, 0))
                .Assign(2, VectorTime(0, 1))
                .Value.Should().BeEquivalentTo(1, 2);
        }
        
        [Fact]
        public void MVRegister_must_mask_duplicate_concurrent_values()
        {
            target
                .Assign(1, VectorTime(1, 0))
                .Assign(1, VectorTime(0, 1))
                .Value.Should().BeEquivalentTo(1);
        }
        
        [Fact]
        public void MVRegister_must_replace_value_if_it_happened_before_new_write()
        {
            target
                .Assign(1, VectorTime(1, 0))
                .Assign(2, VectorTime(2, 0))
                .Value.Should().BeEquivalentTo(2);
        }
        
        [Fact]
        public void MVRegister_must_replace_value_if_it_happened_before_new_write_and_retain_value_if_it_is_concurrent_to_the_new_write()
        {
            target
                .Assign(1, VectorTime(1, 0))
                .Assign(2, VectorTime(0, 1))
                .Assign(3, VectorTime(2, 0))
                .Value.Should().BeEquivalentTo(2, 3);
        }
        
        [Fact]
        public void MVRegister_must_replace_multiple_concurrent_values_if_they_happened_before_new_write()
        {
            target
                .Assign(1, VectorTime(1, 0))
                .Assign(2, VectorTime(0, 1))
                .Assign(3, VectorTime(1, 1))
                .Value.Should().BeEquivalentTo(3);
        }
    }
}
