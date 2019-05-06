using System;
using Akka.Streams.Util;
using Eventuate.Crdt;
using FluentAssertions;
using Xunit;

namespace Eventuate.Tests.Crdt
{
    public class LWWRegisterSpec
    {
        private readonly LWWRegister<int> target = LWWRegister<int>.Empty;

        private static VectorTime VectorTime(int a, int b) => new VectorTime(("p1", a), ("p2", b));

        [Fact]
        public void LWWRegister_must_not_have_value_by_default()
        {
            target.Value.HasValue.Should().Be(false);
        }
        
        [Fact]
        public void LWWRegister_must_store_single_value()
        {
            target
                .Assign(1, VectorTime(1, 0), default, "source-1")
                .Value.Should().Be(new Option<int>(1));
        }

        [Fact]
        public void LWWRegister_must_accept_new_value_if_was_set_after_the_current_value_according_to_the_vector_clock()
        {
            target
                .Assign(1, VectorTime(1, 0), DateTime.Now, "source-1")
                .Assign(2, VectorTime(2, 0), default, "source-1")
                .Value.Should().Be(new Option<int>(2));
        }

        [Fact]
        public void LWWRegister_must_fallback_to_the_wall_clock_if_the_values_vector_clocks_are_concurrent()
        {
            target
                .Assign(1, VectorTime(1, 0), default, "source-1")
                .Assign(2, VectorTime(0, 1), DateTime.UtcNow, "source-2")
                .Value.Should().Be(new Option<int>(2));
            
            target
                .Assign(1, VectorTime(1, 0), DateTime.UtcNow, "source-1")
                .Assign(2, VectorTime(0, 1), default, "source-2")
                .Value.Should().Be(new Option<int>(1));
        }

        [Fact]
        public void LWWRegister_must_fallback_to_the_greates_emitter_if_the_values_vector_clocks_and_wall_clocks_are_concurrent()
        {
            target
                .Assign(1, VectorTime(1, 0), default, "source-1")
                .Assign(2, VectorTime(0, 1), default, "source-2")
                .Value.Should().Be(new Option<int>(2));
            
            target
                .Assign(1, VectorTime(1, 0), default, "source-1")
                .Assign(2, VectorTime(0, 1), default, "source-2")
                .Value.Should().Be(new Option<int>(1));
        }
    }
}