#region copyright
// -----------------------------------------------------------------------
//  <copyright file="ORCartSpec.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System.Collections.Generic;
using Eventuate.Crdt;
using FluentAssertions;
using Xunit;

namespace Eventuate.Tests.Crdt
{
    public class ORCartSpec
    {
        private readonly ORCart<string> target = ORCart<string>.Empty;

        private static VectorTime VectorTime(int a, int b) => new VectorTime(("p1", a), ("p2", b));
        private static KeyValuePair<string, int> Pair(string key, int value) => new KeyValuePair<string, int>(key, value);

        [Fact]
        public void ORCart_must_be_empty_by_default()
        {
            target.Value.Should().BeEmpty();
        }

        [Fact]
        public void ORCart_must_set_initial_entry_quantities()
        {
            target
                .Add("a", 2, VectorTime(1, 0))
                .Add("b", 3, VectorTime(2, 0))
                .Value.Should().BeEquivalentTo(new Dictionary<string, int>
                {
                    {"a", 2},
                    {"b", 3}
                });
        }
        
        [Fact]
        public void ORCart_must_increment_existing_entry_quantities()
        {
            target
                .Add("a", 1, VectorTime(1, 0))
                .Add("b", 3, VectorTime(2, 0))
                .Add("a", 1, VectorTime(3, 0))
                .Add("b", 1, VectorTime(4, 0))
                .Value.Should().BeEquivalentTo(new Dictionary<string, int>
                {
                    {"a", 2},
                    {"b", 4}
                });
        }
        
        [Fact]
        public void ORCart_must_remove_observed_entries()
        {
            target
                .Add("a", 2, VectorTime(1, 0))
                .Add("b", 3, VectorTime(2, 0))
                .Add("a", 1, VectorTime(3, 0))
                .Add("b", 1, VectorTime(4, 0))
                .Remove(VectorTime(1, 0), VectorTime(2, 0), VectorTime(3, 0))
                .Value.Should().BeEquivalentTo(new Dictionary<string, int>
                {
                    {"b", 1}
                });
        }
    }
}