#region copyright
// -----------------------------------------------------------------------
//  <copyright file="ReplicationFilterSpec.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using FluentAssertions;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace Eventuate.Tests
{
    public class ReplicationFilterSpec
    {
        public class TestFilter : ReplicationFilter
        {
            private readonly string prefix;

            public TestFilter(string prefix)
            {
                this.prefix = prefix;
            }

            public override bool Invoke(DurableEvent durableEvent) =>
                durableEvent.Payload is string s && s.StartsWith(prefix);
        }

        private static ReplicationFilter Filter(string prefix) => new TestFilter(prefix);
        private static DurableEvent Event(string payload) => new DurableEvent(payload, string.Empty);

        [Fact]
        public void ReplicationFilter_should_be_composable_with_logical_AND()
        {
            var f1 = Filter("a").And(Filter("ab"));

            f1.Invoke(Event("abc")).Should().Be(true);
            f1.Invoke(Event("a")).Should().Be(false);
            f1.Invoke(Event("b")).Should().Be(false);

            var f2 = Filter("a").And(Filter("ab")).And(Filter("abc"));

            f2.Invoke(Event("abc")).Should().Be(true);
            f2.Invoke(Event("a")).Should().Be(false);

            var f3 = Filter("a").And(Filter("b"));

            f3.Invoke(Event("abc")).Should().Be(false);
            f3.Invoke(Event("a")).Should().Be(false);

            var f4 = f1.And(f2);

            f4.Invoke(Event("abc")).Should().Be(true);
            f4.Invoke(Event("a")).Should().Be(false);
        }

        [Fact]
        public void ReplicationFilter_should_be_composable_with_logical_OR()
        {
            var f1 = Filter("ab").Or(Filter("xy"));

            f1.Invoke(Event("abc")).Should().Be(true);
            f1.Invoke(Event("xy")).Should().Be(true);
            f1.Invoke(Event("ij")).Should().Be(false);
        }
    }
}
