using FluentAssertions;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace Eventuate.Tests
{
    public class ApplicationVersionSpec
    {
        [Fact]
        public void ApplicationVersion_should_be_equal()
        {
            (ApplicationVersion.Parse("1.0") == ApplicationVersion.Parse("1.0")).Should().Be(true);
            (ApplicationVersion.Parse("1.0") == ApplicationVersion.Parse("1.1")).Should().Be(false);
        }

        [Fact]
        public void ApplicationVersion_should_be_less_than()
        {
            (ApplicationVersion.Parse("0.9") < ApplicationVersion.Parse("1.0")).Should().Be(true);
            (ApplicationVersion.Parse("1.0") < ApplicationVersion.Parse("0.9")).Should().Be(false);
        }

        [Fact]
        public void ApplicationVersion_should_be_greater_than()
        {
            (ApplicationVersion.Parse("1.0") >= ApplicationVersion.Parse("1.0")).Should().Be(true);
            (ApplicationVersion.Parse("1.1") >= ApplicationVersion.Parse("1.0")).Should().Be(true);
            (ApplicationVersion.Parse("0.9") >= ApplicationVersion.Parse("1.0")).Should().Be(false);
        }
    }
}
