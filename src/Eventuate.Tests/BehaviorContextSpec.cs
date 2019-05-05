using Akka.Actor;
using FluentAssertions;
using Xunit;

namespace Eventuate.Tests
{
    public class BehaviorContextSpec
    {
        private BehaviorContext context;
        private int state = 0;

        private Receive b1;
        private Receive b2;
        private Receive b3;

        public BehaviorContextSpec()
        {
            this.b1 = _ =>
            {
                this.state = 1;
                return true;
            };
            this.b2 = _ =>
            {
                this.state = 2;
                return true;
            };
            this.b3 = _ =>
            {
                this.state = 3;
                return true;
            };
            this.context = new BehaviorContext(b1);
        }

        private int Run()
        {
            context.Current(0);
            return this.state;
        }

        [Fact]
        public void BehaviorContext_must_have_an_initial_behavior()
        {
            Run().Should().Be(1);
        }

        [Fact]
        public void BehaviorContext_must_add_and_remove_behavior()
        {
            context.Become(b2, replace: false);
            Run().Should().Be(2);
            context.Become(b3, replace: false);
            Run().Should().Be(3);
            context.Unbecome();
            Run().Should().Be(2);
            context.Unbecome();
            Run().Should().Be(1);
        }

        [Fact]
        public void BehaviorContext_must_replace_behavior_and_revert_to_initial_behavior()
        {
            context.Become(b2);
            Run().Should().Be(2);
            context.Become(b3);
            Run().Should().Be(3);
            context.Unbecome();
            Run().Should().Be(1);
        }
    }
}