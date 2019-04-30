using Akka.Actor;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Eventuate
{
    /// <summary>
    /// Provides a context for managing behaviors.
    /// </summary>
    public sealed class BehaviorContext
    {
        private Receive current;
        private Stack<Receive> behaviorStack = null;

        public BehaviorContext(Receive initial)
        {
            this.current = this.Initial = initial;
        }

        /// <summary>
        /// The initial behavior.
        /// </summary>
        public Receive Initial { get; }

        /// <summary>
        /// The current behavior.
        /// </summary>
        public Receive Current
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => current;
        }

        /// <summary>
        /// Sets the given <paramref name="behavior"/> as <see cref="Current"/> behavior.
        /// </summary>
        /// <param name="behavior">The behavior to set.</param>
        /// <param name="replace">If `true` (default) replaces the `current` behavior on the behavior stack, if `false` pushes `behavior` on the behavior stack.</param>
        public void Become(Receive behavior, bool replace = true)
        {
            if (replace)
            {
                if (behaviorStack is null)
                    behaviorStack = new Stack<Receive>();
                behaviorStack.Push(current);
            }

            current = behavior;
        }

        /// <summary>
        /// Pops the current `behavior` from the behavior stack, making the previous behavior the `current` behavior.
        /// If the behavior stack contains only a single element, the `current` behavior is reverted to the `initial`
        /// behavior.
        /// </summary>
        public void Unbecome()
        {
            if (behaviorStack is null || behaviorStack.Count == 0)
                current = Initial;
            else
                current = behaviorStack.Pop();
        }
    }
}
