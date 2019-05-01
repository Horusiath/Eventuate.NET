using System;
using System.Collections.Generic;
using System.Text;

namespace Eventuate
{
    /// <summary>
    /// Thrown to indicate that a `Stash` operation was used at an illegal location.
    /// </summary>
    public class StashException : Exception
    {
        public StashException() { }
        public StashException(string message) : base(message) { }
        public StashException(string message, Exception inner) : base(message, inner) { }
        protected StashException(
          System.Runtime.Serialization.SerializationInfo info,
          System.Runtime.Serialization.StreamingContext context) : base(info, context) { }
    }
}
