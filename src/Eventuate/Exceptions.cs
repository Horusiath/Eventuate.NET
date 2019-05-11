#region copyright
// -----------------------------------------------------------------------
//  <copyright file="Exceptions.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

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
