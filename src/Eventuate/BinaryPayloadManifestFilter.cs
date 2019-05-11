#region copyright
// -----------------------------------------------------------------------
//  <copyright file="BinaryPayloadManifestFilter.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;

namespace Eventuate
{
    /// <summary>
    /// An <see cref="ReplicationFilter"/> that can be used in combination with
    /// <see cref="DurableEventSerializerWithBinaryPayload"/>.
    /// 
    /// It evaluates to `true` if the payload's manifest matches `regex`.
    /// </summary>
    public sealed class BinaryPayloadManifestFilter : ReplicationFilter
    {
        /// <summary>
        /// Creates a <see cref="BinaryPayloadManifestFilter"/> for the regex given in <paramref name="pattern"/>.
        /// </summary>
        /// <param name="pattern">Compiled, case sensitive string patter.</param>
        public BinaryPayloadManifestFilter(string pattern) : this(new Regex(pattern, RegexOptions.Compiled))
        {
        }

        public BinaryPayloadManifestFilter(Regex regex)
        {
            Regex = regex;
        }

        public Regex Regex { get; }

        public override bool Invoke(DurableEvent durableEvent)
        {
            if (durableEvent.Payload is BinaryPayload payload)
            {
                return Regex.IsMatch(payload.Manifest);
            }
            return false;
        }
    }
}
