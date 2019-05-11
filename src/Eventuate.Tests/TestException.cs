#region copyright
// -----------------------------------------------------------------------
//  <copyright file="TestException.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;

namespace Eventuate.Tests
{
    internal class TestException : Exception
    {
        public static readonly TestException Instance = new TestException();
        private TestException() : base() { }
    }
    
    internal readonly struct Ping : IEquatable<Ping>
    {
        public int I { get; }

        public Ping(int i)
        {
            I = i;
        }

        public bool Equals(Ping other)
        {
            return I == other.I;
        }

        public override bool Equals(object obj)
        {
            return obj is Ping other && Equals(other);
        }

        public override int GetHashCode()
        {
            return I;
        }
    }

    internal readonly struct Pong : IEquatable<Pong>
    {
        public int I { get; }

        public Pong(int i)
        {
            I = i;
        }

        public bool Equals(Pong other)
        {
            return I == other.I;
        }

        public override bool Equals(object obj)
        {
            return obj is Pong other && Equals(other);
        }

        public override int GetHashCode()
        {
            return I;
        }
    }

}