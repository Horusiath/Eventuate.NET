using System;

namespace Eventuate.Tests
{
    public class TestException : Exception
    {
        public static readonly TestException Instance = new TestException();
        private TestException() : base() { }
    }
}