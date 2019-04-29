using Eventuate.ReplicationProtocol;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Threading.Tasks;

namespace Eventuate
{
    internal static class CollectionExtensions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Deconstruct<TKey, TValue>(this KeyValuePair<TKey, TValue> entry, out TKey key, out TValue value)
        {
            key = entry.Key;
            value = entry.Value;
        }
    }

    internal readonly struct Void{}

    internal interface IFailure<TException> where TException : Exception
    {
        TException Cause { get; }
    }

    internal static class TasksExtensions
    {
        public static async Task<T> Unwrap<T, TException>(this Task<object> task) where TException : Exception
        {
            var response = await task;
            if (response is IFailure<TException> f)
            {
                ExceptionDispatchInfo.Capture(f.Cause).Throw();
                return default;
            }
            else return (T)response;
        }
    }
}
