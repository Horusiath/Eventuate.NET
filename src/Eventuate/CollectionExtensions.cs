using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;

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
}
