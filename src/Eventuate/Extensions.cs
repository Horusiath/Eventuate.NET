#region copyright
// -----------------------------------------------------------------------
//  <copyright file="Extensions.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using Eventuate.ReplicationProtocol;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
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

        public static bool CollectionEquals<T>(this IReadOnlyCollection<T> a, IReadOnlyCollection<T> b)
            where T: IEquatable<T>
        {
            if (a.Count != b.Count) return false;
            
            using (var e1 = a.GetEnumerator())
            using (var e2 = b.GetEnumerator())
            {
                while (e1.MoveNext() && e2.MoveNext())
                {
                    if (!e1.Current.Equals(e2.Current)) return false;
                }
            }

            return true;
        }
        
        public static bool DictionaryEquals<TKey, TValue>(this ImmutableDictionary<TKey,TValue> a, ImmutableDictionary<TKey,TValue> b)
            where TKey: IEquatable<TKey>
            where TValue: IEquatable<TValue>
        {
            if (a.Count != b.Count) return false;

            foreach (var key in a.Keys.Union(b.Keys))
            {
                if (!a.TryGetValue(key, out var va) || !b.TryGetValue(key, out var vb) || !va.Equals(vb)) return false;
            }

            return true;
        }

        public static int GetDictionaryHashCode<TKey, TValue>(this ImmutableDictionary<TKey, TValue> a)
            where TKey : IEquatable<TKey>
            where TValue : IEquatable<TValue>
        {
            unchecked
            {
                var hash = 0;
                foreach (var (k,v) in a)
                {
                    hash = (hash * 397) ^ k.GetHashCode() ^ v.GetHashCode();
                }
                return hash;
            }
        }
        
        public static int GetCollectionHashCode<T>(this IReadOnlyCollection<T> a)
            where T: IEquatable<T>
        {
            unchecked
            {
                var hash = 0;
                foreach (var x in a)
                {
                    hash = (hash * 397) ^ x.GetHashCode();
                }
                return hash;
            }
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
