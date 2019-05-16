#region copyright
// -----------------------------------------------------------------------
//  <copyright file="Try.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace Eventuate
{
    public static class Try
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Try<T> Success<T>(T value) => new Try<T>(value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Try<T> Failure<T>(Exception exception) => new Try<T>(exception);

        public static Try<T> AsTry<T>(this Task<T> task)
        {
            if (task.IsFaulted) return Failure<T>(task.Exception);
            if (task.IsCanceled) return Failure<T>(new TaskCanceledException(task));
            else return Success(task.Result);
        }
    }

    public readonly struct Try<T>
    {
        private readonly Exception exception;
        private readonly T value;

        public Try(T value)
        {
            this.exception = null;
            this.value = value;
        }

        public Try(Exception exception)
        {
            this.exception = exception;
            this.value = default;
        }

        internal Try(T value, Exception exception)
        {
            this.exception = exception;
            this.value = value;
        }

        public bool IsSuccess
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => this.exception is null;
        }

        public bool IsFailure
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => !(this.exception is null);
        }

        public T Value
        {
            [MethodImpl(MethodImplOptions.NoInlining)]
            get
            {
                if (this.exception is null)
                    return this.value;

                throw new InvalidOperationException("Tried to get value from failed Try wrapper", this.exception);
            }
        }

        public Exception Exception
        {
            [MethodImpl(MethodImplOptions.NoInlining)]
            get
            {
                if (this.exception is null)
                    throw new InvalidOperationException("Tried to get exception from successfully resolved Try wrapper", this.exception);

                return this.exception;
            }
        }

        public bool TryGetValue(out T value)
        {
            if (exception is null)
            {
                value = this.value;
                return true;
            }
            else
            {
                value = default;
                return false;
            }
        }

        public T GetValueOrDefault() => exception is null ? value : default;

        public T GetValueOrDefault(T defaultValue) => exception is null ? value : defaultValue;

        internal Try<T2> Cast<T2>() where T2 : T => new Try<T2>((T2)this.value, this.exception);
    }
}
