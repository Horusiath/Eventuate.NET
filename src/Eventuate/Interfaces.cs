using System;
using System.Collections.Generic;
using System.Text;

namespace Eventuate
{
    public interface ISerializable { }

    public interface IPartiallyComparable<T>
    {
        int? PartiallyCompareTo(T other);
    }

    public interface IPartialComparer<T>
    {
        int? PartiallyCompare(T left, T right);
    }
}
