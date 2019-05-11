#region copyright
// -----------------------------------------------------------------------
//  <copyright file="Interfaces.cs" company="Bartosz Sypytkowski">
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
