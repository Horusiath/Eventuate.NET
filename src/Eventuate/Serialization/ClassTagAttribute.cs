#region copyright
// -----------------------------------------------------------------------
//  <copyright file="ClassTagAttribute.cs" company="Akka.NET Project">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.Collections.Generic;
using System.Reflection;

namespace Eventuate.Serialization
{
    [AttributeUsage(AttributeTargets.Class|AttributeTargets.Enum|AttributeTargets.Struct, Inherited = false, AllowMultiple = false)]
    public class ClassTagAttribute : Attribute
    {
        private static readonly Dictionary<int, Type> taggedToType = new Dictionary<int, Type>();
        private static readonly Dictionary<Type, int> typeToTagged = new Dictionary<Type, int>();
        
        static ClassTagAttribute()
        {
            foreach (var assembly in Assembly.GetExecutingAssembly().GetReferencedAssemblies())
            foreach (var type in Assembly.Load(assembly).DefinedTypes)
            {
                var attr = type.GetCustomAttribute<ClassTagAttribute>();
                var tag = attr?.Tag;
                if (tag.HasValue)
                {
                    taggedToType[tag.Value] = type;
                    typeToTagged[type] = tag.Value;
                }
            }
        }
        
        internal static int ValueFor(Type type)
        {
            if (typeToTagged.TryGetValue(type, out var tag))
                return tag;
            
            throw new NotSupportedException($"Couldn't find a [{nameof(ClassTagAttribute)}(<value>)] on a provided type '{type.FullName}'. Annotate that type with a correct {nameof(ClassTagAttribute)}.");
        }

        internal static Type TypeFor(int tag)
        {
            if (taggedToType.TryGetValue(tag, out var type))
                return type;
            
            throw new NotSupportedException($"Couldn't found any type annotated with {nameof(ClassTagAttribute)} among types in loaded assemblies with a tag value of '{tag}'");
        }
        
        public ClassTagAttribute(int tag)
        {
            Tag = tag;
        }

        public int Tag { get; }
    }
}