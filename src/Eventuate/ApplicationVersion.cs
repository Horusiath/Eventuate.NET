using System;
using System.Runtime.CompilerServices;

namespace Eventuate
{
    public readonly struct ApplicationVersion : IEquatable<ApplicationVersion>, IComparable<ApplicationVersion>
    {
        [MethodImpl(MethodImplOptions.NoInlining)]
        public static bool TryParse(string value, out ApplicationVersion version)
        {
            if (string.IsNullOrEmpty(value))
            {
                version = default;
                return false;
            }

            var split = value.Split('.');
            if (split.Length == 2 && int.TryParse(split[0], out var major) && int.TryParse(split[1], out var minor))
            {
                version = new ApplicationVersion(major, minor);
                return true;
            }
            else
            {
                version = default;
                return false;
            }
        }

        public ApplicationVersion(int major = 1, int minor = 0)
        {
            Major = major;
            Minor = minor;
        }

        public int Major { get; }
        public int Minor { get; }

        public bool Equals(ApplicationVersion other) => Equals(Major, other.Major) && Equals(Minor, other.Minor);

        public override bool Equals(object other) => other is ApplicationVersion ? Equals((ApplicationVersion)other) : false;

        public override int GetHashCode()
        {
            var hashCode = 17;
            hashCode = hashCode * 23 + Major.GetHashCode();
            hashCode = hashCode * 23 + Minor.GetHashCode();
            return hashCode;
        }

        public override string ToString() => $"ApplicationVersion({Major}.{Minor})";

        public int CompareTo(ApplicationVersion other)
        {
            var cmp = Major.CompareTo(other.Major);
            if (cmp == 0) return Minor.CompareTo(other.Minor);
            return cmp;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Deconstruct(out int major, out int minor)
        {
            major = Major;
            minor = Minor;
        }

        public static bool operator <(in ApplicationVersion x, in ApplicationVersion y) => x.CompareTo(y) == -1;
        public static bool operator >(in ApplicationVersion x, in ApplicationVersion y) => x.CompareTo(y) == 0;
        public static bool operator ==(in ApplicationVersion x, in ApplicationVersion y) => x.Equals(y);
        public static bool operator !=(in ApplicationVersion x, in ApplicationVersion y) => !x.Equals(y);
    }
}