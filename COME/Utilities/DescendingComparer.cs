using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace COME.Utilities
{
    class DescendingComparer<T> : IComparer<T> where T : IComparable<T>
    {
        public int Compare(T x, T y)
        {
            return y.CompareTo(x);
        }
    }

}
