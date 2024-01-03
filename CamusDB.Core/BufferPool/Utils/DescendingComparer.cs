
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.BufferPool.Utils;

public sealed class DescendingComparer<T> : IComparer<T> where T : IComparable<T>
{
    public int Compare(T? x, T? y)
    {
        if (x is null && y is null)
            return 0;

        if (x is null && y is not null)
            return -1;

        if (x is not null && y is null)
            return 1;

        if (x is not null && y is not null)
            return y.CompareTo(x);

        return 0;
    }
}
