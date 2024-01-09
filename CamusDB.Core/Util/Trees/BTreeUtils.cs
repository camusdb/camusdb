
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Util.Trees;

public class BTreeUtils
{
    /// <summary>
    /// Calculates  
    /// </summary>
    /// <returns></returns>
    public static int GetNodeCapacity<TKey, TValue>()
    {
        int nodeCapacity = (4096 - 16) / (GetItemSize<TKey>() + GetItemSize<TValue>());

        if (nodeCapacity % 2 != 0)
            nodeCapacity--;

        return nodeCapacity;
    }

    private static int GetItemSize<T>()
    {
        return 32;

        /*return typeof(T) switch
        {
            Type t when t == typeof(string) => 62,
            Type t when t == typeof(long) => 8,
            Type t when t == typeof(int) => 4,
            Type t when t == typeof(ColumnValue) => 48,
            Type t when t == typeof(BTreeTuple) => 24,
            Type t when t == typeof(ObjectIdValue) => 12,
            Type t when t == typeof(int?) => 8,
            _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Unknown type " + typeof(T).Name),
        };*/
    }
}
