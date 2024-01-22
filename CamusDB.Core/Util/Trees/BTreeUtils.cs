
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
        return typeof(T) switch
        {
            Type t when t == typeof(string) => 65,
            Type t when t == typeof(long) => 9,
            Type t when t == typeof(int) => 8,
            Type t when t == typeof(ColumnValue) => 49,
            Type t when t == typeof(CompositeColumnValue) => 49,
            Type t when t == typeof(BTreeTuple) => 25,
            Type t when t == typeof(ObjectIdValue) => 13,
            Type t when t == typeof(int?) => 9,
            _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Unknown type " + typeof(T).Name),
        };
    }
}
