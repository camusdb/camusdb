
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.Trees;
using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Models;

public sealed class TableDescriptor
{
    public string? Name { get; set; }

    public TableSchema? Schema { get; set; }

    public BTree<int> Rows { get; set; } = new(-1);

    public Dictionary<string, TableIndexSchema> Indexes { get; set; } = new();

    //public SemaphoreSlim WriteLock { get; } = new(1, 1);
}
