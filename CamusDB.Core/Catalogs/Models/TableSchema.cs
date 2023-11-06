
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs.Models;

public sealed class TableSchema
{
    public int Version { get; set; }

    public string? Name { get; set; }

    public List<TableColumnSchema>? Columns { get; set; }
}
