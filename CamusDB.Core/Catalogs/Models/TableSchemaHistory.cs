
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// Represents a version of the table schema in the version history.
/// </summary>
public sealed class TableSchemaHistory
{
    public int Version { get; set; }

    public List<TableColumnSchema>? Columns { get; set; }
}
