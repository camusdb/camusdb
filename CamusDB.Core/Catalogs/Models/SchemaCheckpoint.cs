
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs.Models;

public sealed class SchemaCheckpoint
{
    public int FormatVersion { get; set; } = 1;

    public long SchemaVersion { get; set; }

    public Dictionary<string, TableSchema> Tables { get; set; } = new();
}
