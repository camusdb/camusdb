
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.App.Models;

public sealed class CreateTableColumn
{
    public string? Name { get; set; }

    public string? Type { get; set; }

    public bool NotNull { get; set; }

    /// <summary>
    /// True for a column of the primary key. The engine requires a primary key, so at least one
    /// column must set it. Several columns form a composite key, in declaration order.
    /// </summary>
    public bool Primary { get; set; }

    public ColumnValue? DefaultValue { get; set; }

    /// <summary>
    /// Maximum length in characters (string) or bytes (bytes/blob). Null = default cap.
    /// </summary>
    public int? MaxLength { get; set; }

    /// <summary>
    /// Element type keyword for array columns (e.g. "int64", "string"). Null for non-array types.
    /// </summary>
    public string? ArrayElementType { get; set; }
}
