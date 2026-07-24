
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models;

public sealed class ConstraintInfo
{    
    public ConstraintType Type { get; }

    public string Name { get; }

    public ColumnIndexInfo[] Columns { get; }

    /// <summary>
    /// Names of stored/payload (INCLUDE) columns for a covering secondary index declared inline in
    /// CREATE TABLE. Empty for a plain index, a primary key, or a check constraint. These are
    /// materialized into the index entry value, never part of the key.
    /// </summary>
    public string[] IncludeColumns { get; }

    public ConstraintInfo(ConstraintType type, string name, ColumnIndexInfo[] columns, string[]? includeColumns = null)
	{
        Name = name;
        Type = type;
        Columns = columns;
        IncludeColumns = includeColumns ?? [];
	}
}
