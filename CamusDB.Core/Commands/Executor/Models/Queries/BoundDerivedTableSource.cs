
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>
/// A derived-table source after binding and schema inference.
/// </summary>
public sealed record BoundDerivedTableSource(
    DerivedTableSource Source,
    string Alias,
    IReadOnlyList<DerivedColumnSchema> Columns,
    BoundSelectQuery InnerBound)
{
    /// <summary>
    /// The bound operands of the set operation this derived table stands for, or null for any other
    /// derived table. When it is set, <see cref="InnerBound"/> is an empty shell with no sources and
    /// <see cref="Columns"/> equals <see cref="BoundSetOperation.Columns"/>.
    /// </summary>
    public BoundSetOperation? SetOperation { get; init; }

    public bool HasColumn(string columnName)
    {
        foreach (DerivedColumnSchema column in Columns)
        {
            if (string.Equals(column.Name, columnName, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }

    public ColumnType GetColumnType(string columnName)
    {
        foreach (DerivedColumnSchema column in Columns)
        {
            if (string.Equals(column.Name, columnName, StringComparison.OrdinalIgnoreCase))
                return column.Type;
        }

        throw new CamusDBException(CamusDBErrorCodes.UnknownColumn, $"Unknown column: {columnName}");
    }
}
