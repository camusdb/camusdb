
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>
/// A derived-table source after binding and schema inference (QP5.5).
/// </summary>
public sealed record BoundDerivedTableSource(
    DerivedTableSource Source,
    string Alias,
    IReadOnlyList<DerivedColumnSchema> Columns,
    BoundSelectQuery InnerBound)
{
    public bool HasColumn(string columnName)
    {
        foreach (DerivedColumnSchema column in Columns)
        {
            if (column.Name == columnName)
                return true;
        }

        return false;
    }

    public ColumnType GetColumnType(string columnName)
    {
        foreach (DerivedColumnSchema column in Columns)
        {
            if (column.Name == columnName)
                return column.Type;
        }

        throw new CamusDBException(CamusDBErrorCodes.UnknownColumn, $"Unknown column: {columnName}");
    }
}
