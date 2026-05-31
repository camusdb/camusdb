
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>
/// Right input to a nested-loop join: base table or derived subquery (QP5.5).
/// </summary>
public sealed record BoundJoinRightSource
{
    public BoundTableSource? Table { get; init; }

    public BoundDerivedTableSource? Derived { get; init; }

    public string Alias => Table?.Alias ?? Derived!.Alias;

    public static BoundJoinRightSource FromTable(BoundTableSource source) => new() { Table = source };

    public static BoundJoinRightSource FromDerived(BoundDerivedTableSource source) => new() { Derived = source };

    public bool HasColumn(string columnName) =>
        Table is not null
            ? TableHasColumn(Table, columnName)
            : Derived!.HasColumn(columnName);

    private static bool TableHasColumn(BoundTableSource source, string columnName)
    {
        foreach (TableColumnSchema column in source.Table.Schema.Columns ?? [])
        {
            if (column.Name == columnName)
                return true;
        }

        return false;
    }
}
