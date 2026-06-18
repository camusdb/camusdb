/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// Central authority for what an online-schema <see cref="SchemaElementState"/> permits.
/// Every read/write/DDL path consults these helpers instead of comparing states inline, so
/// the visibility/writability rules live in exactly one place. The <c>...Index</c> overloads
/// additionally require <i>all</i> of the index's columns to satisfy the same rule, which is
/// what callers (query planner, scanners, insert/update/delete, <c>SHOW</c>) use.
/// See the architecture documentation.
/// </summary>
public static class SchemaElementStateRules
{
    /// <summary>User-facing reads may surface this column (only when <see cref="SchemaElementState.Public"/>).</summary>
    public static bool IsReadable(TableColumnSchema column) => column.State == SchemaElementState.Public;

    /// <summary>DML may write this column (<see cref="SchemaElementState.WriteOnly"/> or <see cref="SchemaElementState.Public"/>).</summary>
    public static bool IsWritable(TableColumnSchema column) =>
        column.State is SchemaElementState.WriteOnly or SchemaElementState.Public;

    /// <summary>User-facing reads/plans may use this index (only when <see cref="SchemaElementState.Public"/>).</summary>
    public static bool IsReadable(TableIndexSchema index) => index.State == SchemaElementState.Public;

    /// <summary>DML must maintain this index (<see cref="SchemaElementState.WriteOnly"/> or <see cref="SchemaElementState.Public"/>).</summary>
    public static bool IsWritable(TableIndexSchema index) =>
        index.State is SchemaElementState.WriteOnly or SchemaElementState.Public;

    /// <summary>True when the index and <i>every</i> column it covers are readable.</summary>
    public static bool IsReadableIndex(TableSchema table, TableIndexSchema index)
    {
        if (!IsReadable(index))
            return false;

        return index.Columns.All(columnName =>
            table.Columns?.Any(column => column.Name == columnName && IsReadable(column)) == true
        );
    }

    /// <summary>True when the index and <i>every</i> column it covers are writable.</summary>
    public static bool IsWritableIndex(TableSchema table, TableIndexSchema index)
    {
        if (!IsWritable(index))
            return false;

        return index.Columns.All(columnName =>
            table.Columns?.Any(column => column.Name == columnName && IsWritable(column)) == true
        );
    }
}
