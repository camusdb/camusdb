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

        List<TableColumnSchema>? columns = table.Columns;
        if (columns is null)
            return false;

        // Plain loops (no LINQ): this runs inside planner and DML loops, so it must not allocate
        // closures or enumerators per call.
        foreach (string columnName in index.Columns)
        {
            bool readable = false;
            for (int i = 0; i < columns.Count; i++)
            {
                TableColumnSchema column = columns[i];
                if (string.Equals(column.Name, columnName, StringComparison.OrdinalIgnoreCase) && IsReadable(column))
                {
                    readable = true;
                    break;
                }
            }

            if (!readable)
                return false;
        }

        return true;
    }

    /// <summary>True when the index and <i>every</i> column it covers are writable.</summary>
    public static bool IsWritableIndex(TableSchema table, TableIndexSchema index)
    {
        if (!IsWritable(index))
            return false;

        List<TableColumnSchema>? columns = table.Columns;
        if (columns is null)
            return false;

        // Plain loops (no LINQ): this runs inside DML per-row loops, so it must not allocate
        // closures or enumerators per call.
        foreach (string columnName in index.Columns)
        {
            bool writable = false;
            for (int i = 0; i < columns.Count; i++)
            {
                TableColumnSchema column = columns[i];
                if (string.Equals(column.Name, columnName, StringComparison.OrdinalIgnoreCase) && IsWritable(column))
                {
                    writable = true;
                    break;
                }
            }

            if (!writable)
                return false;
        }

        return true;
    }

    /// <summary>
    /// Materializes the subset of <paramref name="indexes"/> that DML must maintain (per
    /// <see cref="IsWritableIndex"/>) so per-row loops iterate a precomputed list instead of
    /// re-evaluating writability for every index of every row. Writability is a pure function of
    /// the schema's element states, which are fixed for the duration of a statement (the
    /// transaction pins the schema version) — but the result must not be cached across
    /// statements: compute it once per statement or chunk.
    /// </summary>
    public static List<TableIndexSchema> CollectWritableIndexes(TableSchema table, Dictionary<string, TableIndexSchema> indexes)
    {
        List<TableIndexSchema> result = new(indexes.Count);

        foreach (KeyValuePair<string, TableIndexSchema> kv in indexes)
        {
            if (IsWritableIndex(table, kv.Value))
                result.Add(kv.Value);
        }

        return result;
    }
}
