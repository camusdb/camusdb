
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;

using CamusDB.Core.CommandsExecutor.Controllers.Queries;

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

    /// <summary>
    /// The side products of the subquery stages that ran over <see cref="InnerBound"/>, for the
    /// ticket the inner query runs with (see <see cref="SubqueryBindProducts"/>). Set when the inner
    /// query was bound through the bind pipeline, which is every bind of an executed statement.
    /// Default when it was bound through the binder alone, for its schema (a view definition under
    /// validation): then <see cref="InnerBound"/> may still hold unresolved subquery nodes and must
    /// not run. Null for a set operation, whose operands keep their own on
    /// <see cref="BoundSetOperation"/>, and for a function in FROM, which has no query.
    /// </summary>
    internal SubqueryBindProducts InnerSubqueries { get; init; }

    /// <summary>
    /// The renames from the key of a cell in a row of the inner query to the
    /// <see cref="DerivedColumnSchema.RowKey"/> of its column, or null when every key stays. Set when
    /// the inner query is a <c>*</c> over a join, whose rows key each cell as <c>{alias}.{column}</c>
    /// (see <see cref="DerivedTableSchemaBuilder.WithPlainRowKeys"/>). <see cref="DerivedTableExecutor"/>
    /// renames the keys of each inner row before any reader of the derived table sees it.
    /// </summary>
    internal (string InnerKey, string RowKey)[]? InnerRowKeys { get; init; }

    public bool HasColumn(string columnName)
    {
        foreach (DerivedColumnSchema column in Columns)
        {
            if (string.Equals(column.Name, columnName, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }

    /// <summary>
    /// The key that the cell of the column named <paramref name="columnName"/> has in a row of this
    /// derived table. It differs from the name when the inner query is a <c>*</c> over a join, whose
    /// rows keep each cell under its qualified key, and for a later column with a name that an earlier
    /// column already has.
    ///
    /// <para>A name that two columns of this derived table have is ambiguous, as in PostgreSQL:
    /// <c>SELECT d.a FROM (SELECT j1.a, j2.a FROM j1 JOIN j2 ON …) d</c> names neither of them, so it is
    /// refused instead of reading the first one.</para>
    /// </summary>
    /// <exception cref="CamusDBException"><see cref="CamusDBErrorCodes.UnknownColumn"/> when no column has
    /// the name; <see cref="CamusDBErrorCodes.InvalidInput"/> when two have it.</exception>
    public string RowKeyOf(string columnName, string reference)
    {
        if (TryGetRowKey(columnName, out string rowKey))
            return rowKey;

        throw HasColumn(columnName)
            ? new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Ambiguous column: {reference}")
            : new CamusDBException(CamusDBErrorCodes.UnknownColumn, $"Unknown column: {reference}");
    }

    /// <summary>
    /// The form of <see cref="RowKeyOf"/> for a planner rule that only asks: false when no column, or
    /// more than one column, has the name. The binder already refused such a reference, so a rule that
    /// gets false keeps its general path.
    /// </summary>
    public bool TryGetRowKey(string columnName, out string rowKey)
    {
        string? found = null;

        foreach (DerivedColumnSchema column in Columns)
        {
            if (!string.Equals(column.Name, columnName, StringComparison.OrdinalIgnoreCase))
                continue;

            if (found is not null)
            {
                rowKey = "";
                return false;
            }

            found = column.RowKey;
        }

        rowKey = found ?? "";
        return found is not null;
    }

    /// <summary>
    /// The column whose cell a key from <see cref="QueryRowNameResolver.ResolveRowLookupKey"/> reads, or
    /// null when the key reads no column of this derived table. The resolver gives the row key of the
    /// column (see <see cref="RowKeyOf"/>), alone or behind <c>{alias}.</c> when the query keys its rows by
    /// qualified keys, so the match is on the row key first. A key that equals a column name is the
    /// fallback, for a reader that built the key from the name.
    /// </summary>
    public DerivedColumnSchema? FindColumnByLookupKey(string lookupKey)
    {
        foreach (DerivedColumnSchema column in Columns)
        {
            if (string.Equals(column.RowKey, lookupKey, StringComparison.OrdinalIgnoreCase))
                return column;

            if (lookupKey.Length == Alias.Length + 1 + column.RowKey.Length
                && lookupKey[Alias.Length] == '.'
                && lookupKey.StartsWith(Alias, StringComparison.OrdinalIgnoreCase)
                && lookupKey.EndsWith(column.RowKey, StringComparison.OrdinalIgnoreCase))
                return column;
        }

        foreach (DerivedColumnSchema column in Columns)
        {
            if (string.Equals(column.Name, lookupKey, StringComparison.OrdinalIgnoreCase))
                return column;
        }

        return null;
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
