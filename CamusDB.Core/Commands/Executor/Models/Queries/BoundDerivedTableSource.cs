
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
