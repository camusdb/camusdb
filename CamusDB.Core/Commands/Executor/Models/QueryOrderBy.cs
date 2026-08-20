
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// One ORDER BY key: either a stored column, or an expression evaluated per row.
///
/// <para>A clause with a null <see cref="Expression"/> orders by the value already present in the
/// row under <see cref="ColumnName"/>, which is every ordering the engine supported before
/// expression ordering existed. A clause carrying an <see cref="Expression"/> orders by a value that
/// has to be computed — <c>ORDER BY l2_distance(embedding, @q)</c> — and <see cref="ColumnName"/> is
/// then only a label for diagnostics, not a lookup key.</para>
///
/// <para>The distinction is deliberate rather than collapsing both into an expression: a bare column
/// key is resolved to a row ordinal once and compared slot-native, and routing it through an
/// evaluator instead would slow down every ordinary sort in the engine.</para>
/// </summary>
public readonly struct QueryOrderBy
{
    /// <summary>
    /// Row key to compare when <see cref="Expression"/> is null. When an expression is present this
    /// carries a human-readable label for the expression and must not be used as a lookup key.
    /// </summary>
    public string ColumnName { get; }

    public OrderType Type { get; }

    /// <summary>
    /// Per-row expression to evaluate as the sort key, or null when the key is a stored column.
    /// </summary>
    public NodeAst? Expression { get; }

    /// <summary>True when this key must be computed rather than read from the row.</summary>
    public bool IsExpression => Expression is not null;

    public QueryOrderBy(string columnName, OrderType type)
        : this(columnName, type, null)
    {
    }

    public QueryOrderBy(string columnName, OrderType type, NodeAst? expression)
    {
        ColumnName = columnName;
        Type = type;
        Expression = expression;
    }
}
