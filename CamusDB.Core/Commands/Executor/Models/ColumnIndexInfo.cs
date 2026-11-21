
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// One key part of an index that a statement asks for: a column, or an expression of an expression
/// index (<c>CREATE INDEX i ON t ((lower(email)))</c>).
///
/// <para>An expression part carries its SQL text only, with column names as the user wrote them,
/// because a ticket is built before its table is open. The add-index paths resolve the text against
/// the table (<c>IndexExpressionBinder.Resolve</c>), which validates it and binds it to column ids. The
/// text, and not an AST, is what travels to the schema leader when a follower forwards the
/// statement.</para>
/// </summary>
public sealed class ColumnIndexInfo
{
    /// <summary>The column name, or an empty string for an expression part.</summary>
    public string Name { get; }

    public OrderType Order { get; }

    /// <summary>
    /// The SQL text of the expression at this key position, or null when the part is a column.
    /// </summary>
    public string? Expression { get; }

    /// <summary>True when this key part is an expression rather than a column.</summary>
    public bool IsExpression => Expression is not null;

    public ColumnIndexInfo(string name, OrderType order)
	{
        Name = name;
        Order = order;
	}

    public ColumnIndexInfo(string name, OrderType order, string? expression)
    {
        Name = expression is null ? name : "";
        Order = order;
        Expression = expression;
    }
}
