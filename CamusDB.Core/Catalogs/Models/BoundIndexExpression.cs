/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// One expression key part of an expression index, bound to the current column names of its table.
/// It is the in-memory form that <c>TableOpener</c> builds from an <see cref="IndexKeyExpression"/>,
/// held by the in-memory <see cref="TableIndexSchema"/> at the same key position.
///
/// <para><b>The hidden key name.</b> At this position, <see cref="TableIndexSchema.Columns"/> holds
/// <see cref="KeyName"/>, a name that no SQL identifier can spell. The write paths put the value of
/// <see cref="Expression"/> into that slot of the key, and the planner turns a WHERE term that matches
/// <see cref="Canonical"/> into a comparison on that name. Every other path that looks the name up in a
/// row or in the table columns finds nothing, which is the safe answer for a plain-column question.
/// Two indexes over the same expression share the name.</para>
/// </summary>
public sealed class BoundIndexExpression
{
    /// <summary>The hidden key name at this position of <see cref="TableIndexSchema.Columns"/>.</summary>
    public string KeyName { get; }

    /// <summary>The expression, with each column reference written as the current column name.</summary>
    public NodeAst Expression { get; }

    /// <summary>The expression rendered with current column names, for SHOW CREATE TABLE and EXPLAIN.</summary>
    public string Sql { get; }

    /// <summary>
    /// The text two spellings of the same expression share: names in lower case, no table qualifier,
    /// no redundant parentheses. String literals keep their case.
    /// </summary>
    public string Canonical { get; }

    /// <summary>Result type of the expression, as stored in <see cref="IndexKeyExpression.Type"/>.</summary>
    public ColumnType Type { get; }

    /// <summary>Current names of the columns the expression reads, without duplicates.</summary>
    public string[] ReferencedColumns { get; }

    /// <summary>
    /// A column shape for the hidden key name: <see cref="KeyName"/> with the result type, nullable.
    /// The planner's constant coercion looks a comparison's name up as a column; for a matched
    /// expression it finds this one, so a constant moves into the expression's type by the same rules
    /// as for a column of that type. It is never part of a table's columns.
    /// </summary>
    public TableColumnSchema KeyColumn { get; }

    public BoundIndexExpression(string keyName, NodeAst expression, string sql, string canonical, ColumnType type, string[] referencedColumns)
    {
        KeyName = keyName;
        Expression = expression;
        Sql = sql;
        Canonical = canonical;
        Type = type;
        ReferencedColumns = referencedColumns;
        KeyColumn = new TableColumnSchema(keyName, keyName, type, notNull: false, defaultValue: null);
    }
}
