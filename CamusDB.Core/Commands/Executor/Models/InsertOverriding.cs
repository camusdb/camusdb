/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// The <c>OVERRIDING { SYSTEM | USER } VALUE</c> clause of an <c>INSERT</c>, with PostgreSQL's
/// meaning. It only changes how identity columns (<c>serial</c>, <c>GENERATED … AS IDENTITY</c>)
/// treat a value the statement supplies; every other column is unaffected.
/// </summary>
public enum InsertOverriding
{
    /// <summary>No clause: a supplied value is written, except that a <c>GENERATED ALWAYS</c> column refuses one.</summary>
    None = 0,

    /// <summary>
    /// <c>OVERRIDING SYSTEM VALUE</c>: a value supplied for a <c>GENERATED ALWAYS</c> column is
    /// accepted and written. This is how a dump restores such a column's values. As with any supplied
    /// value, the sequence is not advanced.
    /// </summary>
    SystemValue = 1,

    /// <summary>
    /// <c>OVERRIDING USER VALUE</c>: a value supplied for any identity column is ignored, and the
    /// column draws from its sequence as if it had been omitted.
    /// </summary>
    UserValue = 2,
}

/// <summary>Reads the <see cref="InsertOverriding"/> of a parsed <c>INSERT</c>.</summary>
public static class InsertOverridingClause
{
    /// <summary>
    /// The clause of an <see cref="NodeType.Insert"/> or <see cref="NodeType.InsertSelect"/> node.
    /// The grammar stores it in the node's <c>yytext</c> as <c>"system"</c> or <c>"user"</c>, and
    /// leaves it null when the statement has none.
    /// </summary>
    public static InsertOverriding Of(NodeAst insertAst) => insertAst.yytext switch
    {
        "system" => InsertOverriding.SystemValue,
        "user" => InsertOverriding.UserValue,
        _ => InsertOverriding.None,
    };
}
