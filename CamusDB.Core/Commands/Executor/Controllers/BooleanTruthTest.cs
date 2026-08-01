
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Evaluates the <c>IS [NOT] TRUE</c> / <c>IS [NOT] FALSE</c> truth tests.
///
/// <para>These are deliberately not sugar for <c>= TRUE</c> / <c>= FALSE</c>. A truth test is
/// total: it always answers true or false and never yields unknown, so a NULL operand makes
/// <c>IS TRUE</c> false and — crucially — makes <c>IS NOT TRUE</c> <em>true</em>. The negated forms
/// therefore match NULL rows, which an equality comparison never does.</para>
///
/// <para>Shared by every expression evaluator (row filtering, HAVING, CHECK constraints) so the
/// three cannot drift apart on the NULL cases, which is exactly where they would drift.</para>
/// </summary>
internal static class BooleanTruthTest
{
    /// <summary>
    /// Applies the truth test <paramref name="nodeType"/> to <paramref name="value"/>.
    /// Throws for a non-boolean, non-NULL operand, matching how <c>NOT</c> rejects one rather than
    /// coercing it — SQL requires the argument of a truth test to be boolean.
    /// </summary>
    public static bool Evaluate(NodeType nodeType, ColumnValue value)
    {
        if (value.Type is not ColumnType.Bool and not ColumnType.Null)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"No matching signature for operator {Describe(nodeType)} for argument type: {value.Type}");
        }

        // NULL is neither true nor false, so both positive tests fail on it and both negated
        // tests succeed on it.
        bool isTrue = value.Type == ColumnType.Bool && value.BoolValue;
        bool isFalse = value.Type == ColumnType.Bool && !value.BoolValue;

        return nodeType switch
        {
            NodeType.ExprIsTrue => isTrue,
            NodeType.ExprIsNotTrue => !isTrue,
            NodeType.ExprIsFalse => isFalse,
            NodeType.ExprIsNotFalse => !isFalse,
            _ => throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Unexpected truth test node: {nodeType}"),
        };
    }

    /// <summary>True when <paramref name="nodeType"/> is one of the four truth tests.</summary>
    public static bool IsTruthTest(NodeType nodeType) =>
        nodeType is NodeType.ExprIsTrue
            or NodeType.ExprIsNotTrue
            or NodeType.ExprIsFalse
            or NodeType.ExprIsNotFalse;

    /// <summary>Renders the operator for error messages and plan/constraint text.</summary>
    public static string Describe(NodeType nodeType) => nodeType switch
    {
        NodeType.ExprIsTrue => "IS TRUE",
        NodeType.ExprIsNotTrue => "IS NOT TRUE",
        NodeType.ExprIsFalse => "IS FALSE",
        NodeType.ExprIsNotFalse => "IS NOT FALSE",
        _ => nodeType.ToString(),
    };
}
