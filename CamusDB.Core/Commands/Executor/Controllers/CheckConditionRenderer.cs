
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Renders a CHECK-constraint condition AST to SQL text that <em>re-parses to the same predicate</em>.
/// A CHECK is persisted as text and <b>enforced against the AST re-parsed from that text on every
/// node</b>, so a lossy render would silently enforce a different predicate than the user wrote —
/// see <see cref="SqlAstRenderer"/> for the fidelity strategy this relies on.
///
/// <para>Deliberately narrow: it adds nothing to <see cref="SqlAstRenderer"/> and therefore cannot
/// render a subquery, a SELECT, or any other construct a CHECK may not legally contain. That is the
/// point — a node reaching the base class's <c>default</c> arm here means the DDL layer should
/// reject the constraint rather than persist a string that re-parses differently.</para>
/// </summary>
internal static class CheckConditionRenderer
{
    private sealed class Renderer : SqlAstRenderer
    {
        protected override CamusDBException Unsupported(NodeAst expr) => new(
            CamusDBErrorCodes.InvalidInput,
            $"CHECK constraint contains an expression that cannot be represented ({expr.nodeType})");
    }

    private static readonly Renderer renderer = new();

    public static string Render(NodeAst expr) => renderer.Render(expr);
}
