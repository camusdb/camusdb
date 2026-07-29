
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Renders a CHECK-constraint condition AST to SQL text that <em>re-parses to the same predicate</em>.
/// This is deliberately distinct from <c>PlanRenderer.RenderExpr</c>, which targets human-readable
/// EXPLAIN output and is intentionally lossy — it drops parentheses (so precedence drifts on
/// re-parse), collapses IN-lists to <c>(...)</c>, and abbreviates CAST targets. A CHECK is persisted
/// as text and <b>enforced against the AST re-parsed from that text on every node</b>, so a lossy
/// render would silently enforce a different predicate than the user wrote.
/// <para>
/// Fidelity strategy: parenthesize every <em>compound</em> sub-expression when it appears as an
/// operand, so the parser's precedence can never regroup it (e.g. <c>(a OR b) AND c</c> stays
/// grouped, and <c>a OR b AND c</c> renders <c>a OR (b AND c)</c>). Atomic and self-delimiting
/// operands (identifiers, literals, function calls, CAST) are left unwrapped, so a simple
/// <c>price &gt; 0</c> renders unchanged. IN-lists, CAST targets, and function arguments are rendered
/// in full. Any node that cannot be represented faithfully raises <see cref="CamusDBException"/> so
/// the DDL layer rejects the constraint rather than persisting a string that re-parses differently.
/// </para>
/// </summary>
internal static class CheckConditionRenderer
{
    public static string Render(NodeAst expr)
    {
        StringBuilder sb = new();
        RenderNode(sb, expr);
        return sb.ToString();
    }

    /// <summary>
    /// True for nodes that are self-delimiting as an operand and never need wrapping parentheses:
    /// leaves (identifiers, literals) and constructs that carry their own bracketing (function
    /// calls, CAST).
    /// </summary>
    private static bool IsAtomic(NodeAst e) => e.nodeType is
        NodeType.Identifier or NodeType.Integer or NodeType.Float or NodeType.Bool or
        NodeType.Null or NodeType.String or NodeType.ObjectIdLiteral or
        NodeType.ExprFuncCall or NodeType.ExprCast or NodeType.ExprCase;

    /// <summary>Renders <paramref name="e"/> as an operand, wrapping compound expressions in parens.</summary>
    private static void RenderOperand(StringBuilder sb, NodeAst e)
    {
        if (IsAtomic(e))
        {
            RenderNode(sb, e);
            return;
        }

        sb.Append('(');
        RenderNode(sb, e);
        sb.Append(')');
    }

    private static void RenderNode(StringBuilder sb, NodeAst expr)
    {
        switch (expr.nodeType)
        {
            // ── leaves ───────────────────────────────────────────────────────────
            case NodeType.Identifier:
            case NodeType.Integer:
            case NodeType.Float:
            case NodeType.Bool:
            case NodeType.ObjectIdLiteral:
                sb.Append(expr.yytext ?? throw Unsupported(expr));
                return;

            case NodeType.Null:
                sb.Append("NULL");
                return;

            case NodeType.String:
                // yytext carries the source form including its outer quotes; decode it and re-emit in
                // the canonical single-quoted form so it re-lexes to the same string.
                sb.Append(SqlStringLiteral.Quote(SqlStringLiteral.Decode(expr.yytext ?? "")));
                return;

            // ── comparisons ─────────────────────────────────────────────────────
            case NodeType.ExprEquals:            RenderBinary(sb, expr, "="); return;
            case NodeType.ExprNotEquals:         RenderBinary(sb, expr, "<>"); return;
            case NodeType.ExprLessThan:          RenderBinary(sb, expr, "<"); return;
            case NodeType.ExprGreaterThan:       RenderBinary(sb, expr, ">"); return;
            case NodeType.ExprLessEqualsThan:    RenderBinary(sb, expr, "<="); return;
            case NodeType.ExprGreaterEqualsThan: RenderBinary(sb, expr, ">="); return;

            // ── boolean combinators ─────────────────────────────────────────────
            case NodeType.ExprAnd: RenderBinary(sb, expr, "AND"); return;
            case NodeType.ExprOr:  RenderBinary(sb, expr, "OR"); return;

            case NodeType.ExprNot:
                sb.Append("NOT ");
                RenderOperand(sb, expr.leftAst!);
                return;

            // ── arithmetic ──────────────────────────────────────────────────────
            case NodeType.ExprAdd:  RenderBinary(sb, expr, "+"); return;
            case NodeType.ExprSub:  RenderBinary(sb, expr, "-"); return;
            case NodeType.ExprMult: RenderBinary(sb, expr, "*"); return;
            case NodeType.ExprDiv:  RenderBinary(sb, expr, "/"); return;

            // ── string matching ─────────────────────────────────────────────────
            case NodeType.ExprLike:  RenderBinary(sb, expr, "LIKE"); return;
            case NodeType.ExprILike: RenderBinary(sb, expr, "ILIKE"); return;

            // ── regex matching ──────────────────────────────────────────────────
            case NodeType.ExprRegexMatch:      RenderBinary(sb, expr, "~");   return;
            case NodeType.ExprRegexMatchCi:    RenderBinary(sb, expr, "~*");  return;
            case NodeType.ExprRegexNotMatch:   RenderBinary(sb, expr, "!~");  return;
            case NodeType.ExprRegexNotMatchCi: RenderBinary(sb, expr, "!~*"); return;

            // ── null tests ──────────────────────────────────────────────────────
            case NodeType.ExprIsNull:
                RenderOperand(sb, expr.leftAst!);
                sb.Append(" IS NULL");
                return;

            case NodeType.ExprIsNotNull:
                RenderOperand(sb, expr.leftAst!);
                sb.Append(" IS NOT NULL");
                return;

            // ── BETWEEN ─────────────────────────────────────────────────────────
            case NodeType.ExprBetween:
                RenderOperand(sb, expr.leftAst!);
                sb.Append(" BETWEEN ");
                RenderOperand(sb, expr.extendedOne!);
                sb.Append(" AND ");
                RenderOperand(sb, expr.extendedTwo!);
                return;

            // ── IN / NOT IN membership (literal value lists) ────────────────────
            case NodeType.ExprInMembership:
                RenderInList(sb, expr, negated: false);
                return;

            case NodeType.ExprNotInMembership:
                RenderInList(sb, expr, negated: true);
                return;

            // ── function call ───────────────────────────────────────────────────
            case NodeType.ExprFuncCall:
                sb.Append(expr.leftAst?.yytext ?? throw Unsupported(expr)).Append('(');
                if (expr.rightAst is not null && expr.rightAst.nodeType != NodeType.ExprAllFields)
                    RenderArgList(sb, expr.rightAst);
                sb.Append(')');
                return;

            // ── CAST ────────────────────────────────────────────────────────────
            case NodeType.ExprCast:
                sb.Append("CAST(");
                RenderNode(sb, expr.leftAst!);
                sb.Append(" AS ").Append(RenderCastType(expr.rightAst!)).Append(')');
                return;

            // ── CASE … WHEN … THEN … [ELSE …] END ───────────────────────────────
            // Self-delimiting: the WHEN/THEN/ELSE/END keywords bound each sub-expression, so no
            // operand needs wrapping parens for a faithful re-parse.
            case NodeType.ExprCase:
                sb.Append("CASE");
                if (expr.leftAst is not null) // simple-CASE operand
                {
                    sb.Append(' ');
                    RenderNode(sb, expr.leftAst);
                }

                foreach (NodeAst clause in CamusDB.Core.CommandsExecutor.Controllers.DML.SQLExecutorBaseCreator
                             .EnumerateWhenClauses(expr.rightAst!))
                {
                    sb.Append(" WHEN ");
                    RenderNode(sb, clause.leftAst!);
                    sb.Append(" THEN ");
                    RenderNode(sb, clause.rightAst!);
                }

                if (expr.extendedOne is not null) // ELSE result
                {
                    sb.Append(" ELSE ");
                    RenderNode(sb, expr.extendedOne);
                }

                sb.Append(" END");
                return;

            default:
                throw Unsupported(expr);
        }
    }

    private static void RenderBinary(StringBuilder sb, NodeAst expr, string op)
    {
        RenderOperand(sb, expr.leftAst!);
        sb.Append(' ').Append(op).Append(' ');
        RenderOperand(sb, expr.rightAst!);
    }

    private static void RenderInList(StringBuilder sb, NodeAst expr, bool negated)
    {
        RenderOperand(sb, expr.leftAst!);
        sb.Append(negated ? " NOT IN (" : " IN (");
        RenderArgList(sb, expr.rightAst ?? throw Unsupported(expr));
        sb.Append(')');
    }

    /// <summary>Renders a comma-separated argument/value list from an <c>ExprList</c> tree.</summary>
    private static void RenderArgList(StringBuilder sb, NodeAst list)
    {
        bool first = true;
        RenderArgListInner(sb, list, ref first);
    }

    private static void RenderArgListInner(StringBuilder sb, NodeAst node, ref bool first)
    {
        if (node.nodeType == NodeType.ExprList)
        {
            if (node.leftAst is not null) RenderArgListInner(sb, node.leftAst, ref first);
            if (node.rightAst is not null) RenderArgListInner(sb, node.rightAst, ref first);
            return;
        }

        if (!first) sb.Append(", ");
        first = false;
        RenderNode(sb, node);
    }

    /// <summary>
    /// Renders a CAST target type node to a keyword that re-parses to the same
    /// <c>ColumnType</c>. Identifier targets are emitted verbatim; sentinel type nodes use their
    /// canonical spelling. An unmappable target raises so the DDL layer rejects the CHECK.
    /// </summary>
    private static string RenderCastType(NodeAst typeNode) => typeNode.nodeType switch
    {
        NodeType.Identifier    => typeNode.yytext ?? throw Unsupported(typeNode),
        NodeType.TypeString or NodeType.TypeStringSized => "string",
        NodeType.TypeInteger64 => "int64",
        NodeType.TypeFloat64   => "float64",
        NodeType.TypeFloat32   => "float32",
        NodeType.TypeBool      => "bool",
        NodeType.TypeObjectId  => "object_id",
        NodeType.TypeDate      => "date",
        NodeType.TypeDateTime  => "datetime",
        NodeType.TypeBytes     => "bytes",
        NodeType.TypeUuid      => "uuid",
        _ => throw Unsupported(typeNode),
    };

    private static CamusDBException Unsupported(NodeAst expr) => new(
        CamusDBErrorCodes.InvalidInput,
        $"CHECK constraint contains an expression that cannot be represented ({expr.nodeType})");
}
