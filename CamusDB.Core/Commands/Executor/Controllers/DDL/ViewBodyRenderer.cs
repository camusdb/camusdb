
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// Renders a <c>SELECT</c> AST back to SQL text that re-parses to the same query. This is what a
/// view's body is stored as.
///
/// <para><b>Why a view is stored as re-rendered text rather than as the user typed it.</b> Three
/// things depend on it. A base-table or base-column rename must rewrite every dependent view's body,
/// and that rewrite is only safe as a targeted AST edit followed by a re-render — a string
/// substitution over user text would happily rewrite a matching word inside a string literal or an
/// unrelated table's column list. <c>CREATE OR REPLACE VIEW</c> needs a canonical form to compare
/// against. And <c>SHOW CREATE VIEW</c> needs something it can print that is guaranteed to parse.
/// PostgreSQL reaches the same place from the other direction: it stores a parse tree and
/// <c>pg_get_viewdef</c> never returns the original text either, so normalized output is the
/// expected behavior rather than a wart.</para>
///
/// <para>Extends <see cref="SqlAstRenderer"/> with the constructs a query body may contain that a
/// CHECK constraint may not: subqueries in every position, derived tables, and the SELECT statement
/// itself. Anything still unrepresentable raises, so <c>CREATE VIEW</c> rejects the statement rather
/// than storing text that would mean something else when re-parsed.</para>
/// </summary>
internal sealed class ViewBodyRenderer : SqlAstRenderer
{
    private static readonly ViewBodyRenderer Instance = new();

    /// <summary>Renders a whole <c>SELECT</c> statement to canonical SQL.</summary>
    public static string RenderSelect(NodeAst select)
    {
        StringBuilder sb = new();
        Instance.RenderSelectInto(sb, select);
        return sb.ToString();
    }

    /// <summary>Renders a single scalar expression (a projection item, a predicate) to canonical SQL.</summary>
    public static string RenderExpression(NodeAst expr) => Instance.Render(expr);

    /// <summary>
    /// Whether the text appended to <paramref name="sb"/> from <paramref name="start"/> onwards is
    /// exactly <paramref name="text"/>. Ordinal, so an alias that differs only in case is kept — the
    /// case a view publishes its columns in is part of what it publishes.
    /// </summary>
    private static bool Spans(StringBuilder sb, int start, string text)
    {
        if (sb.Length - start != text.Length)
            return false;

        for (int i = 0; i < text.Length; i++)
        {
            if (sb[start + i] != text[i])
                return false;
        }

        return true;
    }

    protected override CamusDBException Unsupported(NodeAst expr) => new(
        CamusDBErrorCodes.InvalidInput,
        $"View body contains an expression that cannot be represented ({expr.nodeType})");

    // A parenthesized subquery already carries its own brackets, so wrapping it again as an operand
    // would emit doubled parens that, while harmless to re-parse, make stored definitions noisy.
    protected override bool IsAtomic(NodeAst e) =>
        base.IsAtomic(e) ||
        e.nodeType is NodeType.ExprScalarSubquery or NodeType.ExprExistsSubquery or NodeType.Select;

    protected override bool TryRenderExtension(StringBuilder sb, NodeAst expr)
    {
        switch (expr.nodeType)
        {
            case NodeType.Select:
                sb.Append('(');
                RenderSelectInto(sb, expr);
                sb.Append(')');
                return true;

            case NodeType.ExprAllFields:
                sb.Append('*');
                return true;

            case NodeType.ExprAlias:
                {
                    int start = sb.Length;
                    RenderNode(sb, expr.leftAst!);

                    string? alias = expr.rightAst!.yytext;

                    // An alias that repeats what it aliases carries nothing, so it is not printed.
                    // Stored bodies alias every projection — that is what pins a view's published
                    // column names against a base-column rename — and echoing all of them back would
                    // turn every ordinary view into "SELECT id AS id, total AS total".
                    if (alias is not null && Spans(sb, start, alias))
                        return true;

                    sb.Append(" AS ").Append(alias);
                    return true;
                }

            case NodeType.ExprScalarSubquery:
                sb.Append('(');
                RenderSelectInto(sb, expr.leftAst!);
                sb.Append(')');
                return true;

            case NodeType.ExprExistsSubquery:
                sb.Append("EXISTS (");
                RenderSelectInto(sb, expr.leftAst!);
                sb.Append(')');
                return true;

            case NodeType.ExprInSubquery:
            case NodeType.ExprNotInSubquery:
                RenderOperand(sb, expr.leftAst!);
                sb.Append(expr.nodeType == NodeType.ExprInSubquery ? " IN (" : " NOT IN (");
                RenderSelectInto(sb, expr.rightAst!);
                sb.Append(')');
                return true;

            default:
                return false;
        }
    }

    /// <summary>
    /// Renders the SELECT clauses in the order the grammar accepts them. The clause-to-slot mapping
    /// is not guessable from the node — it is fixed by the <c>select_stmt</c> production — so it is
    /// spelled out here: leftAst = projections, rightAst = FROM, extendedOne = WHERE,
    /// extendedTwo = ORDER BY, extendedThree = LIMIT, extendedFour = OFFSET, extendedFive = GROUP BY,
    /// extendedSix = HAVING, extendedSeven = AS OF SYSTEM TIME, yytext = DISTINCT flag.
    /// </summary>
    private void RenderSelectInto(StringBuilder sb, NodeAst select)
    {
        if (select.nodeType != NodeType.Select)
            throw Unsupported(select);

        sb.Append("SELECT ");

        if (select.yytext is not null)
            sb.Append("DISTINCT ");

        RenderProjectionList(sb, select.leftAst ?? throw Unsupported(select));

        // A FROM-less SELECT is legal (SELECT 1, SELECT (subquery)), and has no source to render.
        if (select.rightAst is not null)
        {
            sb.Append(" FROM ");
            RenderFrom(sb, select.rightAst);
        }

        // AS OF SYSTEM TIME sits between FROM and WHERE, matching the grammar. A view body carrying
        // one is rejected before it ever reaches here (see ViewBodyValidator) — a stored absolute
        // timestamp is frozen forever and a stored relative one silently means something different
        // at every reference — but rendering it keeps this class total for other callers.
        if (select.extendedSeven is not null)
        {
            sb.Append(" AS OF SYSTEM TIME ");
            RenderNode(sb, select.extendedSeven);
        }

        if (select.extendedOne is not null)
        {
            sb.Append(" WHERE ");
            RenderNode(sb, select.extendedOne);
        }

        if (select.extendedFive is not null)
        {
            sb.Append(" GROUP BY ");
            RenderCommaList(sb, select.extendedFive.leftAst ?? throw Unsupported(select.extendedFive));
        }

        if (select.extendedSix is not null)
        {
            sb.Append(" HAVING ");
            RenderNode(sb, select.extendedSix.leftAst ?? throw Unsupported(select.extendedSix));
        }

        if (select.extendedTwo is not null)
        {
            sb.Append(" ORDER BY ");
            RenderOrderList(sb, select.extendedTwo);
        }

        if (select.extendedThree is not null)
        {
            sb.Append(" LIMIT ");
            RenderNode(sb, select.extendedThree);
        }

        if (select.extendedFour is not null)
        {
            sb.Append(" OFFSET ");
            RenderNode(sb, select.extendedFour);
        }
    }

    /// <summary>
    /// Renders the projection list. Aliases are emitted with an explicit <c>AS</c> even where the
    /// user omitted it, so re-parsing yields the same output names.
    /// </summary>
    private void RenderProjectionList(StringBuilder sb, NodeAst list)
    {
        bool first = true;
        RenderIdentifierListInner(sb, list, ref first);
    }

    private void RenderCommaList(StringBuilder sb, NodeAst list)
    {
        bool first = true;
        RenderIdentifierListInner(sb, list, ref first);
    }

    // Both projection lists and GROUP BY lists are left-nested trees; they differ only in the node
    // type the grammar uses to nest them (IdentifierList vs ExprList), so one walker handles both.
    private void RenderIdentifierListInner(StringBuilder sb, NodeAst node, ref bool first)
    {
        if (node.nodeType is NodeType.IdentifierList or NodeType.ExprList)
        {
            if (node.leftAst is not null) RenderIdentifierListInner(sb, node.leftAst, ref first);
            if (node.rightAst is not null) RenderIdentifierListInner(sb, node.rightAst, ref first);
            return;
        }

        if (!first) sb.Append(", ");
        first = false;
        RenderNode(sb, node);
    }

    private void RenderOrderList(StringBuilder sb, NodeAst node, bool first = true)
    {
        if (node.nodeType == NodeType.IdentifierList)
        {
            RenderOrderList(sb, node.leftAst!, first);
            RenderOrderList(sb, node.rightAst!, first: false);
            return;
        }

        if (!first) sb.Append(", ");

        switch (node.nodeType)
        {
            case NodeType.SortAsc:
                RenderNode(sb, node.leftAst!);
                sb.Append(" ASC");
                return;

            case NodeType.SortDesc:
                RenderNode(sb, node.leftAst!);
                sb.Append(" DESC");
                return;

            default:
                RenderNode(sb, node);
                return;
        }
    }

    /// <summary>
    /// Renders the FROM clause: a plain table reference with its optional alias, a derived table, an
    /// explicit JOIN chain, or a comma join.
    /// </summary>
    /// <remarks>
    /// Index and cache hints (<c>@{force_index=…}</c>) are deliberately <b>not</b> rendered. A hint
    /// is a property of the statement a user ran, not of the relation a view defines: baking one into
    /// a stored definition would silently pin every future query through the view to a plan choice
    /// made once, against statistics that have since moved. A view body carrying a hint is rejected
    /// at CREATE time rather than silently stripped here.
    /// </remarks>
    private void RenderFrom(StringBuilder sb, NodeAst from)
    {
        switch (from.nodeType)
        {
            case NodeType.Identifier:
                sb.Append(from.yytext);
                return;

            case NodeType.TableReference:
                sb.Append(from.leftAst!.yytext);
                if (from.rightAst?.yytext is not null)
                    sb.Append(" AS ").Append(from.rightAst.yytext);
                if (from.extendedOne is not null)
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInput,
                        "A view body may not carry an index or cache hint: a hint pins a plan choice " +
                        "into the stored definition, where it would outlive the statistics it was made against");
                return;

            case NodeType.DerivedTableReference:
                sb.Append('(');
                RenderSelectInto(sb, from.leftAst!);
                sb.Append(") AS ").Append(from.rightAst!.yytext);
                return;

            case NodeType.Join:
                RenderFrom(sb, from.leftAst!);
                sb.Append(" INNER JOIN ");
                RenderFrom(sb, from.rightAst!);
                sb.Append(" ON ");
                RenderNode(sb, from.extendedOne!);
                return;

            case NodeType.CommaJoin:
            case NodeType.CommaJoinTableList:
                RenderFrom(sb, from.leftAst!);
                sb.Append(", ");
                RenderFrom(sb, from.rightAst!);
                return;

            default:
                throw Unsupported(from);
        }
    }
}
