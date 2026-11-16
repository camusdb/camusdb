
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

    /// <summary>
    /// The name that each work-table reference renders with, by its CTE's name, while that recursive CTE
    /// renders under another name (see <see cref="RenderRecursiveDerivedTable"/>); null when no CTE is
    /// renamed. State of one rendering, so <see cref="RenderSelect"/> renders with a renderer of its own.
    /// </summary>
    private Dictionary<string, string>? workTableNames;

    /// <summary>Renders a whole <c>SELECT</c> statement to canonical SQL.</summary>
    public static string RenderSelect(NodeAst select)
    {
        StringBuilder sb = new();
        new ViewBodyRenderer().RenderSelectInto(sb, select);
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

        if (SetOperationAst.IsSetOperationSelect(select))
        {
            RenderSetOperationInto(sb, select);
            return;
        }

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
    /// Renders the Select wrapper of a set operation as <c>q1 OP q2</c> followed by the wrapper's own
    /// ORDER BY, LIMIT and OFFSET.
    ///
    /// <para><b>Parentheses decide the grouping, not precedence.</b> An operand that is itself a set
    /// operation is always parenthesized, so the stored text groups exactly as the tree does, even
    /// where the operators' precedence would group it the same way without them. A plain operand is
    /// parenthesized only when it has its own ORDER BY, LIMIT or OFFSET, which the grammar admits only
    /// in parentheses. Every form here re-parses: an operand in parentheses is one level deep, never
    /// <c>((SELECT …))</c>.</para>
    /// </summary>
    private void RenderSetOperationInto(StringBuilder sb, NodeAst wrapper)
    {
        NodeAst setOperation = wrapper.rightAst!;

        RenderSetOperationOperand(sb, setOperation.leftAst!);
        sb.Append(' ').Append(setOperation.yytext).Append(' ');
        RenderSetOperationOperand(sb, setOperation.rightAst!);

        if (wrapper.extendedTwo is not null)
        {
            sb.Append(" ORDER BY ");
            RenderOrderList(sb, wrapper.extendedTwo);
        }

        if (wrapper.extendedThree is not null)
        {
            sb.Append(" LIMIT ");
            RenderNode(sb, wrapper.extendedThree);
        }

        if (wrapper.extendedFour is not null)
        {
            sb.Append(" OFFSET ");
            RenderNode(sb, wrapper.extendedFour);
        }
    }

    /// <summary>
    /// The recursive UNION that the derived table <paramref name="derived"/> reads, or null. The parser
    /// gives a reader of a recursive CTE the form <c>(non-recursive UNION recursive) AS alias</c>. When the
    /// stored text of that reader is parsed again, the parser gives it one more level,
    /// <c>(SELECT * FROM (…) AS name) AS alias</c>. Both forms are read here, so both render to the same
    /// text, and a view's text does not change when it is rendered again.
    /// </summary>
    private static NodeAst? RecursiveQueryOf(NodeAst derived)
    {
        NodeAst query = derived.leftAst!;

        if (IsRecursiveUnion(query))
            return query;

        bool passesThrough = query.nodeType == NodeType.Select
            && query.leftAst?.nodeType == NodeType.ExprAllFields
            && query.rightAst is { nodeType: NodeType.DerivedTableReference } inner
            && IsRecursiveUnion(inner.leftAst!)
            && query.extendedOne is null && query.extendedTwo is null && query.extendedThree is null
            && query.extendedFour is null && query.extendedFive is null && query.extendedSix is null
            && query.extendedSeven is null && query.yytext is null;

        return passesThrough ? query.rightAst!.leftAst : null;

        static bool IsRecursiveUnion(NodeAst node) =>
            SetOperationAst.IsSetOperationSelect(node) && node.rightAst!.extendedOne is { nodeType: NodeType.RecursiveCte };
    }

    /// <summary>
    /// Renders a reader of a recursive CTE as
    /// <c>(WITH RECURSIVE name [(columns)] AS (query) SELECT * FROM name) AS alias [(columns)]</c>. The text
    /// parses again to the same recursion.
    ///
    /// <para><b>The name.</b> It is the CTE's own name, unless the recursive query also reads a table of
    /// that name. That happens when the query reads the table through a CTE of an outer WITH, or when a
    /// table it reads took that name in a rename. In the text the WITH would hide that table, so the CTE
    /// renders as <c>name_1</c> (or the first free number), and its work-table reference as
    /// <c>name_1 AS name</c>, so the recursive term still reads its columns through the old name.</para>
    /// </summary>
    private void RenderRecursiveDerivedTable(StringBuilder sb, NodeAst derived, NodeAst recursive)
    {
        NodeAst marker = recursive.rightAst!.extendedOne!;
        string name = marker.yytext!;
        string rendered = name;

        for (int suffix = 1; CommonTableExpressions.ReadsRelationNamed(recursive, rendered); suffix++)
            rendered = $"{name}_{suffix}";

        // A recursive CTE inside this one with the same name hides it, so the mapping of an outer CTE of
        // that name does not apply in here.
        Dictionary<string, string>? outer = workTableNames;

        if (!string.Equals(rendered, name, StringComparison.Ordinal))
            workTableNames = new(outer ?? [], StringComparer.OrdinalIgnoreCase) { [name] = rendered };
        else if (outer is not null && outer.ContainsKey(name))
        {
            workTableNames = new(outer, StringComparer.OrdinalIgnoreCase);
            workTableNames.Remove(name);
        }

        try
        {
            sb.Append("(WITH RECURSIVE ").Append(rendered);

            if (marker.leftAst is not null)
            {
                sb.Append(" (");
                RenderFunctionArguments(sb, marker.leftAst, NodeType.IdentifierList);
                sb.Append(')');
            }

            sb.Append(" AS (");
            RenderSetOperationInto(sb, recursive);
            sb.Append(") SELECT * FROM ").Append(rendered);
        }
        finally
        {
            workTableNames = outer;
        }

        sb.Append(") AS ").Append(derived.rightAst!.yytext);

        if (derived.extendedThree is not null)
        {
            sb.Append('(');
            RenderFunctionArguments(sb, derived.extendedThree, NodeType.IdentifierList);
            sb.Append(')');
        }
    }

    private void RenderSetOperationOperand(StringBuilder sb, NodeAst operand)
    {
        bool parenthesize = SetOperationAst.IsSetOperationSelect(operand)
                            || operand.extendedTwo is not null
                            || operand.extendedThree is not null
                            || operand.extendedFour is not null;

        if (parenthesize)
            sb.Append('(');

        RenderSelectInto(sb, operand);

        if (parenthesize)
            sb.Append(')');
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
    /// <summary>
    /// Renders a left-recursive list (<paramref name="listType"/> nodes whose left child holds the
    /// earlier elements) in call order, comma-separated. Iterative: the spine is one node per element.
    /// </summary>
    private void RenderFunctionArguments(StringBuilder sb, NodeAst? list, NodeType listType = NodeType.ExprArgumentList)
    {
        if (list is null)
            return;

        List<NodeAst> items = new();
        NodeAst node = list;

        while (node.nodeType == listType)
        {
            items.Add(node.rightAst!);
            node = node.leftAst!;
        }

        items.Add(node);

        for (int i = items.Count - 1; i >= 0; i--)
        {
            RenderNode(sb, items[i]);
            if (i > 0)
                sb.Append(", ");
        }
    }

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

            case NodeType.TableFunctionReference:
                // fn(args) [AS alias[(column)]]: re-parses to the same relation and column names.
                sb.Append(from.leftAst!.yytext).Append('(');
                RenderFunctionArguments(sb, from.rightAst);
                sb.Append(')');
                if (from.extendedOne?.yytext is not null)
                    sb.Append(" AS ").Append(from.extendedOne.yytext);
                if (from.extendedTwo is not null)
                {
                    sb.Append('(');
                    RenderFunctionArguments(sb, from.extendedTwo, NodeType.IdentifierList);
                    sb.Append(')');
                }
                return;

            case NodeType.WorkTableReference:
            {
                // The reference of a recursive CTE to itself, inside the WITH RECURSIVE that the
                // derived-table case below writes for the CTE. Under another name it keeps its old
                // name as its alias, which is what the recursive term reads its columns through.
                string name = from.leftAst!.yytext!;
                string? alias = from.rightAst?.yytext;

                if (workTableNames is not null && workTableNames.TryGetValue(name, out string? renamed))
                {
                    sb.Append(renamed).Append(" AS ").Append(alias ?? name);
                    return;
                }

                sb.Append(name);
                if (alias is not null)
                    sb.Append(" AS ").Append(alias);
                return;
            }

            case NodeType.DerivedTableReference when RecursiveQueryOf(from) is { } recursive:
                if (LateralAst.IsLateral(from))
                    sb.Append("LATERAL ");

                RenderRecursiveDerivedTable(sb, from, recursive);
                return;

            // A function in FROM that reads the items to its left: the parser wrapped the call, so the call
            // renders and parses again to the same wrap.
            case NodeType.DerivedTableReference when LateralAst.WrappedFunction(from) is { } wrapped:
                RenderFrom(sb, wrapped);
                return;

            case NodeType.DerivedTableReference:
                // A CTE of the body was written out as a derived table when the body was parsed, so the
                // stored body has no WITH. Its column list renders as the derived table's.
                if (LateralAst.IsLateral(from))
                    sb.Append("LATERAL ");

                sb.Append('(');
                RenderSelectInto(sb, from.leftAst!);
                sb.Append(") AS ").Append(from.rightAst!.yytext);
                if (from.extendedThree is not null)
                {
                    sb.Append('(');
                    RenderFunctionArguments(sb, from.extendedThree, NodeType.IdentifierList);
                    sb.Append(')');
                }
                return;

            case NodeType.Join:
                // The kind is rendered in its canonical spelling so the stored body re-parses to the
                // same join form; a cross join has no ON clause to render.
                RenderFrom(sb, from.leftAst!);
                sb.Append(JoinAstKind.Read(from) switch
                {
                    JoinAstKind.Kind.Left => " LEFT OUTER JOIN ",
                    JoinAstKind.Kind.Right => " RIGHT OUTER JOIN ",
                    JoinAstKind.Kind.Cross => " CROSS JOIN ",
                    _ => " INNER JOIN ",
                });
                RenderFrom(sb, from.rightAst!);
                if (from.extendedOne is not null)
                {
                    sb.Append(" ON ");
                    RenderNode(sb, from.extendedOne);
                }
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
