
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.SQLParser;

/// <summary>
/// Builds and reads the AST of <c>UNION</c>, <c>INTERSECT</c> and <c>EXCEPT</c>.
///
/// <para><b>Shape.</b> <c>q1 UNION q2 ORDER BY 1 LIMIT 10</c> parses to
/// <c>Select(*, from = SetOperationReference(q1, q2), order, limit, offset)</c>. The set operation is
/// always in the FROM slot of a plain <see cref="NodeType.Select"/>, never the root, because many
/// consumers check for a Select or read its fixed slots: the subquery executors, INSERT … SELECT,
/// CTAS, the view builder and the subquery boundary walkers. To each of them a set operation is one
/// more relation in FROM, and the ORDER BY, LIMIT and OFFSET of the combined result reuse the code
/// that a plain SELECT uses.</para>
///
/// <para><b>Operands are always Selects.</b> An operand that is itself a set operation, as the left
/// operand of <c>a UNION b UNION c</c> is, gets the same wrapper, so a consumer reads every operand
/// the same way. A long chain of one associative operator is rebalanced after the parse
/// (<see cref="Normalize"/>); any other chain nests one level per operator, and
/// <see cref="StatementDepthGuard"/> bounds that.</para>
/// </summary>
internal static class SetOperationAst
{
    public const string Union = "UNION";

    public const string Intersect = "INTERSECT";

    public const string Except = "EXCEPT";

    private const string AllSuffix = " ALL";

    /// <summary>
    /// Builds <c>left kind [ALL] right</c>. <paramref name="quantifier"/> is <c>"ALL"</c> for the ALL
    /// form and null for the default, which removes duplicate rows.
    /// </summary>
    public static NodeAst Combine(string kind, string? quantifier, NodeAst left, NodeAst right) =>
        new(
            NodeType.SetOperationReference,
            AsOperand(left),
            AsOperand(right),
            null, null, null, null, null,
            quantifier is null ? kind : string.Concat(kind, AllSuffix));

    /// <summary>
    /// Attaches the ORDER BY, LIMIT and OFFSET that follow a query. A set operation gets its Select
    /// wrapper here. A plain Select gets the clauses in its own slots, unless it is a parenthesized
    /// query that already has one of them: <c>(SELECT … LIMIT 5) ORDER BY a</c> would otherwise
    /// silently sort before the limit instead of after it.
    /// </summary>
    public static NodeAst Finish(NodeAst query, NodeAst? orderBy, NodeAst? limit, NodeAst? offset)
    {
        if (query.nodeType == NodeType.SetOperationReference)
            return Wrap(query, orderBy, limit, offset);

        if (orderBy is null && limit is null && offset is null)
            return query;

        // A FROM-less SELECT returns one row, so the grammar never gave it an ORDER BY; it keeps
        // LIMIT and OFFSET, which can drop that row.
        if (orderBy is not null && query.rightAst is null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "ORDER BY requires a FROM clause");

        if (query.extendedTwo is not null || query.extendedThree is not null || query.extendedFour is not null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "A parenthesized query that has its own ORDER BY, LIMIT or OFFSET cannot take another one; " +
                "move all of them inside the parentheses");

        return new NodeAst(
            NodeType.Select,
            query.leftAst,
            query.rightAst,
            query.extendedOne,
            orderBy,
            limit,
            offset,
            query.extendedFive,
            query.yytext,
            query.extendedSix,
            query.extendedSeven);
    }

    /// <summary>True when <paramref name="select"/> is the Select wrapper of a set operation.</summary>
    public static bool IsSetOperationSelect(NodeAst select) =>
        select.nodeType == NodeType.Select && select.rightAst?.nodeType == NodeType.SetOperationReference;

    /// <summary>The operator of a <see cref="NodeType.SetOperationReference"/>: UNION, INTERSECT or EXCEPT.</summary>
    public static string KindOf(NodeAst setOperation)
    {
        string text = setOperation.yytext ?? "";
        int space = text.IndexOf(' ');
        return space < 0 ? text : text[..space];
    }

    /// <summary>True for the ALL form, which keeps duplicate rows.</summary>
    public static bool IsAll(NodeAst setOperation) =>
        setOperation.yytext?.EndsWith(AllSuffix, StringComparison.Ordinal) ?? false;

    /// <summary>
    /// A chain with at most this many operands keeps the shape the parser gave it. A set operation
    /// costs much more stack per level than an expression (see
    /// <see cref="StatementDepthGuard.MaxSetOperationNesting"/>), so the threshold is lower than the
    /// one for AND and OR.
    /// </summary>
    internal const int BalanceThreshold = 8;

    /// <summary>
    /// Rebalances the chain of one set operator rooted at <paramref name="root"/> when it is long and
    /// the operator is associative, and appends the chain's operands, in source order, to
    /// <paramref name="operands"/> so the caller's walk continues into them. Runs once per parse, in
    /// <see cref="ExpressionChains.Normalize"/>, before the tree is shared.
    ///
    /// <para><b>Why.</b> The grammar builds <c>q1 UNION ALL q2 UNION ALL … qn</c> left-deep, one level
    /// per operator, and generated SQL writes hundreds of operands that way. Every level keeps a
    /// cursor open on the stack while the first row is read, so such a chain overflowed the stack, and
    /// a stack overflow ends the process. A balanced tree has <c>log2 n</c> levels.</para>
    ///
    /// <para><b>Which chains.</b> UNION, UNION ALL, INTERSECT and INTERSECT ALL are associative: the
    /// rows, and the count of each row, do not depend on the grouping. EXCEPT is not, and a mix of
    /// operators is not one chain, so those keep their shape and are bounded by
    /// <see cref="StatementDepthGuard.MaxSetOperationNesting"/>. A link of the chain must be a plain
    /// wrapper (<c>*</c> with no clause of its own), so a parenthesized operand with its own ORDER BY,
    /// LIMIT or OFFSET stays one operand.</para>
    ///
    /// <para><b>What does not change.</b> The operands keep their order, so the leftmost operand still
    /// names the output columns and <c>UNION ALL</c> still returns the rows in source order. The
    /// result types do not change either: the type rules are associative, and a column that is
    /// untyped on both sides stays untyped (<c>BoundSetOperation.Untyped</c>).</para>
    /// </summary>
    internal static void Normalize(NodeAst root, List<NodeAst> operands)
    {
        string? text = root.yytext;
        bool associative = KindOf(root) is Union or Intersect;

        if (!associative)
        {
            operands.Add(root.leftAst!);
            operands.Add(root.rightAst!);
            return;
        }

        // Flatten iteratively: right first, so the left side pops first and source order holds.
        Stack<NodeAst> pending = new();
        pending.Push(root.rightAst!);
        pending.Push(root.leftAst!);

        while (pending.Count > 0)
        {
            NodeAst operand = pending.Pop();

            if (IsPlainWrapper(operand) && string.Equals(operand.rightAst!.yytext, text, StringComparison.Ordinal))
            {
                pending.Push(operand.rightAst.rightAst!);
                pending.Push(operand.rightAst.leftAst!);
                continue;
            }

            operands.Add(operand);
        }

        if (operands.Count <= BalanceThreshold)
            return;

        // Pair neighbours level by level, as ExpressionChains.Balance does, with each pair wrapped so
        // every operand is a Select.
        NodeAst[] level = operands.ToArray();
        int count = level.Length;

        while (count > 2)
        {
            int write = 0;
            int read = 0;

            for (; read + 1 < count; read += 2)
                level[write++] = Wrap(new NodeAst(NodeType.SetOperationReference, level[read], level[read + 1], null, null, null, null, null, text), null, null, null);

            if (read < count)
                level[write++] = level[read];

            count = write;
        }

        root.leftAst = level[0];
        root.rightAst = level[1];
    }

    /// <summary>The Select wrapper of a set operation with no clause of its own.</summary>
    private static bool IsPlainWrapper(NodeAst node) =>
        node.nodeType == NodeType.Select
        && node.rightAst?.nodeType == NodeType.SetOperationReference
        && node.leftAst?.nodeType == NodeType.ExprAllFields
        && node.extendedOne is null && node.extendedTwo is null && node.extendedThree is null
        && node.extendedFour is null && node.extendedFive is null && node.extendedSix is null
        && node.extendedSeven is null && node.yytext is null;

    private static NodeAst AsOperand(NodeAst node) =>
        node.nodeType == NodeType.SetOperationReference ? Wrap(node, null, null, null) : node;

    private static NodeAst Wrap(NodeAst setOperation, NodeAst? orderBy, NodeAst? limit, NodeAst? offset) =>
        new(NodeType.Select, NodeAst.ExprAllFields, setOperation, null, orderBy, limit, offset, null, null);
}
