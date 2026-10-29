
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.SQLParser;

/// <summary>
/// Builds the AST of a row-locking read, <c>SELECT … FOR UPDATE [OF t] [NOWAIT | SKIP LOCKED]</c>.
///
/// <para><b>A wrapper at the statement root, not a slot on the Select.</b> The parser accepts the
/// clause only after a top-level statement (<c>stat : select_stmt locking_clause</c>) and after the
/// query of an EXPLAIN (<c>explain_target</c>). The result is a <see cref="NodeType.SelectForUpdate"/>
/// whose <c>leftAst</c> is the untouched Select and whose <c>rightAst</c> is the
/// <see cref="NodeType.LockingClause"/>. The Select node uses all of its slots already, and
/// <c>select_stmt</c> is reused by subqueries, derived tables, INSERT … SELECT, CTAS and views. A
/// wrapper keeps the clause out of every one of those places: in any of them it is a parse error, and
/// every consumer of a Select keeps working without a change.</para>
///
/// <para><b>The options are words, checked here.</b> <c>OF</c>, <c>NOWAIT</c>, <c>SKIP</c> and
/// <c>LOCKED</c> are not keywords: the grammar collects the identifiers that follow the strength and
/// <see cref="Clause"/> reads them in order, as the SET TRANSACTION productions check their words.
/// Keywords would reserve four more common words in every other statement.</para>
/// </summary>
internal static class RowLockingAst
{
    /// <summary>
    /// The default wait policy: a held row fails the statement as an UPDATE of the same row would,
    /// with <see cref="CamusDBErrorCodes.TransactionConflict"/>.
    /// </summary>
    public const string Wait = "wait";

    /// <summary><c>NOWAIT</c>: a held row fails the statement with <see cref="CamusDBErrorCodes.LockNotAvailable"/>.</summary>
    public const string NoWait = "nowait";

    /// <summary><c>SKIP LOCKED</c>: a held row is left out of the result.</summary>
    public const string SkipLocked = "skip locked";

    /// <summary>
    /// Checks the words between <c>FOR</c> and the options. Only <c>UPDATE</c> and its PostgreSQL
    /// synonym <c>NO KEY UPDATE</c> are supported; the share strengths are refused with
    /// <see cref="CamusDBErrorCodes.FeatureNotSupported"/>, because a shared row lock cannot stop a
    /// concurrent <c>FOR UPDATE</c> of the same row: the exclusive key-lock grant does not consult range
    /// locks.
    /// </summary>
    public static string Strength(string? first, bool key, bool update)
    {
        // FOR UPDATE
        if (first is null && !key && update)
            return "update";

        // FOR NO KEY UPDATE
        if (string.Equals(first, "no", StringComparison.OrdinalIgnoreCase) && key && update)
            return "update";

        // FOR SHARE, FOR KEY SHARE
        if (!update && string.Equals(first, "share", StringComparison.OrdinalIgnoreCase))
            throw new CamusDBException(
                CamusDBErrorCodes.FeatureNotSupported,
                key ? "FOR KEY SHARE is not supported; use FOR UPDATE" : "FOR SHARE is not supported; use FOR UPDATE");

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            "Expected FOR UPDATE, FOR NO KEY UPDATE, FOR SHARE or FOR KEY SHARE");
    }

    /// <summary>
    /// Appends one option word to the left-nested <see cref="NodeType.IdentifierList"/> the grammar
    /// builds while it reads the options.
    /// </summary>
    public static NodeAst AppendWord(NodeAst? words, NodeAst word) =>
        words is null ? word : new(NodeType.IdentifierList, words, word, null, null, null, null, null, null);

    /// <summary>
    /// Reads the option words in order: an optional <c>OF table</c>, then an optional <c>NOWAIT</c> or
    /// <c>SKIP LOCKED</c>. Returns the <see cref="NodeType.LockingClause"/>: <c>yytext</c> is the wait
    /// policy (<see cref="Wait"/>, <see cref="NoWait"/> or <see cref="SkipLocked"/>), and <c>leftAst</c>
    /// is the identifier that <c>OF</c> names, or null.
    /// </summary>
    public static NodeAst Clause(NodeAst? words)
    {
        List<NodeAst> list = [];
        Flatten(words, list);

        NodeAst? of = null;
        string? policy = null;
        int i = 0;

        while (i < list.Count)
        {
            string word = list[i].yytext ?? "";

            if (string.Equals(word, "of", StringComparison.OrdinalIgnoreCase) && of is null && policy is null)
            {
                if (i + 1 >= list.Count)
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "FOR UPDATE OF needs a table name");

                of = list[i + 1];
                i += 2;
                continue;
            }

            if (string.Equals(word, "nowait", StringComparison.OrdinalIgnoreCase) && policy is null)
            {
                policy = NoWait;
                i++;
                continue;
            }

            if (string.Equals(word, "skip", StringComparison.OrdinalIgnoreCase) && policy is null
                && i + 1 < list.Count && string.Equals(list[i + 1].yytext, "locked", StringComparison.OrdinalIgnoreCase))
            {
                policy = SkipLocked;
                i += 2;
                continue;
            }

            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Unexpected '{word}' in the FOR UPDATE clause. Expected: FOR UPDATE [OF table] [NOWAIT | SKIP LOCKED]");
        }

        return new(NodeType.LockingClause, of, null, null, null, null, null, null, policy ?? Wait);
    }

    /// <summary>
    /// Wraps a top-level query with its locking clause. A set operation is refused here, because its
    /// rows do not come from one table: <c>(SELECT … UNION SELECT …) FOR UPDATE</c> has no row to lock.
    /// The other shape rules (one base table, no grouping, no DISTINCT) need the bound query and are
    /// checked by the executor.
    /// </summary>
    public static NodeAst Wrap(NodeAst select, NodeAst clause)
    {
        if (SetOperationAst.IsSetOperationSelect(select))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "FOR UPDATE is not allowed with UNION, INTERSECT or EXCEPT");

        return new(NodeType.SelectForUpdate, select, clause, null, null, null, null, null, null);
    }

    /// <summary>
    /// The root type of <c>EXPLAIN (ANALYZE)</c> over <paramref name="target"/>:
    /// <see cref="NodeType.ExplainAnalyzeForUpdate"/> for a locking read, whose ANALYZE takes real row
    /// locks, and <see cref="NodeType.ExplainAnalyze"/> otherwise.
    /// </summary>
    public static NodeType ExplainAnalyzeTypeOf(NodeAst target) =>
        target.nodeType == NodeType.SelectForUpdate ? NodeType.ExplainAnalyzeForUpdate : NodeType.ExplainAnalyze;

    /// <summary>The wait policy of a <see cref="NodeType.LockingClause"/>.</summary>
    public static string PolicyOf(NodeAst clause) => clause.yytext ?? Wait;

    private static void Flatten(NodeAst? node, List<NodeAst> list)
    {
        // The list is left-nested and as long as the clause has words (a handful), so the walk is
        // iterative on the left spine.
        Stack<NodeAst> right = new();

        while (node is { nodeType: NodeType.IdentifierList })
        {
            right.Push(node.rightAst!);
            node = node.leftAst;
        }

        if (node is not null)
            list.Add(node);

        while (right.Count > 0)
            list.Add(right.Pop());
    }
}
