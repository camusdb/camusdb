/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Deterministic, rule-based join-order heuristic.
///
/// Rules (applied in priority order, stable for ties):
///   0 — source has an equality predicate on a unique-indexed column (point-lookup; at most one row)
///   1 — source has any predicate on an indexed column (range or equality scan)
///   2 — source has no pushable predicate (full table/index scan)
///
/// Lower score → outer side of the nested-loop join (drives the loop with fewer rows).
///
/// After scoring, the optimizer rebuilds a left-deep <see cref="QuerySource"/> tree using the
/// reordered source list.  Each join edge is assigned the predicate from the original predicate
/// pool that best connects the new right source to the accumulated left-side result.
///
/// Feasibility guard: if no valid predicate can be assigned for some join edge after reordering
/// (e.g. a chain join A→B→C where no A→C predicate exists), the optimizer falls back to the
/// original declared source order.
///
/// Outer-join guard: the optimizer bails out immediately if any join in the tree has a kind
/// other than <see cref="JoinKind.Inner"/>.  Outer joins are not commutative and must not be
/// reordered.
///
/// Volatile guard: a tree with an <c>ON</c> predicate that calls a volatile function
/// (<c>random()</c>, <c>now()</c>, …) keeps its declared order. A whole <c>ON</c> runs once for
/// each row that reaches its join; reordering changes which rows, and how many, reach it.
///
/// Cross-product repair (<see cref="ChooseWithGraph"/>): the steps above work on whole
/// <c>ON</c> predicates, so a comma join whose tables connect only through a later table keeps a
/// cross product (<c>ON TRUE</c>) that its predicates do not need. When the resulting tree has
/// such a join and the conjunct graph (<see cref="JoinGraph"/>) is connected, the repair builds a
/// connected order instead: from the scored order, each step takes the first remaining table that
/// connects to the tables already in the tree. A tree with no avoidable cross product is returned
/// unchanged, so its plan is the same as before the repair. The repair needs no statistics, so it
/// also runs when the cost-based enumerator cannot: with the flag off, while a row count is not
/// loaded, above the enumeration cap, and when the enumerator finds no plan.
///
/// INL placement note: scoring is based purely on pushed-down WHERE scan-selectivity, not on
/// whether a source holds the indexed join key that would enable
/// <see cref="IndexNestedLoopJoinNode"/> selection.  Placing a selectively filtered source on
/// the outer side is generally correct (it narrows the outer loop), and
/// <see cref="JoinEquiJoinAnalyzer"/> independently picks the INL path when the right source's
/// join column is indexed.  In the uncommon case where the selectively filtered source is also
/// the ideal INL inner (indexed join key on it), the optimizer may pull it to the outer side and lose the
/// INL opportunity.  A future cost-model pass can address this by incorporating join-key
/// index availability into the scoring function.
/// </summary>
internal static class JoinOrderOptimizer
{
    private sealed record LeafEntry(QuerySource AstNode, string Alias);

    /// <summary>
    /// Applies a previously-computed alias ordering (e.g. from the plan cache) to
    /// <paramref name="root"/>, using the <em>current</em> query's ON predicates.
    ///
    /// Unlike <see cref="Reorder"/> (which scores sources to determine the order),
    /// this overload forces the leaf order to match <paramref name="targetOrder"/>.
    /// The ON predicates are always taken from <paramref name="root"/> so that literal
    /// values inside <c>JOIN … ON …</c> clauses are never frozen from a cached plan.
    ///
    /// Falls back to <paramref name="root"/> unchanged when:
    /// <list type="bullet">
    ///   <item>any join has a kind other than <see cref="JoinKind.Inner"/> (outer joins are not commutative);</item>
    ///   <item>any alias in <paramref name="targetOrder"/> is not found in the current tree (stale cache, defensive);</item>
    ///   <item>no feasible predicate assignment exists for the given ordering.</item>
    /// </list>
    /// </summary>
    public static QuerySource ReorderByAliases(
        QuerySource root,
        IReadOnlyList<string> targetOrder,
        BoundSelectQuery bound)
    {
        List<LeafEntry> leaves = [];
        List<NodeAst> predicatePool = [];

        if (!CollectLeaves(root, leaves, predicatePool))
            return root; // outer joins — not commutative, bail

        if (leaves.Count <= 1 || leaves.Count != targetOrder.Count || HasVolatilePredicate(predicatePool))
            return root;

        // Build alias → leaf map from the current source tree.
        Dictionary<string, LeafEntry> leafMap = new(StringComparer.OrdinalIgnoreCase);
        foreach (LeafEntry leaf in leaves)
            leafMap[leaf.Alias] = leaf;

        // Reconstruct the leaf list in the target order, using CURRENT source nodes.
        List<LeafEntry> reordered = new(leaves.Count);
        foreach (string alias in targetOrder)
        {
            if (!leafMap.TryGetValue(alias, out LeafEntry? entry))
                return root; // unknown alias (stale cache), bail safely
            reordered.Add(entry);
        }

        // Skip the rebuild when the target order matches the current order already.
        bool changed = reordered.Zip(leaves).Any(p => p.First.Alias != p.Second.Alias);
        if (!changed)
            return root;

        // Feasibility guard: verify a predicate exists for every join edge in this ordering.
        if (!IsFeasible(reordered, predicatePool, bound))
            return root; // can't re-apply this order with current predicates, fall back

        return RebuildTree(reordered, predicatePool, bound);
    }

    /// <summary>
    /// The heuristic join order with the cross-product repair (see the class summary). Builds the
    /// conjunct graph of <paramref name="root"/> first.
    /// </summary>
    public static JoinOrderChoice Choose(
        QuerySource root,
        BoundSelectQuery bound,
        JoinPredicatePushdown.Result pushdown)
    {
        JoinGraph? graph = JoinGraph.TryBuild(root, bound, pushdown.PostJoinConjuncts);
        return ChooseWithGraph(root, bound, pushdown, graph);
    }

    /// <summary>
    /// The heuristic join order with the cross-product repair, over a graph that the caller
    /// already built. A null <paramref name="graph"/> means the graph refused the tree; the
    /// heuristic tree is then returned with no repair.
    /// </summary>
    public static JoinOrderChoice ChooseWithGraph(
        QuerySource root,
        BoundSelectQuery bound,
        JoinPredicatePushdown.Result pushdown,
        JoinGraph? graph)
    {
        QuerySource heuristic = Reorder(root, bound, pushdown);

        if (graph is null || graph.LeafCount < 2 || graph.EveryJoinConnects(heuristic))
            return JoinOrderChoice.WholePredicate(heuristic, pushdown);

        // The heuristic tree has a cross product. Repair it only when the predicates do not need
        // one; a query that needs a cross product keeps the heuristic tree.
        int[]? order = graph.FindConnectedOrder(ScoredOrder(graph, bound, pushdown));
        if (order is null)
            return JoinOrderChoice.WholePredicate(heuristic, pushdown);

        QuerySource? repaired = graph.BuildTree(order);

        return repaired is null
            ? JoinOrderChoice.WholePredicate(heuristic, pushdown)
            : JoinOrderChoice.Conjunct(repaired, graph);
    }

    /// <summary>
    /// The leaves of <paramref name="graph"/> sorted by the heuristic score, ties in declared
    /// order: the same order <see cref="Reorder"/> tries first.
    /// </summary>
    private static List<int> ScoredOrder(
        JoinGraph graph,
        BoundSelectQuery bound,
        JoinPredicatePushdown.Result pushdown)
    {
        List<(int Leaf, int Score)> scored = new(graph.LeafCount);

        for (int i = 0; i < graph.LeafCount; i++)
        {
            JoinGraph.Leaf leaf = graph.Leaves[i];
            scored.Add((i, ScoreSource(new LeafEntry(leaf.Node, leaf.Alias), bound, pushdown)));
        }

        scored.Sort((a, b) => a.Score != b.Score ? a.Score.CompareTo(b.Score) : a.Leaf.CompareTo(b.Leaf));

        List<int> order = new(scored.Count);
        foreach ((int leaf, _) in scored)
            order.Add(leaf);

        return order;
    }

    /// <summary>
    /// Returns a (potentially reordered) <see cref="QuerySource"/> tree root. The tree structure
    /// is always left-deep. If the heuristic produces no improvement or the reordering is not
    /// feasible, returns <paramref name="root"/> unchanged. Each join keeps one whole <c>ON</c>
    /// predicate; <see cref="Choose"/> adds the cross-product repair on top.
    /// </summary>
    public static QuerySource Reorder(
        QuerySource root,
        BoundSelectQuery bound,
        JoinPredicatePushdown.Result pushdown)
    {
        List<LeafEntry> leaves = [];
        List<NodeAst> predicatePool = [];

        // Outer joins are not commutative; bail immediately if any join kind is not Inner.
        if (!CollectLeaves(root, leaves, predicatePool))
            return root;

        if (leaves.Count <= 1 || HasVolatilePredicate(predicatePool))
            return root;

        // Score and sort (stable: primary=score, secondary=declared order)
        List<(LeafEntry Leaf, int Score, int Idx)> scored = [];
        for (int i = 0; i < leaves.Count; i++)
            scored.Add((leaves[i], ScoreSource(leaves[i], bound, pushdown), i));

        scored.Sort((a, b) => a.Score != b.Score ? a.Score.CompareTo(b.Score) : a.Idx.CompareTo(b.Idx));

        List<LeafEntry> reordered = scored.Select(x => x.Leaf).ToList();

        // If order did not change, skip the rebuild entirely.
        bool changed = reordered.Zip(leaves).Any(p => p.First.Alias != p.Second.Alias);
        if (!changed)
            return root;

        // Feasibility guard: ensure every join step has a connecting predicate.
        if (!IsFeasible(reordered, predicatePool, bound))
            return root;

        return RebuildTree(reordered, predicatePool, bound);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Tree flattening
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Returns <c>false</c> if any <see cref="JoinSource"/> in the tree has a kind other than
    /// <see cref="JoinKind.Inner"/>. Outer joins are not commutative and must not be reordered.
    /// </summary>
    private static bool CollectLeaves(
        QuerySource node,
        List<LeafEntry> leaves,
        List<NodeAst> predicatePool)
    {
        switch (node)
        {
            case TableSource ts:
                leaves.Add(new(ts, ts.Alias ?? ts.TableName));
                return true;

            case DerivedTableSource ds:
                leaves.Add(new(ds, ds.Alias!));
                return true;

            case JoinSource js:
                if (js.Kind != JoinKind.Inner)
                    return false;

                if (!CollectLeaves(js.Left, leaves, predicatePool))
                    return false;

                if (!CollectLeaves(js.Right, leaves, predicatePool))
                    return false;

                predicatePool.Add(js.OnPredicate);
                return true;

            default:
                // Unknown QuerySource subtype: bail so the caller keeps the ORIGINAL join tree.
                // Returning true here would accept the node WITHOUT adding it to the leaves,
                // and the rebuilt tree would silently drop that source's rows.
                return false;
        }
    }

    /// <summary>
    /// True when an <c>ON</c> predicate calls a volatile function. See the volatile guard in the
    /// class summary.
    /// </summary>
    private static bool HasVolatilePredicate(List<NodeAst> predicatePool)
    {
        foreach (NodeAst predicate in predicatePool)
        {
            if (ScalarFunctionEvaluator.ContainsVolatileFunction(predicate))
                return true;
        }

        return false;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Scoring
    // ─────────────────────────────────────────────────────────────────────────

    private static int ScoreSource(
        LeafEntry leaf,
        BoundSelectQuery bound,
        JoinPredicatePushdown.Result pushdown)
    {
        if (!pushdown.ScanFiltersByAlias.TryGetValue(leaf.Alias, out NodeAst? scanFilter) || scanFilter is null)
            return 2;

        TableDescriptor? table = FindTable(leaf.Alias, bound);
        if (table is null)
            return 2;

        if (HasUniqueEqualityPredicate(scanFilter, table))
            return 0;

        if (HasIndexedPredicate(scanFilter, table))
            return 1;

        return 2;
    }

    private static bool HasUniqueEqualityPredicate(NodeAst filter, TableDescriptor table)
    {
        List<NodeAst> conjuncts = [];
        PredicateAnalyzer.CollectAndConjuncts(filter, conjuncts);

        foreach (NodeAst conjunct in conjuncts)
        {
            if (conjunct.nodeType != NodeType.ExprEquals)
                continue;

            string? lCol = ExtractUnqualifiedColumn(conjunct.leftAst);
            string? rCol = ExtractUnqualifiedColumn(conjunct.rightAst);

            foreach (string? col in (string?[])[lCol, rCol])
            {
                if (col is null)
                    continue;

                foreach (TableIndexSchema idx in table.Indexes.Values)
                {
                    if (idx.Type == IndexType.Unique && idx.Columns is [var c] && c == col)
                        return true;
                }
            }
        }

        return false;
    }

    private static bool HasIndexedPredicate(NodeAst filter, TableDescriptor table)
    {
        List<NodeAst> conjuncts = [];
        PredicateAnalyzer.CollectAndConjuncts(filter, conjuncts);

        HashSet<string> indexedCols = new(StringComparer.Ordinal);
        foreach (TableIndexSchema idx in table.Indexes.Values)
            foreach (string col in idx.Columns)
                indexedCols.Add(col);

        foreach (NodeAst conjunct in conjuncts)
        {
            string? col = ExtractUnqualifiedColumn(conjunct.leftAst)
                       ?? ExtractUnqualifiedColumn(conjunct.rightAst);
            if (col is not null && indexedCols.Contains(col))
                return true;
        }

        return false;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Feasibility guard
    // ─────────────────────────────────────────────────────────────────────────

    private static bool IsFeasible(
        List<LeafEntry> reordered,
        List<NodeAst> predicatePool,
        BoundSelectQuery bound)
    {
        HashSet<string> inScope = [reordered[0].Alias];
        List<NodeAst> remaining = new(predicatePool);

        for (int i = 1; i < reordered.Count; i++)
        {
            string rightAlias = reordered[i].Alias;
            HashSet<string> allowed = new(inScope) { rightAlias };

            NodeAst? feasible = null;
            foreach (NodeAst pred in remaining)
            {
                HashSet<string> refs = CollectReferencedAliases(pred, bound);
                if (refs.Contains(rightAlias) && refs.IsSubsetOf(allowed))
                {
                    feasible = pred;
                    break;
                }
            }

            if (feasible is null)
                return false;

            remaining.Remove(feasible);
            inScope.Add(rightAlias);
        }

        return true;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Tree rebuild
    // ─────────────────────────────────────────────────────────────────────────

    private static QuerySource RebuildTree(
        List<LeafEntry> sources,
        List<NodeAst> predicatePool,
        BoundSelectQuery bound)
    {
        QuerySource tree = sources[0].AstNode;
        HashSet<string> inScope = [sources[0].Alias];
        List<NodeAst> remaining = new(predicatePool);

        for (int i = 1; i < sources.Count; i++)
        {
            string rightAlias = sources[i].Alias;
            HashSet<string> allowed = new(inScope) { rightAlias };

            // Pick the predicate whose references are fully in scope for this step.
            NodeAst? chosen = null;
            foreach (NodeAst pred in remaining)
            {
                HashSet<string> refs = CollectReferencedAliases(pred, bound);
                if (refs.Contains(rightAlias) && refs.IsSubsetOf(allowed))
                {
                    chosen = pred;
                    break;
                }
            }

            // Feasibility was already verified; chosen must be non-null here.
            if (chosen is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"JoinOrderOptimizer: no predicate found for join step {i} (alias '{rightAlias}')");

            remaining.Remove(chosen);
            tree = new JoinSource(tree, sources[i].AstNode, JoinKind.Inner, chosen);
            inScope.Add(rightAlias);
        }

        return tree;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    private static HashSet<string> CollectReferencedAliases(NodeAst node, BoundSelectQuery bound)
    {
        HashSet<string> aliases = new(StringComparer.Ordinal);
        WalkIdentifiers(node, bound, aliases);
        return aliases;
    }

    private static void WalkIdentifiers(NodeAst? node, BoundSelectQuery bound, HashSet<string> aliases)
    {
        if (node is null)
            return;

        if (node.nodeType == NodeType.Identifier && node.yytext is not null)
        {
            AddIdentifierAlias(node.yytext, bound, aliases);
            return;
        }

        WalkIdentifiers(node.leftAst, bound, aliases);
        WalkIdentifiers(node.rightAst, bound, aliases);
        WalkIdentifiers(node.extendedOne, bound, aliases);
        WalkIdentifiers(node.extendedTwo, bound, aliases);
        WalkIdentifiers(node.extendedThree, bound, aliases);
        WalkIdentifiers(node.extendedFour, bound, aliases);
        WalkIdentifiers(node.extendedFive, bound, aliases);
    }

    private static void AddIdentifierAlias(string identifier, BoundSelectQuery bound, HashSet<string> aliases)
    {
        int dot = identifier.IndexOf('.');

        if (dot > 0 && dot < identifier.Length - 1)
        {
            aliases.Add(identifier[..dot]);
            return;
        }

        // Unqualified: resolve to whichever source uniquely owns this column.
        List<string> owners = [];

        foreach (BoundTableSource src in bound.Sources)
            if (BoundSourceCatalog.TableHasColumn(src, identifier))
                owners.Add(src.Alias);

        foreach (BoundDerivedTableSource src in bound.DerivedSources)
            if (src.HasColumn(identifier))
                owners.Add(src.Alias);

        if (owners.Count == 1)
            aliases.Add(owners[0]);
    }

    private static string? ExtractUnqualifiedColumn(NodeAst? node)
    {
        if (node is null || node.nodeType != NodeType.Identifier || node.yytext is null)
            return null;

        string text = node.yytext;
        int dot = text.IndexOf('.');
        return dot >= 0 ? text[(dot + 1)..] : text;
    }

    private static TableDescriptor? FindTable(string alias, BoundSelectQuery bound)
    {
        foreach (BoundTableSource src in bound.Sources)
            if (src.Alias == alias)
                return src.Table;

        return null;
    }
}
