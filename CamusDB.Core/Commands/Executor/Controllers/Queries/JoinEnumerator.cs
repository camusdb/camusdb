/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Numerics;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Predicates;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Statistics;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// System-R–style bottom-up DP join-order enumerator.
///
/// Replaces <see cref="JoinOrderOptimizer"/>'s heuristic reordering with a cost-based
/// dynamic program: for each subset of joined tables, memoize the cheapest left-deep
/// plan (cost + estimated output cardinality). Build subsets bottom-up from singletons
/// to the full N-table join.
///
/// Cost model:
/// <list type="bullet">
///   <item>Single-table scan: if a pushed-down filter hits an indexed leading column →
///     cost = estimatedRows × 2 (index entry + row fetch); otherwise → cost = tableRowCount
///     (full scan with in-memory filter).</item>
///   <item>Extending plan(leftMask) with rightTable via INLJ (right side has index on join
///     key leading column): cost = leftCard × 2 (one index lookup + row fetch per left row).</item>
///   <item>Extending plan(leftMask) with rightTable via hash join:
///     cost = rightScanCost (build phase dominates).</item>
///   <item>Pick INLJ when available and cheaper; otherwise hash.</item>
/// </list>
///
/// Scope:
/// <list type="bullet">
///   <item>Left-deep plans only (bushy plans are a future extension).</item>
///   <item>Inner joins only — same outer-join guard as <see cref="JoinOrderOptimizer"/>.</item>
///   <item>Capped at <see cref="MaxTablesForEnumeration"/> tables; wider joins fall back to
///     the heuristic.</item>
///   <item>Interesting orders (folding sort cost into candidate comparison) are a future
///     enhancement.</item>
/// </list>
///
/// Gated by <see cref="CamusDBConfig.CostBasedJoinOrderEnabled"/> (default false). With the
/// flag off plans are byte-identical to the heuristic output.
/// </summary>
internal static class JoinEnumerator
{
    internal const int MaxTablesForEnumeration = 12;

    private sealed record LeafEntry(QuerySource AstNode, string Alias);

    private sealed record SubplanEntry(
        QuerySource AstTree,
        double TotalCost,
        long OutputCardinality);

    /// <summary>
    /// Returns the cheapest left-deep join ordering for <paramref name="root"/> (inner joins only),
    /// or falls back to <see cref="JoinOrderOptimizer.Reorder"/> for outer joins or when the table
    /// count exceeds <see cref="MaxTablesForEnumeration"/>.
    /// </summary>
    public static QuerySource Enumerate(
        QuerySource root,
        BoundSelectQuery bound,
        JoinPredicatePushdown.Result pushdown,
        DatabaseDescriptor database,
        StatisticsManager stats)
    {
        List<LeafEntry> leaves = [];
        List<NodeAst> predicatePool = [];

        if (!CollectLeaves(root, leaves, predicatePool))
            return JoinOrderOptimizer.Reorder(root, bound, pushdown);

        if (leaves.Count <= 1)
            return root;

        if (leaves.Count > MaxTablesForEnumeration)
            return JoinOrderOptimizer.Reorder(root, bound, pushdown);

        int n = leaves.Count;

        double[] scanCost = new double[n];
        long[] scanCard = new long[n];

        for (int i = 0; i < n; i++)
            EstimateLeaf(i, leaves, pushdown, bound, database, stats, out scanCost[i], out scanCard[i]);

        // Precompute one alias bitmask per pool predicate: the DP evaluates connectivity
        // ~n·2^(n−1) times, and re-walking each ON AST (allocating a HashSet per probe) at every
        // (mask, bit) candidate is ~250k AST walks for a 12-table plan. With a bitmask the test
        // is two integer ops. Aliases that resolve to no leaf set a sentinel bit so a predicate
        // referencing an unknown alias can never satisfy the subset test (same outcome as the
        // HashSet-based check, computed once instead of per candidate).
        long[] predicateAliasMasks = BuildPredicateAliasMasks(predicatePool, leaves, bound);

        SubplanEntry?[] dp = new SubplanEntry?[1 << n];

        for (int i = 0; i < n; i++)
            dp[1 << i] = new SubplanEntry(leaves[i].AstNode, scanCost[i], scanCard[i]);

        for (int size = 2; size <= n; size++)
        {
            foreach (int mask in SubsetsOfSize(n, size))
            {
                for (int bit = 0; bit < n; bit++)
                {
                    if ((mask & (1 << bit)) == 0)
                        continue;

                    int leftMask = mask ^ (1 << bit);
                    if (dp[leftMask] is not { } leftPlan)
                        continue;

                    NodeAst? pred = FindConnectingPredicate(bit, leftMask, predicatePool, predicateAliasMasks);
                    if (pred is null)
                        continue;

                    TableDescriptor? rightTable = FindTableDescriptor(leaves[bit].Alias, bound);
                    double joinCost = EstimateJoinCost(
                        leftPlan.OutputCardinality,
                        scanCost[bit],
                        rightTable,
                        pred,
                        leaves[bit].Alias);

                    double totalCost = leftPlan.TotalCost + joinCost;

                    long outputCard = EstimateJoinCard(
                        leftPlan.OutputCardinality,
                        scanCard[bit],
                        leftMask, bit, leaves, pred, bound, database, stats);

                    if (dp[mask] is null || totalCost < dp[mask]!.TotalCost)
                    {
                        QuerySource joinTree = new JoinSource(
                            leftPlan.AstTree, leaves[bit].AstNode, JoinKind.Inner, pred);
                        dp[mask] = new SubplanEntry(joinTree, totalCost, outputCard);
                    }
                }
            }
        }

        int fullMask = (1 << n) - 1;
        return dp[fullMask] is { } best ? best.AstTree : JoinOrderOptimizer.Reorder(root, bound, pushdown);
    }

    // ─── Leaf cost ───────────────────────────────────────────────────────────

    private static void EstimateLeaf(
        int idx,
        List<LeafEntry> leaves,
        JoinPredicatePushdown.Result pushdown,
        BoundSelectQuery bound,
        DatabaseDescriptor database,
        StatisticsManager stats,
        out double cost,
        out long card)
    {
        string alias = leaves[idx].Alias;
        TableDescriptor? table = FindTableDescriptor(alias, bound);

        long tableRows = (table is not null
            ? stats.GetRowCountEstimate(database, table)
            : null) ?? CostEstimator.DefaultTableRowCount;

        pushdown.ScanFiltersByAlias.TryGetValue(alias, out NodeAst? filter);

        if (filter is null || table is null)
        {
            cost = tableRows;
            card = tableRows;
            return;
        }

        // When CBO access-path selection is enabled, BuildJoinTree may emit an IndexRangeScanNode
        // instead of a TableScanNode. Mirror that decision here so the DP costs what is actually built.
        if (CamusDBConfig.CostBasedAccessPathEnabled)
        {
            PredicateAnalysis raw = PredicateAnalyzer.Analyze(filter, null);
            PredicateAnalysis analysis = JoinEnumerator.StripAliasPrefix(raw, alias);

            if (analysis.IndexableComparisons.Count > 0 || analysis.InListComparisons.Count > 0)
            {
                QueryPlanStep? step = IndexScanSelector.TrySelectScan(table, analysis);
                if (step is { Type: QueryPlanStepType.RangeScanFromIndex } rStep
                    && (rStep.FromBound is not null || rStep.ToBound is not null))
                {
                    var candidate = (IndexRangeScanNode)QueryPlanner.ToScanNode(step.Value);

                    // Breakeven veto through the SAME decision the builder (TryBuildIndexLeaf)
                    // uses — a hand-rolled copy of the comparison can drift and make the DP cost
                    // a plan shape the builder then refuses to build.
                    if (!CostEstimator.ShouldPreferFullScan(candidate, tableRows, stats, database, table))
                    {
                        long rangeRows = CostEstimator.EstimateRangeScanRows(candidate, tableRows, stats, database, table);
                        cost = rangeRows * 2.0; // index entry + row fetch
                        card = rangeRows;
                        return;
                    }
                }

                // IN-list leaf: same costing as TryBuildIndexLeaf uses.
                foreach (AnalyzedInList inList in analysis.InListComparisons)
                {
                    if (inList.Values.Count == 0) continue;

                    TableIndexSchema? bestIndex = null;
                    bool bestIsUnique = false;
                    foreach (TableIndexSchema tidx in table.Indexes.Values)
                    {
                        if (!SchemaElementStateRules.IsReadableIndex(table.Schema, tidx)) continue;
                        if (tidx.Columns.Length == 0 ||
                            !string.Equals(tidx.Columns[0], inList.ColumnName, StringComparison.Ordinal))
                            continue;
                        bool isUnique = tidx.Type == IndexType.Unique;
                        if (isUnique && tidx.Columns.Length != 1) continue;
                        if (bestIndex is null || (isUnique && !bestIsUnique))
                        {
                            bestIndex = tidx;
                            bestIsUnique = isUnique;
                        }
                    }

                    if (bestIndex is null) continue;

                    long inListRows = CardinalityEstimator.EstimateInListRows(
                        inList.ColumnName, inList.Values, bestIsUnique, tableRows, database, table, stats);
                    long inListBreakeven = (long)Math.Ceiling(tableRows * CostEstimator.BreakevenFraction);
                    if (inListRows >= inListBreakeven) continue;

                    cost = inListRows * 2.0;
                    card = inListRows;
                    return;
                }
            }
        }

        double selectivity = CardinalityEstimator.EstimateFilterSelectivity(filter, database, table, stats);
        long estimatedRows = Math.Max(1L, (long)(tableRows * selectivity));

        // Full scan + residual ExecutionFilter (the physical plan BuildJoinTree produces when no
        // selective index scan is chosen).
        cost = tableRows;
        card = estimatedRows;
    }

    // No longer used for cost: BuildJoinTree never builds an index scan for a join leaf (always
    // TableScanNode), so pricing estimatedRows×2 modelled a plan that does not exist. Kept for
    // reference until join-leaf index-scan support lands (a documented follow-up).
    private static bool HasIndexedFilter(NodeAst filter, TableDescriptor table)
    {
        List<NodeAst> conjuncts = [];
        PredicateAnalyzer.CollectAndConjuncts(filter, conjuncts);

        HashSet<string> indexedLeadingCols = new(StringComparer.Ordinal);
        foreach (TableIndexSchema idx in table.Indexes.Values)
        {
            if (!SchemaElementStateRules.IsReadable(idx)) continue;
            if (idx.Columns is { Length: > 0 })
                indexedLeadingCols.Add(idx.Columns[0]);
        }

        foreach (NodeAst conjunct in conjuncts)
        {
            string? col = ExtractUnqualifiedColumn(conjunct.leftAst)
                       ?? ExtractUnqualifiedColumn(conjunct.rightAst);
            if (col is not null && indexedLeadingCols.Contains(col))
                return true;
        }

        return false;
    }

    // ─── Join cost ───────────────────────────────────────────────────────────

    private static double EstimateJoinCost(
        long leftCard,
        double rightScanCost,
        TableDescriptor? rightTable,
        NodeAst pred,
        string rightAlias)
    {
        if (rightTable is not null)
        {
            string? rightJoinKey = ExtractRightJoinKey(pred, rightAlias);
            if (rightJoinKey is not null && TableHasIndexOnLeadingColumn(rightTable, rightJoinKey))
            {
                double inljCost = leftCard * 2.0;
                return Math.Min(inljCost, rightScanCost);
            }
        }

        return rightScanCost;
    }

    private static bool TableHasIndexOnLeadingColumn(TableDescriptor table, string columnName)
    {
        foreach (TableIndexSchema idx in table.Indexes.Values)
        {
            if (!SchemaElementStateRules.IsReadable(idx)) continue;
            if (idx.Columns is { Length: > 0 } && idx.Columns[0] == columnName)
                return true;
        }
        return false;
    }

    private static string? ExtractRightJoinKey(NodeAst pred, string rightAlias)
    {
        if (pred.nodeType != NodeType.ExprEquals) return null;

        if (pred.leftAst?.yytext is { } lt && TrySplitQualified(lt, out string la, out string lc) && la == rightAlias)
            return lc;

        if (pred.rightAst?.yytext is { } rt && TrySplitQualified(rt, out string ra, out string rc) && ra == rightAlias)
            return rc;

        return null;
    }

    private static long EstimateJoinCard(
        long leftCard,
        long rightCard,
        int leftMask,
        int rightBit,
        List<LeafEntry> leaves,
        NodeAst pred,
        BoundSelectQuery bound,
        DatabaseDescriptor database,
        StatisticsManager stats)
    {
        string rightAlias = leaves[rightBit].Alias;
        string? rightKey = ExtractRightJoinKey(pred, rightAlias);
        string? leftKey = ExtractLeftJoinKey(pred, rightAlias);

        string[] rightKeyColumns = rightKey is null ? [] : [rightKey];
        string[] leftKeyColumns = leftKey is null ? [] : [leftKey];

        TableDescriptor? rightTable = FindTableDescriptor(rightAlias, bound);
        TableDescriptor? leftTable = leftMask != 0 && (leftMask & (leftMask - 1)) == 0
            ? FindTableDescriptor(leaves[BitOperations.TrailingZeroCount((uint)leftMask)].Alias, bound)
            : null;

        return CardinalityEstimator.EstimateJoinCardinality(
            leftCard, rightCard,
            rightKeyColumns, rightTable,
            leftKeyColumns, leftTable,
            database, stats);
    }

    private static string? ExtractLeftJoinKey(NodeAst pred, string rightAlias)
    {
        if (pred.nodeType != NodeType.ExprEquals) return null;

        if (pred.leftAst?.yytext is { } lt && TrySplitQualified(lt, out string la, out string lc) && la != rightAlias)
            return lc;

        if (pred.rightAst?.yytext is { } rt && TrySplitQualified(rt, out string ra, out string rc) && ra != rightAlias)
            return rc;

        return null;
    }

    // ─── Predicate matching ──────────────────────────────────────────────────

    // Sentinel bit for aliases referenced by a predicate that match no join leaf: it is never
    // part of any candidate mask, so such predicates fail the subset test (never connect).
    private const long UnknownAliasBit = 1L << 62;

    /// <summary>
    /// Walks each pool predicate ONCE and encodes its referenced aliases as a bitmask over the
    /// leaf indices, so the DP's per-candidate connectivity test is pure integer arithmetic
    /// instead of an AST walk + HashSet subset check.
    /// </summary>
    private static long[] BuildPredicateAliasMasks(
        List<NodeAst> predicatePool,
        List<LeafEntry> leaves,
        BoundSelectQuery bound)
    {
        Dictionary<string, int> bitByAlias = new(leaves.Count, StringComparer.Ordinal);
        for (int i = 0; i < leaves.Count; i++)
            bitByAlias[leaves[i].Alias] = i;

        long[] masks = new long[predicatePool.Count];
        for (int p = 0; p < predicatePool.Count; p++)
        {
            long mask = 0;
            foreach (string alias in CollectReferencedAliases(predicatePool[p], bound))
            {
                if (bitByAlias.TryGetValue(alias, out int bit))
                    mask |= 1L << bit;
                else
                    mask |= UnknownAliasBit;
            }
            masks[p] = mask;
        }

        return masks;
    }

    private static NodeAst? FindConnectingPredicate(
        int rightBit,
        int leftMask,
        List<NodeAst> predicatePool,
        long[] predicateAliasMasks)
    {
        long combined = (uint)(leftMask | (1 << rightBit));
        long rightMask = 1L << rightBit;

        for (int p = 0; p < predicatePool.Count; p++)
        {
            long refs = predicateAliasMasks[p];
            // Must reference the newly added right leaf, and every referenced alias must be
            // inside the combined set (subset test: no bits outside `combined`).
            if ((refs & rightMask) != 0 && (refs & ~combined) == 0)
                return predicatePool[p];
        }

        return null;
    }

    // ─── Tree flattening ─────────────────────────────────────────────────────

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

    // ─── Subset enumeration ──────────────────────────────────────────────────

    private static IEnumerable<int> SubsetsOfSize(int n, int size)
    {
        int mask = (1 << size) - 1;
        int limit = 1 << n;
        while (mask < limit)
        {
            yield return mask;
            // Gosper's hack: next integer with the same popcount.
            int c = mask & -mask;
            int r = mask + c;
            mask = (((r ^ mask) >> 2) / c) | r;
        }
    }

    // ─── Shared utilities ────────────────────────────────────────────────────

    /// <summary>
    /// Returns a <see cref="PredicateAnalysis"/> where each comparison's column name has the
    /// <c>alias.</c> qualifier removed if present. The original <c>Conjunct</c> ASTs are
    /// preserved so residual evaluation still uses the original (possibly qualified) node text.
    /// </summary>
    internal static PredicateAnalysis StripAliasPrefix(PredicateAnalysis analysis, string alias)
    {
        string prefix = alias + ".";

        bool anyQualified =
            analysis.IndexableComparisons.Any(c => c.ColumnName.StartsWith(prefix, StringComparison.Ordinal))
            || analysis.InListComparisons.Any(c => c.ColumnName.StartsWith(prefix, StringComparison.Ordinal));

        if (!anyQualified)
            return analysis;

        List<AnalyzedComparison> stripped = new(analysis.IndexableComparisons.Count);
        foreach (AnalyzedComparison c in analysis.IndexableComparisons)
        {
            string col = c.ColumnName.StartsWith(prefix, StringComparison.Ordinal)
                ? c.ColumnName[prefix.Length..] : c.ColumnName;
            stripped.Add(col == c.ColumnName
                ? c : new AnalyzedComparison(col, c.Operator, c.Constant, c.Conjunct));
        }

        List<AnalyzedInList> strippedIn = new(analysis.InListComparisons.Count);
        foreach (AnalyzedInList il in analysis.InListComparisons)
        {
            string col = il.ColumnName.StartsWith(prefix, StringComparison.Ordinal)
                ? il.ColumnName[prefix.Length..] : il.ColumnName;
            strippedIn.Add(col == il.ColumnName ? il : il with { ColumnName = col });
        }

        return new PredicateAnalysis(stripped, analysis.ColumnComparisons, analysis.ResidualConjuncts, strippedIn);
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private static HashSet<string> CollectReferencedAliases(NodeAst node, BoundSelectQuery bound)
    {
        HashSet<string> aliases = new(StringComparer.Ordinal);
        WalkIdentifiers(node, bound, aliases);
        return aliases;
    }

    private static void WalkIdentifiers(NodeAst? node, BoundSelectQuery bound, HashSet<string> aliases)
    {
        if (node is null) return;

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

    private static bool TrySplitQualified(string id, out string alias, out string column)
    {
        int dot = id.IndexOf('.');
        if (dot > 0 && dot < id.Length - 1)
        {
            alias = id[..dot];
            column = id[(dot + 1)..];
            return true;
        }

        alias = column = "";
        return false;
    }

    private static TableDescriptor? FindTableDescriptor(string alias, BoundSelectQuery bound)
    {
        foreach (BoundTableSource src in bound.Sources)
            if (src.Alias == alias)
                return src.Table;

        return null;
    }
}
