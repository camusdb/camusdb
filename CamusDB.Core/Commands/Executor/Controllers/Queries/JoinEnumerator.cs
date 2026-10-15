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
/// System-R–style bottom-up dynamic program that selects the cheapest left-deep order for an
/// inner-join tree. For each subset of tables it keeps the cheapest plan, built from the cheapest
/// plan of the subset without one table plus one join step.
///
/// <para>The search runs over a <see cref="JoinGraph"/>, so tables connect through conjuncts,
/// not through whole <c>ON</c> clauses, and the cross-table conjuncts of <c>WHERE</c> count too.
/// A step is a candidate only when the new table connects to the tables before it; the DP never
/// costs a cross product. The winning order is built through <see cref="JoinGraph.BuildTree"/>,
/// which puts each conjunct at the join where its last table enters.</para>
///
/// <para>Cost model:</para>
/// <list type="bullet">
///   <item>First table: the cost of the scan that the builder makes for it (a full scan with the
///   pushed filter in memory, or an index range or IN-list scan when the access-path optimizer
///   would build one).</item>
///   <item>Join step: <see cref="CostEstimator.JoinStepCost"/>, with the operator that the builder
///   selects for the conjuncts that the step attaches. The right input of a join is read with a
///   full scan, so its read cost is its table row count, whatever filter it has. A hash step counts
///   the size of its left input, which is the intermediate result of the steps before it, so the
///   total of a plan is the scan costs plus the size of each intermediate result.</item>
///   <item>Builder agreement: <see cref="EstimateSteps"/> hands the builder the left rows, right
///   rows and right read cost of each join, and the builder selects the operator from them through
///   <see cref="CostEstimator.SelectJoinStepOperator"/>, the rule the step cost uses. The plan
///   therefore has the operators the order was costed with (merge join excepted: the builder can
///   still prefer it, and the DP does not model it).</item>
///   <item>Ties: candidates are tried from the last declared table to the first, and a candidate
///   replaces the current best only when it is strictly cheaper. The declared order is the first
///   candidate for each prefix set, so two orders of equal cost keep the order the user wrote.</item>
/// </list>
///
/// <para>Row estimates: <see cref="EstimateSubsetRows"/> gives one estimate for each subset of
/// tables, from the subset alone. A dynamic program over subsets is correct only when "the best
/// plan for set S" has one meaning, which needs the same row estimate for every plan of S.</para>
///
/// <para>Scope and fallbacks:</para>
/// <list type="bullet">
///   <item>Left-deep plans only.</item>
///   <item>Inner joins only. When <see cref="JoinGraph.TryBuild"/> refuses the tree (an outer
///   join, a subquery in an <c>ON</c>, an identifier it cannot resolve), the heuristic
///   <see cref="JoinOrderOptimizer"/> plans it with whole predicates.</item>
///   <item>A volatile <c>ON</c> conjunct sets an order boundary: no order joins another table
///   between the tables of its scope (see <see cref="JoinGraph"/>).</item>
///   <item>At most <see cref="MaxTablesForEnumeration"/> tables. A wider join, and a query whose
///   predicates need a cross product, also go to the heuristic, which repairs an avoidable cross
///   product (<see cref="JoinOrderOptimizer.ChooseWithGraph"/>).</item>
///   <item>Merge join and interesting orders are not modeled.</item>
/// </list>
///
/// <para>Gated by <see cref="CamusDBOptions.CostBasedJoinOrderEnabled"/> (default true). With the
/// flag off the planner uses the heuristic only.</para>
/// </summary>
internal static class JoinEnumerator
{
    internal const int MaxTablesForEnumeration = 12;

    /// <summary>
    /// One edge that names a table, seen from that table as the right input of a step: whether
    /// the builder can use the edge as a hash or merge key, and whether it can drive an index
    /// lookup into the table.
    /// </summary>
    private readonly record struct StepEdge(ulong Mask, bool EquiKey, bool IndexLookup);

    /// <summary>
    /// The inputs of the cost model for one graph, computed once and shared by the dynamic program
    /// and <see cref="EstimateSteps"/>, so the estimates that the builder receives are the ones the
    /// order was chosen with.
    /// </summary>
    private sealed class CostInputs
    {
        /// <summary>Cost of the scan when the leaf is the first input: the builder can use an index leaf there.</summary>
        public required double[] FirstLeafCost { get; init; }

        /// <summary>
        /// Cost of reading the leaf as the right input of a join. A hash or nested-loop join reads
        /// its right table through the same index leaf the leaf would get as the first input
        /// (<c>JoinQueryPlanner.AttachRightAccessPath</c>), so this is the same cost as
        /// <see cref="FirstLeafCost"/>: the index cost when the pushed filter drives an index, the
        /// table's row count otherwise.
        /// </summary>
        public required double[] RightScanCost { get; init; }

        /// <summary>Rows the leaf returns after its pushed filter.</summary>
        public required double[] LeafRows { get; init; }

        /// <summary>
        /// Row estimate of each subset, from <see cref="EstimateSubsetRows"/>, or null when the
        /// inputs serve a cache replay. The dynamic program reads every subset, so it gets the
        /// dense array; a replay reads only the prefixes of one known order, so it computes those
        /// alone through <see cref="RowsOf"/> instead of all 2^n.
        /// </summary>
        public required double[]? SubsetRows { get; init; }

        /// <summary>The selectivity model <see cref="EstimateMaskRows"/> reads for one subset.</summary>
        public required SelectivityModel Selectivity { get; init; }

        public required List<StepEdge>[] StepEdges { get; init; }

        /// <summary>
        /// The row estimate of the subset <paramref name="mask"/>: from the dense array when there is
        /// one, otherwise computed alone. Both use <see cref="EstimateMaskRows"/>, so the value is the
        /// same bit for bit.
        /// </summary>
        public double RowsOf(int mask) =>
            SubsetRows is { } dense ? dense[mask] : EstimateMaskRows(mask, LeafRows, Selectivity, []);
    }

    /// <summary>
    /// The inputs of a subset row estimate that do not depend on the subset: the pair and edge
    /// groups with their selectivities, and the column equivalence classes. Built once per plan by
    /// <see cref="BuildSelectivityGroups"/>.
    /// </summary>
    private sealed record SelectivityModel(List<(ulong Mask, double Selectivity)> Groups, List<EquivalenceClass> Classes);

    /// <summary>
    /// Returns the cheapest left-deep join tree for <paramref name="root"/>, the filter that stays
    /// after its last join, and the estimates of each join for the builder. Falls back to
    /// <see cref="JoinOrderOptimizer"/> when the graph refuses the tree, when it has more than
    /// <see cref="MaxTablesForEnumeration"/> tables, or when no order avoids a cross product.
    /// </summary>
    /// <param name="clockValues">
    /// The statement's transaction and statement times alone
    /// (<see cref="Functions.ClockScalarFunctions.ClockValuesOnly"/>), or null. A leaf filter on
    /// <c>now()</c> is costed as the index range the builder will make from the same values.
    /// </param>
    public static JoinOrderChoice Enumerate(
        QuerySource root,
        BoundSelectQuery bound,
        JoinPredicatePushdown.Result pushdown,
        DatabaseDescriptor database,
        StatisticsManager stats,
        CamusDBOptions options,
        Dictionary<string, ColumnValue>? clockValues = null)
    {
        JoinGraph? graph = JoinGraph.TryBuild(root, bound, pushdown.PostJoinConjuncts);

        if (graph is null)
            return JoinOrderOptimizer.ChooseWithGraph(root, bound, pushdown, graph: null);

        if (graph.LeafCount <= 1)
            return JoinOrderChoice.WholePredicate(root, pushdown);

        if (graph.LeafCount > MaxTablesForEnumeration)
            return JoinOrderOptimizer.ChooseWithGraph(root, bound, pushdown, graph);

        CostInputs inputs = ComputeCostInputs(graph, bound, pushdown, database, stats, options, clockValues);
        int[]? order = FindCheapestOrder(graph, inputs, out long costedSteps);

        // The fallbacks keep the count: the search ran, and its work is what the count reports.
        if (order is null)
            return JoinOrderOptimizer.ChooseWithGraph(root, bound, pushdown, graph) with { CostedJoinSteps = costedSteps };

        // A wrong conjunct count keeps the original tree: a conjunct evaluated zero or two times
        // changes the rows.
        QuerySource? tree = graph.BuildTree(order);

        return tree is null
            ? JoinOrderChoice.WholePredicate(root, pushdown) with { CostedJoinSteps = costedSteps }
            : JoinOrderChoice.CostBased(tree, graph, EstimateSteps(graph, order, tree, inputs), costedSteps);
    }

    /// <summary>
    /// The estimates of each join of <paramref name="tree"/>, a tree that
    /// <see cref="JoinGraph.BuildTree"/> built for <paramref name="order"/>: the rows of its left
    /// input (the subset estimate of the tables before it), the rows and the read cost of its right
    /// input, and the operator the dynamic program costed. The builder selects the operator from the
    /// same numbers through the same rule (<see cref="CostEstimator.SelectJoinStepOperator"/>), so
    /// the plan has the operators the order was chosen for. The plan-cache replay of a cost-based
    /// order calls this too, so a hit gets the operators of the miss.
    /// </summary>
    internal static IReadOnlyDictionary<JoinSource, JoinStepEstimate> EstimateSteps(
        JoinGraph graph,
        IReadOnlyList<int> order,
        QuerySource tree,
        BoundSelectQuery bound,
        JoinPredicatePushdown.Result pushdown,
        DatabaseDescriptor database,
        StatisticsManager stats,
        CamusDBOptions options,
        Dictionary<string, ColumnValue>? clockValues = null) =>
        EstimateSteps(graph, order, tree, ComputeCostInputs(graph, bound, pushdown, database, stats, options, clockValues, denseSubsets: false));

    private static IReadOnlyDictionary<JoinSource, JoinStepEstimate> EstimateSteps(
        JoinGraph graph,
        IReadOnlyList<int> order,
        QuerySource tree,
        CostInputs inputs)
    {
        // The joins of the left spine, bottom first: join k adds order[k].
        List<JoinSource> joins = [];
        QuerySource current = tree;

        while (current is JoinSource join)
        {
            joins.Add(join);
            current = join.Left;
        }

        joins.Reverse();

        Dictionary<JoinSource, JoinStepEstimate> steps = new(joins.Count, ReferenceEqualityComparer.Instance);
        int prefix = 1 << order[0];

        for (int k = 1; k < order.Count && k - 1 < joins.Count; k++)
        {
            int leaf = order[k];
            (bool equiKey, bool indexLookup) = StepOperators(inputs.StepEdges[leaf], (ulong)(prefix | (1 << leaf)));

            double leftRows = inputs.RowsOf(prefix);
            double rightRows = inputs.LeafRows[leaf];
            double rightScanCost = inputs.RightScanCost[leaf];

            steps[joins[k - 1]] = new JoinStepEstimate(
                leftRows,
                rightRows,
                rightScanCost,
                CostEstimator.SelectJoinStepOperator(leftRows, rightRows, rightScanCost, equiKey, indexLookup),
                inputs.RowsOf(prefix | (1 << leaf)));

            prefix |= 1 << leaf;
        }

        return steps;
    }

    /// <param name="denseSubsets">
    /// True to estimate every subset up front, for the dynamic program. A cache replay passes false:
    /// it reads only the n prefixes of its order, which <see cref="CostInputs.RowsOf"/> then computes
    /// one by one.
    /// </param>
    private static CostInputs ComputeCostInputs(
        JoinGraph graph,
        BoundSelectQuery bound,
        JoinPredicatePushdown.Result pushdown,
        DatabaseDescriptor database,
        StatisticsManager stats,
        CamusDBOptions options,
        Dictionary<string, ColumnValue>? clockValues = null,
        bool denseSubsets = true)
    {
        int n = graph.LeafCount;

        double[] firstLeafCost = new double[n];
        double[] rightScanCost = new double[n];
        double[] leafRows = new double[n];

        for (int i = 0; i < n; i++)
        {
            string alias = graph.Leaves[i].Alias;
            EstimateLeaf(alias, pushdown, bound, database, stats, out firstLeafCost[i], out long rows, options, clockValues);
            leafRows[i] = rows;
            rightScanCost[i] = firstLeafCost[i];
        }

        (List<(ulong Mask, double Selectivity)> groups, List<EquivalenceClass> classes) =
            BuildSelectivityGroups(graph, bound, database, stats);
        SelectivityModel selectivity = new(groups, classes);

        return new CostInputs
        {
            FirstLeafCost = firstLeafCost,
            RightScanCost = rightScanCost,
            LeafRows = leafRows,
            SubsetRows = denseSubsets ? EstimateAllSubsets(n, leafRows, selectivity) : null,
            Selectivity = selectivity,
            StepEdges = BuildStepEdges(graph, bound),
        };
    }

    /// <summary>
    /// Runs the dynamic program and returns the leaf order of the cheapest plan, or null when no
    /// order avoids a cross product. A subset that breaks an order boundary of the graph
    /// (<see cref="JoinGraph.RespectsBoundaries"/>) is never a plan, so no order crosses one.
    /// Internal for tests.
    /// </summary>
    internal static int[]? FindCheapestOrder(
        JoinGraph graph,
        BoundSelectQuery bound,
        JoinPredicatePushdown.Result pushdown,
        DatabaseDescriptor database,
        StatisticsManager stats,
        CamusDBOptions options) =>
        FindCheapestOrder(graph, ComputeCostInputs(graph, bound, pushdown, database, stats, options), out _);

    /// <summary>
    /// The dynamic program over left-deep orders. <paramref name="costedSteps"/> is the number of
    /// join steps it costed, one for each (left subset, right leaf) pair that passes the boundary
    /// and connectivity checks. The count depends only on the graph, so it repeats between runs of
    /// the same query.
    /// </summary>
    private static int[]? FindCheapestOrder(JoinGraph graph, CostInputs inputs, out long costedSteps)
    {
        costedSteps = 0;
        int n = graph.LeafCount;
        int size = 1 << n;
        double[] cost = new double[size];
        int[] backLeft = new int[size];
        int[] backLeaf = new int[size];

        Array.Fill(cost, double.PositiveInfinity);

        for (int i = 0; i < n; i++)
        {
            if (!graph.RespectsBoundaries(1UL << i))
                continue;

            cost[1 << i] = inputs.FirstLeafCost[i];
            backLeaf[1 << i] = i;
        }

        for (int k = 2; k <= n; k++)
        {
            // Gosper's hack walks every mask with k bits in ascending order.
            for (int mask = (1 << k) - 1; mask < size; mask = NextWithSameBitCount(mask))
            {
                if (!graph.RespectsBoundaries((ulong)mask))
                    continue;

                // From the last declared table to the first: the declared order is the first
                // candidate of each prefix set, and the strict comparison keeps it on a tie.
                for (int bit = n - 1; bit >= 0; bit--)
                {
                    int leafBit = 1 << bit;
                    if ((mask & leafBit) == 0)
                        continue;

                    int left = mask ^ leafBit;
                    if (double.IsPositiveInfinity(cost[left]))
                        continue;

                    if (!graph.Connects((ulong)left, bit))
                        continue;

                    (bool equiKey, bool indexLookup) = StepOperators(inputs.StepEdges[bit], (ulong)mask);
                    costedSteps++;

                    double total = cost[left] + CostEstimator.JoinStepCost(
                        inputs.SubsetRows![left], inputs.LeafRows[bit], inputs.RightScanCost[bit], equiKey, indexLookup);

                    if (total < cost[mask])
                    {
                        cost[mask] = total;
                        backLeft[mask] = left;
                        backLeaf[mask] = bit;
                    }
                }
            }
        }

        int full = size - 1;
        if (double.IsPositiveInfinity(cost[full]))
            return null;

        int[] order = new int[n];
        int current = full;

        for (int position = n - 1; position >= 1; position--)
        {
            order[position] = backLeaf[current];
            current = backLeft[current];
        }

        order[0] = backLeaf[current];
        return order;
    }

    /// <summary>The next integer with the same number of set bits (Gosper's hack).</summary>
    private static int NextWithSameBitCount(int mask)
    {
        int lowest = mask & -mask;
        int ripple = mask + lowest;
        return (((ripple ^ mask) >> 2) / lowest) | ripple;
    }

    /// <summary>
    /// Which operators the builder can use when the table of <paramref name="rightEdges"/> joins
    /// the rest of <paramref name="union"/>: the conjuncts that this step attaches are the edges
    /// that name the table and lie inside the union. The checks are the builder's own
    /// (<see cref="JoinEquiJoinAnalyzer.TryExtractEquiKeys"/> and
    /// <see cref="JoinEquiJoinAnalyzer.TryMatch"/>), one conjunct at a time, which is what they do
    /// over a combined <c>ON</c>.
    /// </summary>
    private static (bool EquiKey, bool IndexLookup) StepOperators(List<StepEdge> rightEdges, ulong union)
    {
        bool equiKey = false;
        bool indexLookup = false;

        foreach (StepEdge edge in rightEdges)
        {
            if ((edge.Mask & ~union) != 0)
                continue;

            equiKey |= edge.EquiKey;
            indexLookup |= edge.IndexLookup;
        }

        return (equiKey, indexLookup);
    }

    private static List<StepEdge>[] BuildStepEdges(JoinGraph graph, BoundSelectQuery bound)
    {
        int n = graph.LeafCount;
        List<StepEdge>[] edges = new List<StepEdge>[n];
        BoundJoinRightSource[] rights = new BoundJoinRightSource[n];

        for (int i = 0; i < n; i++)
        {
            edges[i] = [];
            rights[i] = BoundSourceCatalog.FindJoinRightSource(bound, graph.Leaves[i].Node);
        }

        foreach (JoinGraph.Conjunct conjunct in graph.Conjuncts)
        {
            if (!conjunct.IsEdge)
                continue;

            bool isEquality = conjunct.Node.nodeType == NodeType.ExprEquals;
            ulong mask = conjunct.Mask;

            while (mask != 0)
            {
                int leaf = BitOperations.TrailingZeroCount(mask);
                mask &= mask - 1;

                bool equiKey = isEquality
                    && JoinEquiJoinAnalyzer.TryExtractEquiKeys(rights[leaf], conjunct.Node, bound, out _);

                bool indexLookup = isEquality
                    && rights[leaf].Table is { } table
                    && JoinEquiJoinAnalyzer.TryMatch(table, conjunct.Node, bound, out _);

                edges[leaf].Add(new StepEdge(conjunct.Mask, equiKey, indexLookup));
            }
        }

        return edges;
    }

    // ─── Row estimate of a subset ────────────────────────────────────────────

    /// <summary>
    /// A set of equal columns: the columns that the equi-conjuncts of the pool join, closed over
    /// shared columns (<c>a.x = b.x</c> and <c>b.x = c.x</c> put <c>a.x</c>, <c>b.x</c> and
    /// <c>c.x</c> in one class). <see cref="Leaves"/> has one entry for each leaf with a column in
    /// the class, in leaf order, with that column's distinct count (null when unknown).
    /// </summary>
    private sealed record EquivalenceClass(IReadOnlyList<(int Leaf, long? DistinctCount)> Leaves);

    /// <summary>
    /// Returns one row estimate for each subset of the leaves, indexed by leaf mask. The estimate
    /// of a subset depends on the subset only, never on a join tree:
    /// <c>rows[S] = Π leafRows × Π selectivity(g) × Π classFactor(C, S)</c>, over each edge group
    /// <c>g</c> that lies inside <c>S</c> and each column class <c>C</c>.
    ///
    /// <list type="bullet">
    ///   <item>A <b>column class</b> with columns of three or more tables, or of three or more
    ///   columns, counts once for a subset, however many of its equalities the query writes. Over
    ///   the <c>k</c> tables of the subset that have a column in it, it gives <c>k − 1</c> factors:
    ///   <c>1 / d</c> for each table except the one with the smallest distinct count <c>d</c> (the
    ///   join of <c>k</c> tables on one value, when the smaller value sets are contained in the
    ///   larger). Without classes, a query that names one movie through six columns and writes all
    ///   fifteen equalities multiplies fifteen selectivities where five describe the result, and
    ///   every join of the plan looks almost empty.</item>
    ///   <item>The equi-conjuncts <c>x.c = y.d</c> whose class has those two columns only form one
    ///   <b>pair group</b> for each pair of tables, with one selectivity
    ///   (<see cref="CardinalityEstimator.EstimateEquiJoinSelectivity"/>), so a composite key uses
    ///   its composite distinct-value count.</item>
    ///   <item>Every other edge (non-equi, an expression, three or more tables) is its own group with
    ///   <see cref="CardinalityEstimator.FallbackSelectivity"/>. Single-table conjuncts and constants
    ///   are not counted.</item>
    /// </list>
    ///
    /// <para>The distinct count of a column is its table row count when it covers a unique index,
    /// else its NDV. When no column of a class has a count, each factor is
    /// <see cref="CardinalityEstimator.FallbackSelectivity"/>; when only some have one, the others
    /// take the smallest known count.</para>
    ///
    /// <para>The values are doubles with no floor and no clamp, so a long chain cannot overflow
    /// a <see cref="long"/>; a caller that needs a row count floors and clamps it. Internal for
    /// tests.</para>
    /// </summary>
    internal static double[] EstimateSubsetRows(
        JoinGraph graph,
        IReadOnlyList<double> leafRows,
        BoundSelectQuery bound,
        DatabaseDescriptor database,
        StatisticsManager? stats)
    {
        (List<(ulong Mask, double Selectivity)> groups, List<EquivalenceClass> classes) =
            BuildSelectivityGroups(graph, bound, database, stats);

        return EstimateAllSubsets(graph.LeafCount, leafRows, new SelectivityModel(groups, classes));
    }

    /// <summary>The estimate of every non-empty subset of <paramref name="n"/> leaves; index 0 is unused.</summary>
    private static double[] EstimateAllSubsets(int n, IReadOnlyList<double> leafRows, SelectivityModel selectivity)
    {
        double[] rows = new double[1 << n];
        List<long?> counts = [];

        for (int mask = 1; mask < rows.Length; mask++)
            rows[mask] = EstimateMaskRows(mask, leafRows, selectivity, counts);

        return rows;
    }

    /// <summary>
    /// The row estimate of one subset (see <see cref="EstimateSubsetRows"/>): the product of its
    /// leaf rows, then the selectivity of each group inside it, then the factor of each column
    /// class over its leaves. It reads no other subset, so any subset can be computed alone. The
    /// order of the products is fixed; the dense array and a single-mask call give the same double.
    /// <paramref name="counts"/> is scratch space, reused across calls by a caller that loops.
    /// </summary>
    private static double EstimateMaskRows(int mask, IReadOnlyList<double> leafRows, SelectivityModel selectivity, List<long?> counts)
    {
        double estimate = 1.0;

        for (int leaf = 0; leaf < leafRows.Count; leaf++)
        {
            if ((mask & (1 << leaf)) != 0)
                estimate *= leafRows[leaf];
        }

        foreach ((ulong groupMask, double groupSelectivity) in selectivity.Groups)
        {
            if ((groupMask & ~(ulong)mask) == 0)
                estimate *= groupSelectivity;
        }

        foreach (EquivalenceClass equivalence in selectivity.Classes)
        {
            counts.Clear();
            foreach ((int leaf, long? distinct) in equivalence.Leaves)
            {
                if ((mask & (1 << leaf)) != 0)
                    counts.Add(distinct);
            }

            estimate *= ClassFactor(counts);
        }

        return estimate;
    }

    /// <summary>
    /// The selectivity of one column class over the tables of a subset, given their distinct
    /// counts: <c>1 / d</c> for each table except the one with the smallest count. See
    /// <see cref="EstimateSubsetRows"/>.
    /// </summary>
    private static double ClassFactor(List<long?> counts)
    {
        if (counts.Count < 2)
            return 1.0;

        long? smallestKnown = null;
        foreach (long? count in counts)
        {
            if (count is { } c && c > 0 && (smallestKnown is null || c < smallestKnown))
                smallestKnown = c;
        }

        if (smallestKnown is null)
            return Math.Pow(CardinalityEstimator.FallbackSelectivity, counts.Count - 1);

        double factor = 1.0;
        bool skippedSmallest = false;

        foreach (long? count in counts)
        {
            long d = count is { } c && c > 0 ? c : smallestKnown.Value;

            if (!skippedSmallest && d == smallestKnown.Value)
            {
                skippedSmallest = true;
                continue;
            }

            factor /= d;
        }

        return factor;
    }

    private static (List<(ulong Mask, double Selectivity)> Groups, List<EquivalenceClass> Classes) BuildSelectivityGroups(
        JoinGraph graph,
        BoundSelectQuery bound,
        DatabaseDescriptor database,
        StatisticsManager? stats)
    {
        List<(ulong Mask, double Selectivity)> groups = [];

        // Union-find over the columns of the equi-conjuncts, keyed by (leaf, column).
        Dictionary<(int Leaf, string Column), int> nodeOf = [];
        List<(int Leaf, string Column)> nodes = [];
        List<int> parent = [];

        int Node(JoinGraph.EquiSide side)
        {
            (int, string) key = (side.Leaf, side.Column.ToLowerInvariant());
            if (!nodeOf.TryGetValue(key, out int id))
            {
                id = nodes.Count;
                nodeOf[key] = id;
                nodes.Add((side.Leaf, side.Column));
                parent.Add(id);
            }
            return id;
        }

        int Find(int x)
        {
            while (parent[x] != x)
            {
                parent[x] = parent[parent[x]];
                x = parent[x];
            }
            return x;
        }

        List<(JoinGraph.EquiSide A, JoinGraph.EquiSide B)> equalities = [];

        foreach (JoinGraph.Conjunct conjunct in graph.Conjuncts)
        {
            if (!conjunct.IsEdge)
                continue;

            if (conjunct.Left is not { } a || conjunct.Right is not { } b)
            {
                groups.Add((conjunct.Mask, CardinalityEstimator.FallbackSelectivity));
                continue;
            }

            equalities.Add((a, b));
            int ra = Find(Node(a));
            int rb = Find(Node(b));
            if (ra != rb)
                parent[Math.Max(ra, rb)] = Math.Min(ra, rb);
        }

        Dictionary<int, List<int>> membersOf = [];
        for (int i = 0; i < nodes.Count; i++)
        {
            int root = Find(i);
            if (!membersOf.TryGetValue(root, out List<int>? members))
                membersOf[root] = members = [];
            members.Add(i);
        }

        // Two-column classes: equi-conjuncts grouped by their unordered pair of leaves, in pool order.
        List<(int Low, int High, List<string> LowColumns, List<string> HighColumns)> pairs = [];

        foreach ((JoinGraph.EquiSide a, JoinGraph.EquiSide b) in equalities)
        {
            if (membersOf[Find(nodeOf[(a.Leaf, a.Column.ToLowerInvariant())])].Count > 2)
                continue;

            (JoinGraph.EquiSide low, JoinGraph.EquiSide high) = a.Leaf < b.Leaf ? (a, b) : (b, a);

            int index = pairs.FindIndex(p => p.Low == low.Leaf && p.High == high.Leaf);
            if (index < 0)
            {
                pairs.Add((low.Leaf, high.Leaf, [], []));
                index = pairs.Count - 1;
            }

            (_, _, List<string> lowColumns, List<string> highColumns) = pairs[index];

            bool duplicate = false;
            for (int i = 0; i < lowColumns.Count; i++)
            {
                if (string.Equals(lowColumns[i], low.Column, StringComparison.OrdinalIgnoreCase)
                    && string.Equals(highColumns[i], high.Column, StringComparison.OrdinalIgnoreCase))
                {
                    duplicate = true;
                    break;
                }
            }

            if (!duplicate)
            {
                lowColumns.Add(low.Column);
                highColumns.Add(high.Column);
            }
        }

        foreach ((int low, int high, List<string> lowColumns, List<string> highColumns) in pairs)
        {
            TableDescriptor? lowTable = FindTableDescriptor(graph.Leaves[low].Alias, bound);
            TableDescriptor? highTable = FindTableDescriptor(graph.Leaves[high].Alias, bound);

            double selectivity = CardinalityEstimator.EstimateEquiJoinSelectivity(
                lowTable, lowColumns, TableRows(lowTable, database, stats),
                highTable, highColumns, TableRows(highTable, database, stats),
                database, stats);

            groups.Add(((1UL << low) | (1UL << high), selectivity));
        }

        // Larger classes: one entry for each leaf with a column in the class (its first column in
        // pool order), in leaf order, so the factor of a subset has one fixed evaluation order.
        List<EquivalenceClass> classes = [];

        foreach (List<int> members in membersOf.Values.OrderBy(m => m[0]))
        {
            if (members.Count <= 2)
                continue;

            SortedDictionary<int, long?> byLeaf = [];
            foreach (int member in members)
            {
                (int leaf, string column) = nodes[member];
                if (byLeaf.ContainsKey(leaf))
                    continue;

                TableDescriptor? table = FindTableDescriptor(graph.Leaves[leaf].Alias, bound);
                byLeaf[leaf] = CardinalityEstimator.EstimateKeyDistinctCount(
                    table, [column], TableRows(table, database, stats), database, stats);
            }

            classes.Add(new EquivalenceClass(byLeaf.Select(kv => (kv.Key, kv.Value)).ToList()));
        }

        return (groups, classes);
    }

    private static long TableRows(TableDescriptor? table, DatabaseDescriptor database, StatisticsManager? stats) =>
        (table is not null ? stats?.GetRowCountEstimate(database, table) : null) ?? CostEstimator.DefaultTableRowCount;

    // ─── Leaf cost ───────────────────────────────────────────────────────────

    /// <summary>
    /// Cost and output rows of the scan that the builder makes for one join input, with its
    /// pushed-down filter. <paramref name="cost"/> is what a step pays to read the input;
    /// <paramref name="card"/> is the number of rows the scan returns.
    /// </summary>
    private static void EstimateLeaf(
        string alias,
        JoinPredicatePushdown.Result pushdown,
        BoundSelectQuery bound,
        DatabaseDescriptor database,
        StatisticsManager stats,
        out double cost,
        out long card,
        CamusDBOptions options,
        Dictionary<string, ColumnValue>? clockValues)
    {
        TableDescriptor? table = FindTableDescriptor(alias, bound);

        // A function in FROM has no statistics, but with constant arguments its count is exact.
        long tableRows = (table is not null
            ? stats.GetRowCountEstimate(database, table)
            : FindFunctionRows(alias, bound)) ?? CostEstimator.DefaultTableRowCount;

        pushdown.ScanFiltersByAlias.TryGetValue(alias, out NodeAst? filter);

        if (filter is null || table is null)
        {
            cost = tableRows;
            card = tableRows;
            return;
        }

        // When CBO access-path selection is enabled, BuildJoinTree may emit an IndexRangeScanNode
        // instead of a TableScanNode. Mirror that decision here so the DP costs what is actually built.
        if (options.CostBasedAccessPathEnabled)
        {
            // Clock values only, the same input the builder's leaf analysis gets: a placeholder stays
            // unresolved here as it does there.
            PredicateAnalysis raw = PredicateAnalyzer.Analyze(filter, clockValues);
            PredicateAnalysis analysis = JoinEnumerator.StripAliasPrefix(raw, alias);
            // Cost the leaf over the same coerced constants the builder will select with, so the
            // DP never costs an index shape the builder then refuses.
            analysis = PredicateAnalyzer.CoerceConstantsForColumns(analysis, table);

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
                    if (!CostEstimator.ShouldPreferFullScan(candidate, tableRows, options, stats, database, table))
                    {
                        long rangeRows = CostEstimator.EstimateRangeScanRows(candidate, tableRows, options, stats, database, table);
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

    private static long? FindFunctionRows(string alias, BoundSelectQuery bound)
    {
        foreach (BoundDerivedTableSource src in bound.DerivedSources)
            if (src.Alias == alias)
                return TableFunctionEvaluation.TryCountRows(src);

        return null;
    }

    private static TableDescriptor? FindTableDescriptor(string alias, BoundSelectQuery bound)
    {
        foreach (BoundTableSource src in bound.Sources)
            if (src.Alias == alias)
                return src.Table;

        return null;
    }
}
