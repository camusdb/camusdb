/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Numerics;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// The query graph of an inner-join tree: its leaves, its pool of predicate conjuncts, the
/// connectivity test, and the one rule that rebuilds a left-deep tree for a leaf order.
///
/// <para>The planner reads the predicates of a join as conjuncts, not as whole <c>ON</c> clauses.
/// The shape of the query text does not decide which tables connect: <c>ON TRUE</c> connects
/// nothing, and an <c>ON</c> with two conjuncts that name three tables is two edges, not one
/// three-table edge. A comma join therefore gets the same graph as the explicit <c>JOIN</c> form
/// of the same query. The graph also takes the cross-table conjuncts of <c>WHERE</c> that are
/// safe to evaluate at a join (see <see cref="TryBuild"/>), so a predicate written in
/// <c>WHERE</c> connects tables too.</para>
///
/// <para>The attach rule (<see cref="BuildTree"/>): a conjunct goes to the join where the last
/// of its tables enters the tree. A conjunct with no table reference, and a conjunct on the first
/// table only, go to the first join. Each conjunct is attached exactly once, so each one is
/// evaluated exactly once. For inner joins this is equivalent to the original tree, because an
/// inner join <c>ON</c> is a filter over the joined rows: any join at or above the point where all
/// of its tables are present gives the same rows. A conjunct can therefore run at an earlier join
/// than the query text shows, on rows that a later join would remove. A conjunct that can raise a
/// runtime error (a division, a cast) can raise it on such a row, which scan pushdown already
/// does for single-table conjuncts.</para>
///
/// <para><b>Pinned conjuncts.</b> An <c>ON</c> conjunct that fails
/// <see cref="FragmentFilterShippability.IsShippable"/> does not move on its own, and it does not
/// connect tables.</para>
/// <list type="bullet">
///   <item>A <b>volatile</b> conjunct (<c>random()</c>, <c>now()</c>, …) runs once for each row
///   that reaches its join, so moving it changes how many times it runs: a constant
///   <c>random() &lt; 0.5</c> moved to the first join would decide once for many rows. It keeps the
///   <b>scope</b> of the join it was written on (that join and every table before it) and attaches
///   where the last table of the scope enters. It also sets an <b>order boundary</b>
///   (<see cref="RespectsBoundaries"/>): the tables of its scope join before any other table, so
///   each combination of their rows reaches it exactly once, as in the query text.</item>
///   <item>Any other unshippable conjunct (a parameter) cannot run on a peer node, so the join that
///   holds it cannot be a broadcast join. It stays with the <c>ON</c> clause it was written in: it
///   attaches where the last table of that clause enters, which is where the whole-predicate path
///   puts the clause. For any order it therefore blocks the same join as before, and no
///   other.</item>
/// </list>
///
/// <para>The graph refuses (returns null from <see cref="TryBuild"/>) a tree that it cannot
/// rebuild without a risk to the results: a join that is not inner, a source that is not a table
/// or a derived table, an <c>ON</c> conjunct that contains a subquery, and an <c>ON</c> conjunct
/// with an identifier that does not resolve to exactly one leaf. The caller then keeps the
/// whole-predicate path (<see cref="JoinOrderOptimizer"/>). <see cref="JoinEnumerator"/>,
/// the plan-cache replay in <see cref="JoinQueryPlanner"/> and the cross-product repair in
/// <see cref="JoinOrderOptimizer"/> all use this type, so the attach rule exists once.</para>
/// </summary>
internal sealed class JoinGraph
{
    /// <summary>One bit for each leaf in a <see cref="ulong"/> mask.</summary>
    internal const int MaxLeaves = 64;

    /// <summary>A join input in declared order: one table or one derived table.</summary>
    internal sealed record Leaf(QuerySource Node, string Alias);

    /// <summary>One side of an equi-conjunct <c>x.c = y.d</c>: the leaf and the bare column name.</summary>
    internal readonly record struct EquiSide(int Leaf, string Column);

    /// <summary>
    /// One conjunct of the pool. <see cref="Mask"/> has one bit for each leaf that the conjunct
    /// names. <see cref="AttachMask"/> is the set of leaves that must be in the tree before the
    /// conjunct runs: <see cref="Mask"/> itself, or, when it is <see cref="Pinned"/>, the tables of
    /// its original <c>ON</c> clause or of its original scope (see the class summary). <see cref="Left"/> and <see cref="Right"/> are set only for a
    /// column-to-column equality between two different leaves, the form a row estimate can use.
    /// </summary>
    internal sealed record Conjunct(
        NodeAst Node,
        ulong Mask,
        ulong AttachMask,
        bool Pinned,
        bool FromWhere,
        EquiSide? Left,
        EquiSide? Right)
    {
        public int AliasCount => BitOperations.PopCount(Mask);

        /// <summary>
        /// True for a conjunct that names two or more leaves and can run where they meet: only an
        /// edge connects tables. A pinned conjunct is not an edge, because it does not run at the
        /// join where its own tables meet.
        /// </summary>
        public bool IsEdge => AliasCount >= 2 && !Pinned;
    }

    private readonly BoundSelectQuery bound;

    private readonly List<Leaf> leaves;

    private readonly Dictionary<string, int> leafByAlias;

    private readonly List<Conjunct> conjuncts = [];

    /// <summary>The post-join conjuncts in their original order, and which of them the graph took.</summary>
    private readonly List<(NodeAst Node, bool Taken)> postJoinConjuncts = [];

    /// <summary>The masks of the conjuncts that are edges, for the connectivity test.</summary>
    private ulong[] edgeMasks = [];

    /// <summary>The scopes of the volatile <c>ON</c> conjuncts; see <see cref="RespectsBoundaries"/>.</summary>
    private readonly List<ulong> boundaries = [];

    public IReadOnlyList<Leaf> Leaves => leaves;

    public IReadOnlyList<Conjunct> Conjuncts => conjuncts;

    public int LeafCount => leaves.Count;

    /// <summary>
    /// The filter that stays after the last join when a tree from <see cref="BuildTree"/> is used:
    /// the post-join conjuncts that the graph did not take, in their original order. Null when
    /// the graph took all of them.
    /// </summary>
    public NodeAst? RemainingPostJoinFilter
    {
        get
        {
            List<NodeAst> remaining = [];

            foreach ((NodeAst node, bool taken) in postJoinConjuncts)
            {
                if (!taken)
                    remaining.Add(node);
            }

            return PredicateAnalyzer.CombineConjuncts(remaining);
        }
    }

    private JoinGraph(BoundSelectQuery bound, List<Leaf> leaves)
    {
        this.bound = bound;
        this.leaves = leaves;

        // Aliases are unique without regard to case (the binder refuses a duplicate), and an
        // identifier can spell its qualifier in any case, as everywhere else in binding.
        leafByAlias = new(leaves.Count, StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < leaves.Count; i++)
            leafByAlias[leaves[i].Alias] = i;
    }

    /// <summary>
    /// Builds the graph of <paramref name="root"/>, or returns null when the tree must keep the
    /// whole-predicate path (see the class summary).
    ///
    /// <para><paramref name="postJoinConjuncts"/> are the <c>WHERE</c> conjuncts that
    /// <see cref="JoinPredicatePushdown"/> keeps after the last join. The graph takes one only
    /// when it names two or more leaves, contains no subquery, has only identifiers that resolve
    /// to one leaf each, and passes <see cref="FragmentFilterShippability.IsShippable"/>. The last
    /// rule keeps a join that the broadcast planner can ship from becoming one it cannot ship. A
    /// conjunct that the graph does not take stays in <see cref="RemainingPostJoinFilter"/>.</para>
    /// </summary>
    public static JoinGraph? TryBuild(
        QuerySource root,
        BoundSelectQuery bound,
        IReadOnlyList<NodeAst> postJoinConjuncts)
    {
        List<Leaf> leaves = [];
        List<(NodeAst Node, int ScopeSize)> onConjuncts = [];

        if (!CollectLeaves(root, leaves, onConjuncts))
            return null;

        if (leaves.Count > MaxLeaves)
            return null;

        JoinGraph graph = new(bound, leaves);

        if (graph.leafByAlias.Count != leaves.Count)
            return null;

        ulong[] masks = new ulong[onConjuncts.Count];

        for (int i = 0; i < onConjuncts.Count; i++)
        {
            if (!graph.TryResolve(onConjuncts[i].Node, out masks[i], out bool hasSubquery) || hasSubquery)
                return null;
        }

        // The tables of each ON clause, keyed by its join (the scope size identifies the join in
        // a left-deep tree), plus the table that join adds: where the whole-predicate path puts it.
        Dictionary<int, ulong> clauseMask = [];
        for (int i = 0; i < onConjuncts.Count; i++)
        {
            int scopeSize = onConjuncts[i].ScopeSize;
            clauseMask[scopeSize] = clauseMask.GetValueOrDefault(scopeSize, 1UL << (scopeSize - 1)) | masks[i];
        }

        for (int i = 0; i < onConjuncts.Count; i++)
        {
            (NodeAst node, int scopeSize) = onConjuncts[i];
            ulong mask = masks[i];

            if (FragmentFilterShippability.IsShippable(node))
            {
                graph.conjuncts.Add(graph.Describe(node, mask, mask, pinned: false, fromWhere: false));
                continue;
            }

            if (!ScalarFunctionEvaluator.ContainsVolatileFunction(node))
            {
                graph.conjuncts.Add(graph.Describe(node, mask, mask | clauseMask[scopeSize], pinned: true, fromWhere: false));
                continue;
            }

            // The leaves are in declared order, so the scope of the join this conjunct was written
            // on is the first scopeSize leaves.
            ulong scope = scopeSize >= MaxLeaves ? ulong.MaxValue : (1UL << scopeSize) - 1;
            graph.conjuncts.Add(graph.Describe(node, mask, mask | scope, pinned: true, fromWhere: false));

            if (!graph.boundaries.Contains(scope))
                graph.boundaries.Add(scope);
        }

        foreach (NodeAst node in postJoinConjuncts)
        {
            bool take = graph.TryResolve(node, out ulong mask, out bool hasSubquery)
                && !hasSubquery
                && BitOperations.PopCount(mask) >= 2
                && FragmentFilterShippability.IsShippable(node);

            graph.postJoinConjuncts.Add((node, take));

            if (take)
                graph.conjuncts.Add(graph.Describe(node, mask, mask, pinned: false, fromWhere: true));
        }

        List<ulong> edges = [];
        foreach (Conjunct conjunct in graph.conjuncts)
        {
            if (conjunct.IsEdge)
                edges.Add(conjunct.Mask);
        }

        graph.edgeMasks = edges.ToArray();
        return graph;
    }

    /// <summary>
    /// True when <paramref name="leaf"/> can join <paramref name="leftMask"/> without a cross
    /// product: some edge names the leaf, has all of its tables inside the union, and so names at
    /// least one table of the left set. A single-table conjunct does not connect.
    /// </summary>
    public bool Connects(ulong leftMask, int leaf)
    {
        ulong leafBit = 1UL << leaf;
        ulong union = leftMask | leafBit;

        foreach (ulong edge in edgeMasks)
        {
            if ((edge & leafBit) != 0 && (edge & ~union) == 0 && (edge & leftMask) != 0)
                return true;
        }

        return false;
    }

    /// <summary>
    /// True when the leaves of <paramref name="prefix"/> can be the first leaves of an order: for
    /// each order boundary, the prefix lies inside it or contains it. A boundary is the scope of a
    /// volatile <c>ON</c> conjunct; when every prefix respects it, the tables of the scope join
    /// before any other table, and each combination of their rows reaches the conjunct exactly
    /// once.
    /// </summary>
    public bool RespectsBoundaries(ulong prefix)
    {
        foreach (ulong boundary in boundaries)
        {
            if ((prefix & ~boundary) != 0 && (boundary & ~prefix) != 0)
                return false;
        }

        return true;
    }

    /// <summary>
    /// True when each leaf of <paramref name="order"/> after the first connects to the leaves
    /// before it, and each prefix respects the order boundaries.
    /// </summary>
    public bool IsConnectedOrder(IReadOnlyList<int> order)
    {
        if (order.Count == 0)
            return false;

        ulong inTree = 1UL << order[0];

        if (!RespectsBoundaries(inTree))
            return false;

        for (int i = 1; i < order.Count; i++)
        {
            if (!Connects(inTree, order[i]))
                return false;

            inTree |= 1UL << order[i];

            if (!RespectsBoundaries(inTree))
                return false;
        }

        return true;
    }

    /// <summary>
    /// A leaf order with no cross product, or null when the predicates need one. The order starts
    /// at the first leaf of <paramref name="preference"/> from which all leaves can be reached,
    /// and each next step takes the first remaining leaf, in preference order, that connects to
    /// the leaves already in the tree.
    ///
    /// <para>A leaf that can connect stays able to connect as the tree grows, so the result does
    /// not depend on which connecting leaf a step takes; only the start matters. An edge with
    /// three tables can make some starts fail, so each start is tried in turn. A step also takes
    /// only a leaf that keeps the order boundaries; the boundaries are nested scopes of the declared
    /// order, so they only fix which group of leaves comes next.</para>
    /// </summary>
    public int[]? FindConnectedOrder(IReadOnlyList<int> preference)
    {
        int n = leaves.Count;

        foreach (int start in preference)
        {
            if (!RespectsBoundaries(1UL << start))
                continue;

            int[] order = new int[n];
            bool[] used = new bool[n];
            order[0] = start;
            used[start] = true;
            ulong inTree = 1UL << start;
            int count = 1;

            while (count < n)
            {
                int next = -1;

                foreach (int candidate in preference)
                {
                    if (!used[candidate]
                        && Connects(inTree, candidate)
                        && RespectsBoundaries(inTree | (1UL << candidate)))
                    {
                        next = candidate;
                        break;
                    }
                }

                if (next < 0)
                    break;

                order[count++] = next;
                used[next] = true;
                inTree |= 1UL << next;
            }

            if (count == n)
                return order;
        }

        return null;
    }

    /// <summary>The leaf index of <paramref name="alias"/>, or <c>-1</c> when no leaf has it.</summary>
    public int LeafOf(string alias) => leafByAlias.TryGetValue(alias, out int leaf) ? leaf : -1;

    /// <summary>
    /// Maps an alias order (for example one from the plan cache) to leaf indices. Returns null
    /// when it is not a permutation of the leaves of this graph.
    /// </summary>
    public int[]? MapAliases(IReadOnlyList<string> aliases)
    {
        if (aliases.Count != leaves.Count)
            return null;

        int[] order = new int[aliases.Count];
        bool[] seen = new bool[leaves.Count];

        for (int i = 0; i < aliases.Count; i++)
        {
            if (!leafByAlias.TryGetValue(aliases[i], out int leaf) || seen[leaf])
                return null;

            seen[leaf] = true;
            order[i] = leaf;
        }

        return order;
    }

    /// <summary>
    /// True when each join of <paramref name="tree"/>, as built, has an <c>ON</c> conjunct that
    /// names a leaf on each of its two sides. A false result means the tree contains a cross
    /// product. Used to decide whether a whole-predicate tree needs the cross-product repair; the
    /// <c>WHERE</c> conjuncts of the graph are not in such a tree and do not count.
    /// </summary>
    public bool EveryJoinConnects(QuerySource tree)
    {
        Dictionary<NodeAst, ulong> maskByNode = new(ReferenceEqualityComparer.Instance);
        foreach (Conjunct conjunct in conjuncts)
        {
            if (!conjunct.FromWhere)
                maskByNode[conjunct.Node] = conjunct.Mask;
        }

        // The planner keeps every right input a leaf, so the walk follows the left spine.
        QuerySource current = tree;
        List<NodeAst> onConjuncts = [];

        while (current is JoinSource join)
        {
            if (!TryLeafMask(join.Right, out ulong rightMask) || !TryLeafMaskOfTree(join.Left, out ulong leftMask))
                return false;

            onConjuncts.Clear();
            PredicateAnalyzer.CollectAndConjuncts(join.OnPredicate, onConjuncts);

            bool connects = false;
            foreach (NodeAst node in onConjuncts)
            {
                if (maskByNode.TryGetValue(node, out ulong mask) && (mask & leftMask) != 0 && (mask & rightMask) != 0)
                {
                    connects = true;
                    break;
                }
            }

            if (!connects)
                return false;

            current = join.Left;
        }

        return true;
    }

    /// <summary>
    /// Builds the left-deep inner-join tree for <paramref name="order"/> with the attach rule of
    /// the class summary, over each conjunct's <see cref="Conjunct.AttachMask"/>. The <c>ON</c>
    /// of a join is the conjunction of its conjuncts in pool order, or the literal true when it
    /// has none. The pool order is the <c>ON</c> conjuncts in tree order, then the taken
    /// <c>WHERE</c> conjuncts, so the output is deterministic.
    ///
    /// <para>Returns null when <paramref name="order"/> is not a permutation of the leaves, or
    /// when the count of attached conjuncts is not the pool size. The caller then keeps the
    /// original tree: a conjunct that is evaluated zero times or two times changes the rows.</para>
    /// </summary>
    public QuerySource? BuildTree(IReadOnlyList<int> order)
    {
        int n = leaves.Count;

        if (order.Count != n || n < 2)
            return null;

        int[] position = new int[n];
        Array.Fill(position, -1);

        for (int i = 0; i < n; i++)
        {
            if (order[i] < 0 || order[i] >= n || position[order[i]] >= 0)
                return null;

            position[order[i]] = i;
        }

        // Join k (1..n-1) is where order[k] enters the tree.
        List<NodeAst>[] attached = new List<NodeAst>[n];
        int attachedCount = 0;

        foreach (Conjunct conjunct in conjuncts)
        {
            int step = 1;
            ulong mask = conjunct.AttachMask;

            while (mask != 0)
            {
                int leaf = BitOperations.TrailingZeroCount(mask);
                mask &= mask - 1;
                step = Math.Max(step, position[leaf]);
            }

            (attached[step] ??= []).Add(conjunct.Node);
            attachedCount++;
        }

        if (attachedCount != conjuncts.Count)
            return null;

        QuerySource tree = leaves[order[0]].Node;

        for (int step = 1; step < n; step++)
        {
            NodeAst onPredicate = attached[step] is { } list
                ? PredicateAnalyzer.CombineConjuncts(list)!
                : CommaJoinNormalizer.CreateCrossJoinOnPredicate();

            tree = new JoinSource(tree, leaves[order[step]].Node, JoinKind.Inner, onPredicate);
        }

        return tree;
    }

    /// <summary>The leaf indices of a tree built over this graph, left to right.</summary>
    public int[]? OrderOf(QuerySource tree)
    {
        List<int> order = [];
        QuerySource current = tree;
        Stack<int> rights = new();

        while (current is JoinSource join)
        {
            if (!TryLeafIndex(join.Right, out int right))
                return null;

            rights.Push(right);
            current = join.Left;
        }

        if (!TryLeafIndex(current, out int first))
            return null;

        order.Add(first);
        while (rights.Count > 0)
            order.Add(rights.Pop());

        return order.Count == leaves.Count ? order.ToArray() : null;
    }

    // ─── Construction ────────────────────────────────────────────────────────

    /// <summary>
    /// Flattens an inner-join tree into its leaves (declared order) and its <c>ON</c> conjuncts
    /// (tree order, the literal true dropped). Returns false for any join kind that is not inner,
    /// for a right input that is not a leaf, and for any source type other than a table or a
    /// derived table. The planner keeps every right input a leaf, so the tree is left-deep and
    /// the walk follows the left spine without recursion.
    /// </summary>
    private static bool CollectLeaves(QuerySource root, List<Leaf> leaves, List<(NodeAst Node, int ScopeSize)> onConjuncts)
    {
        Stack<JoinSource> spine = new();
        QuerySource current = root;

        while (current is JoinSource join)
        {
            if (join.Kind != JoinKind.Inner)
                return false;

            spine.Push(join);
            current = join.Left;
        }

        if (!TryAddLeaf(current, leaves))
            return false;

        while (spine.Count > 0)
        {
            JoinSource join = spine.Pop();

            if (!TryAddLeaf(join.Right, leaves))
                return false;

            // The scope of this join is every leaf collected so far.
            AddOnConjuncts(join.OnPredicate, leaves.Count, onConjuncts);
        }

        return true;
    }

    private static bool TryAddLeaf(QuerySource source, List<Leaf> leaves)
    {
        switch (source)
        {
            case TableSource table:
                leaves.Add(new(table, table.Alias ?? table.TableName));
                return true;

            case DerivedTableSource derived:
                leaves.Add(new(derived, derived.Alias));
                return true;

            default:
                return false;
        }
    }

    private static void AddOnConjuncts(NodeAst onPredicate, int scopeSize, List<(NodeAst Node, int ScopeSize)> onConjuncts)
    {
        List<NodeAst> split = [];
        PredicateAnalyzer.CollectAndConjuncts(onPredicate, split);

        foreach (NodeAst conjunct in split)
        {
            // A literal true adds nothing to an inner join; CROSS JOIN and a comma join that has
            // no equi-join conjunct both arrive as ON TRUE.
            if (IsLiteralTrue(conjunct))
                continue;

            onConjuncts.Add((conjunct, scopeSize));
        }
    }

    private static bool IsLiteralTrue(NodeAst node) =>
        ReferenceEquals(node, NodeAst.True)
        || (node.nodeType == NodeType.Bool && string.Equals(node.yytext, "true", StringComparison.OrdinalIgnoreCase));

    private Conjunct Describe(NodeAst node, ulong mask, ulong attachMask, bool pinned, bool fromWhere)
    {
        EquiSide? left = null;
        EquiSide? right = null;

        if (node.nodeType == NodeType.ExprEquals
            && node.leftAst is { nodeType: NodeType.Identifier, yytext: { } leftText }
            && node.rightAst is { nodeType: NodeType.Identifier, yytext: { } rightText }
            && TryResolveIdentifier(leftText, out int leftLeaf)
            && TryResolveIdentifier(rightText, out int rightLeaf)
            && leftLeaf != rightLeaf)
        {
            left = new EquiSide(leftLeaf, BareColumn(leftText));
            right = new EquiSide(rightLeaf, BareColumn(rightText));
        }

        return new Conjunct(node, mask, attachMask, pinned, fromWhere, left, right);
    }

    /// <summary>
    /// Computes the leaf mask of a conjunct. Returns false when an identifier does not resolve to
    /// exactly one leaf. <paramref name="hasSubquery"/> is true when the conjunct contains a
    /// subquery, whose own identifiers are not walked. A function name and a cast target type are
    /// identifiers in the tree but not column references, so they are skipped. Iterative: an
    /// expression can be deep (a long CASE chain), and a stack overflow ends the process.
    /// </summary>
    private bool TryResolve(NodeAst conjunct, out ulong mask, out bool hasSubquery)
    {
        mask = 0;
        hasSubquery = false;
        bool resolved = true;

        Stack<NodeAst> pending = new();
        pending.Push(conjunct);

        while (pending.Count > 0)
        {
            NodeAst node = pending.Pop();

            switch (node.nodeType)
            {
                case NodeType.Identifier:
                    if (node.yytext is { } text && TryResolveIdentifier(text, out int leaf))
                        mask |= 1UL << leaf;
                    else
                        resolved = false;
                    continue;

                case NodeType.ExprScalarSubquery:
                case NodeType.ExprExistsSubquery:
                case NodeType.ExprExistsCorrelated:
                case NodeType.ExprInSubquery:
                case NodeType.ExprNotInSubquery:
                case NodeType.Select:
                    hasSubquery = true;
                    continue;

                case NodeType.ExprFuncCall:
                    // leftAst is the function name.
                    PushChildren(node, pending, skipLeft: true);
                    continue;

                case NodeType.ExprCast:
                    // rightAst is the target type, which can be an identifier.
                    if (node.leftAst is not null)
                        pending.Push(node.leftAst);
                    continue;

                default:
                    PushChildren(node, pending, skipLeft: false);
                    continue;
            }
        }

        return resolved;
    }

    private static void PushChildren(NodeAst node, Stack<NodeAst> pending, bool skipLeft)
    {
        if (!skipLeft && node.leftAst is not null) pending.Push(node.leftAst);
        if (node.rightAst is not null) pending.Push(node.rightAst);
        if (node.extendedOne is not null) pending.Push(node.extendedOne);
        if (node.extendedTwo is not null) pending.Push(node.extendedTwo);
        if (node.extendedThree is not null) pending.Push(node.extendedThree);
        if (node.extendedFour is not null) pending.Push(node.extendedFour);
        if (node.extendedFive is not null) pending.Push(node.extendedFive);
        if (node.extendedSix is not null) pending.Push(node.extendedSix);
        if (node.extendedSeven is not null) pending.Push(node.extendedSeven);
    }

    /// <summary>
    /// Resolves a column identifier to a leaf: by its qualifier when it has one, else by the one
    /// source that has a column of that name. An ambiguous or unknown name does not resolve.
    /// </summary>
    private bool TryResolveIdentifier(string identifier, out int leaf)
    {
        int dot = identifier.IndexOf('.');

        if (dot > 0 && dot < identifier.Length - 1)
            return leafByAlias.TryGetValue(identifier[..dot], out leaf);

        if (BoundSourceCatalog.TryResolveUnqualifiedOwner(bound, identifier, out string owner))
            return leafByAlias.TryGetValue(owner, out leaf);

        leaf = -1;
        return false;
    }

    private static string BareColumn(string identifier)
    {
        int dot = identifier.IndexOf('.');
        return dot >= 0 ? identifier[(dot + 1)..] : identifier;
    }

    private bool TryLeafIndex(QuerySource source, out int leaf)
    {
        string? alias = source switch
        {
            TableSource table => table.Alias ?? table.TableName,
            DerivedTableSource derived => derived.Alias,
            _ => null,
        };

        leaf = -1;
        return alias is not null && leafByAlias.TryGetValue(alias, out leaf);
    }

    private bool TryLeafMask(QuerySource source, out ulong mask)
    {
        mask = 0;
        if (!TryLeafIndex(source, out int leaf))
            return false;

        mask = 1UL << leaf;
        return true;
    }

    private bool TryLeafMaskOfTree(QuerySource tree, out ulong mask)
    {
        mask = 0;
        QuerySource current = tree;

        while (current is JoinSource join)
        {
            if (!TryLeafMask(join.Right, out ulong right))
                return false;

            mask |= right;
            current = join.Left;
        }

        if (!TryLeafMask(current, out ulong first))
            return false;

        mask |= first;
        return true;
    }
}
