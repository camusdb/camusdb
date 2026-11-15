/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.SQLParser;

/// <summary>
/// Writes out the CTEs of a <c>WITH</c> clause, so that no tree that leaves the parser holds a
/// <see cref="NodeType.With"/> node. Each reference to a CTE becomes a derived table over a copy of
/// the CTE's query, with the reference's alias (or the CTE name) as the alias and the CTE's column
/// list as the derived table's column list. Every later stage (binding, planning, privileges, spill,
/// EXPLAIN, view bodies) then reads a derived table it already knows.
///
/// <para><b>Names.</b> A CTE is visible in the statement of its WITH and in the queries of the CTEs
/// after it, subqueries included. It is not visible in its own query or in an earlier one, so such a
/// name reads the table of that name, as in PostgreSQL. A name in a FROM list, an UPDATE … FROM list
/// or a DELETE … USING list is a reference; the target of a write never is. A CTE name hides a table
/// or a view of the same name.</para>
///
/// <para><b>RECURSIVE.</b> Under <c>WITH RECURSIVE</c> every CTE is visible in every query of the list,
/// its own included, as in PostgreSQL. A CTE whose query reads itself is recursive: its query must be
/// <c>non-recursive term UNION [ALL] recursive term</c>, and its one reference to itself becomes a
/// <see cref="NodeType.WorkTableReference"/>, with a <see cref="NodeType.RecursiveCte"/> marker on the
/// UNION. Its readers then get a copy of that query, as for any CTE, and the executor runs it step by
/// step (see <c>RecursiveUnionExecutor</c>). The other queries are written out in the order of what
/// they read, and two CTEs that read each other are refused.</para>
///
/// <para><b>Inner WITH first.</b> A WITH inside parentheses is written out when the parser reduces
/// the parentheses, before the enclosing WITH is known. That is correct: the inner WITH replaces
/// only the names it defines, and a name it leaves is still a plain relation name, which the
/// enclosing WITH then replaces when it defines it. So an inner CTE hides an outer one of the same
/// name. A WITH at the start of a statement or of the statement's source query (INSERT … SELECT,
/// CREATE TABLE … AS, a view body) is written out at the end of the parse
/// (<see cref="ExpandRemaining"/>).</para>
///
/// <para><b>Computed once.</b> Only a WITH of the second kind can mark a CTE to be computed once
/// (<see cref="NodeType.CteMaterialization"/>): its queries cannot read a column of an enclosing
/// query, so one set of rows is right for every reader. A CTE gets the marker when the WITH and the
/// later CTEs name it two or more times, or when it is written <c>AS MATERIALIZED</c>, and not
/// when it is written <c>AS NOT MATERIALIZED</c>. Every reference of one CTE holds the same marker
/// instance. A CTE in parentheses is always inlined, as is every CTE that is read once.</para>
///
/// <para><b>Copies, not shared nodes.</b> Each reference gets its own copy of the query, because
/// later stages key state by node reference (prepared IN sets, EXISTS registries, bound queries).
/// Leaves are shared: no stage writes into a leaf, and the static sentinels must stay the same
/// instances. Copies grow the tree, so one WITH may write out at most
/// <see cref="MaxExpandedNodes"/> nodes; without the limit a chain of CTEs that each read the one
/// before two times would grow the tree exponentially.</para>
///
/// <para>Every walk here uses an explicit stack: a list rule of the grammar makes the tree as deep
/// as the list is long, and a recursive walk over a long list would overflow the stack. The walks
/// assign child slots of nodes that the parse just built; that is allowed only until the tree
/// leaves the parser (the parser cache shares one tree between executions).</para>
/// </summary>
internal static class CommonTableExpressions
{
    /// <summary>The <see cref="NodeAst.yytext"/> of a definition written <c>AS MATERIALIZED</c>.</summary>
    public const string Materialized = "materialized";

    /// <summary>The <see cref="NodeAst.yytext"/> of a definition written <c>AS NOT MATERIALIZED</c>.</summary>
    public const string NotMaterialized = "not materialized";

    /// <summary>
    /// The most nodes that the references of one WITH may copy. A constant on purpose, like
    /// <see cref="StatementDepthGuard.MaxDepth"/>: it protects the parser, and no real statement
    /// comes near it.
    /// </summary>
    internal const int MaxExpandedNodes = 250_000;

    private const int SlotCount = 9;

    /// <summary>
    /// The markers of the CTEs that a parsed statement computes once, by the root that
    /// <see cref="SQLParserProcessor.Parse(string)"/> returned. A weak table, so an entry lives as long as
    /// the tree: a tree from the parser cache keeps its markers for every execution.
    /// </summary>
    private static readonly System.Runtime.CompilerServices.ConditionalWeakTable<NodeAst, NodeAst[]> markersByRoot = new();

    /// <summary>
    /// The markers of the CTEs that the statement parsed to <paramref name="root"/> computes once, or null
    /// when it has none. The statement entry points read them to open the statement's store of kept rows.
    /// </summary>
    public static NodeAst[]? ComputedOnceMarkers(NodeAst root) =>
        markersByRoot.TryGetValue(root, out NodeAst[]? markers) ? markers : null;

    /// <summary>The <see cref="NodeAst.yytext"/> of a <see cref="NodeType.With"/> written <c>WITH RECURSIVE</c>.</summary>
    public const string RecursiveWord = "recursive";

    /// <summary>The definitions of <c>WITH RECURSIVE</c>, for <see cref="With"/>.</summary>
    public static NodeAst Recursive(NodeAst definitions) =>
        new(NodeType.WithRecursiveList, definitions, null, null, null, null, null, null, null);

    /// <summary>A WITH node over <paramref name="statement"/>.</summary>
    public static NodeAst With(NodeAst definitions, NodeAst statement) =>
        definitions.nodeType == NodeType.WithRecursiveList
            ? new(NodeType.With, definitions.leftAst, statement, null, null, null, null, null, RecursiveWord)
            : new(NodeType.With, definitions, statement, null, null, null, null, null, null);

    /// <summary>One CTE definition. A qualified name is refused: a CTE belongs to no database.</summary>
    public static NodeAst Definition(NodeAst name, NodeAst? columns, string? materialization, NodeAst query)
    {
        if (name.yytext is not { Length: > 0 } text || text.Contains('.'))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"The name of a WITH query cannot be qualified: '{name.yytext}'");

        return new NodeAst(NodeType.CommonTableExpression, name, query, columns, null, null, null, null, materialization);
    }

    /// <summary>
    /// Writes out a WITH in parentheses, with every CTE inlined, or returns <paramref name="query"/>
    /// when it has no WITH.
    /// </summary>
    public static NodeAst InlineNested(NodeAst query) =>
        query.nodeType == NodeType.With ? Expand(query, markersOut: null) : query;

    /// <summary>
    /// Writes out every WITH that is left in <paramref name="root"/> at the end of the parse and returns
    /// the new root. Such a WITH starts the statement or its source query, so its CTEs may be marked to
    /// be computed once. A WITH found inside another is written out first.
    /// </summary>
    public static NodeAst ExpandRemaining(NodeAst root)
    {
        // Ancestors come before their descendants in this list, so the reverse order writes out an
        // inner WITH before the WITH around it.
        List<(NodeAst? Parent, int Slot, NodeAst Node)> found = [];

        if (root.nodeType == NodeType.With)
            found.Add((null, -1, root));

        Stack<NodeAst> pending = new();
        pending.Push(root);

        while (pending.TryPop(out NodeAst? node))
        {
            for (int i = 0; i < SlotCount; i++)
            {
                NodeAst? child = Child(node, i);
                if (child is null)
                    continue;

                if (child.nodeType == NodeType.With)
                    found.Add((node, i, child));

                pending.Push(child);
            }
        }

        List<NodeAst> markers = [];

        for (int k = found.Count - 1; k >= 0; k--)
        {
            (NodeAst? parent, int slot, NodeAst with) = found[k];
            NodeAst expanded = Expand(with, markers);

            if (parent is null)
                root = expanded;
            else
                SetChild(parent, slot, expanded);
        }

        if (markers.Count > 0)
            markersByRoot.AddOrUpdate(root, markers.ToArray());

        return root;
    }

    /// <summary>
    /// Writes out one WITH: checks the names, counts the references of each CTE, gives a marker to each
    /// CTE that is computed once, and replaces every reference by a derived table over a copy of the
    /// CTE's query. The queries are written out in order, so a copy of a CTE's query already holds the
    /// CTEs before it.
    /// </summary>
    /// <param name="markersOut">Gets the marker of each CTE that is computed once; null for a WITH in
    /// parentheses, whose CTEs are all inlined.</param>
    private static NodeAst Expand(NodeAst with, List<NodeAst>? markersOut)
    {
        List<NodeAst> definitions = Flatten(with.leftAst!);
        Dictionary<string, int> indexByName = new(definitions.Count, StringComparer.OrdinalIgnoreCase);

        for (int i = 0; i < definitions.Count; i++)
        {
            string name = definitions[i].leftAst!.yytext!;
            if (!indexByName.TryAdd(name, i))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"WITH query name '{name}' is specified more than once");
        }

        NodeAst statement = with.rightAst!;
        bool recursive = with.yytext == RecursiveWord;
        int count = definitions.Count;

        // Without RECURSIVE, the query of CTE k sees the CTEs before it. With RECURSIVE, every query sees
        // every CTE of the list, itself included, as in PostgreSQL, and the queries are written out in the
        // order of what they read. The statement sees all of them.
        int[] order = recursive ? PrepareRecursive(definitions, indexByName) : Enumerable.Range(0, count).ToArray();

        int[] references = new int[count];
        for (int k = 0; k < count; k++)
            CountReferences(definitions[k].rightAst!, indexByName, visible: recursive ? count : k, references);

        CountReferences(statement, indexByName, visible: count, references);

        NodeAst?[] markers = new NodeAst?[definitions.Count];
        if (markersOut is not null)
        {
            for (int i = 0; i < definitions.Count; i++)
            {
                string? hint = definitions[i].yytext;
                bool once = hint == Materialized || (hint != NotMaterialized && references[i] >= 2);

                // A CTE that nothing reads gets no marker: no reader would ever compute it.
                if (once && references[i] > 0)
                {
                    markers[i] = new NodeAst(NodeType.CteMaterialization, null, null, null, null, null, null, null, definitions[i].leftAst!.yytext);
                    markersOut.Add(markers[i]!);
                }
            }
        }

        Expansion expansion = new(definitions, markers);

        // A copy of a CTE's query must already hold the CTEs that it reads, so each query is written out
        // after theirs. A recursive CTE no longer names itself here: its reference is a work table now.
        foreach (int k in order)
            expansion.Substitute(definitions[k].rightAst!, indexByName, visible: recursive ? count : k);

        expansion.Substitute(statement, indexByName, visible: count);
        return statement;
    }

    /// <summary>
    /// Prepares the definitions of a WITH RECURSIVE list and returns the order in which their queries are
    /// written out: each CTE after the CTEs that its query reads. A CTE whose query reads itself is
    /// recursive, and <see cref="RecursiveTerm.Prepare"/> checks it and replaces that reference. Two or
    /// more CTEs that read each other are refused, as in PostgreSQL.
    /// </summary>
    private static int[] PrepareRecursive(List<NodeAst> definitions, Dictionary<string, int> indexByName)
    {
        int count = definitions.Count;
        List<int>[] reads = new List<int>[count];

        for (int k = 0; k < count; k++)
        {
            int[] counts = new int[count];
            CountReferences(definitions[k].rightAst!, indexByName, visible: count, counts);

            reads[k] = [];
            for (int i = 0; i < count; i++)
            {
                if (counts[i] > 0)
                    reads[k].Add(i);
            }
        }

        int[] order = DependencyOrder(definitions, reads);

        for (int k = 0; k < count; k++)
        {
            if (reads[k].Contains(k))
                RecursiveTerm.Prepare(definitions[k]);
        }

        return order;
    }

    /// <summary>
    /// The definitions in an order where each one comes after the ones it reads (a depth-first post-order).
    /// A definition that reads itself is not a cycle here. Any other cycle is mutual recursion, which
    /// PostgreSQL does not implement either.
    /// </summary>
    private static int[] DependencyOrder(List<NodeAst> definitions, List<int>[] reads)
    {
        int count = definitions.Count;

        // 0 = not seen, 1 = on the current path, 2 = done.
        byte[] state = new byte[count];
        List<int> order = new(count);
        Stack<(int Index, int Next)> path = new();

        for (int start = 0; start < count; start++)
        {
            if (state[start] != 0)
                continue;

            state[start] = 1;
            path.Push((start, 0));

            while (path.TryPop(out (int Index, int Next) frame))
            {
                if (frame.Next == reads[frame.Index].Count)
                {
                    state[frame.Index] = 2;
                    order.Add(frame.Index);
                    continue;
                }

                path.Push((frame.Index, frame.Next + 1));
                int read = reads[frame.Index][frame.Next];

                if (read == frame.Index || state[read] == 2)
                    continue;

                if (state[read] == 1)
                    throw new CamusDBException(
                        CamusDBErrorCodes.FeatureNotSupported,
                        $"Mutual recursion between WITH queries is not supported: '{definitions[frame.Index].leftAst!.yytext}' " +
                        $"and '{definitions[read].leftAst!.yytext}' are in a cycle of WITH queries that read each other");

                state[read] = 1;
                path.Push((read, 0));
            }
        }

        return order.ToArray();
    }

    /// <summary>
    /// Checks the query of a recursive CTE and replaces its reference to itself by a
    /// <see cref="NodeType.WorkTableReference"/>. The rules are the ones PostgreSQL checks before it plans
    /// the query:
    /// <list type="bullet">
    /// <item>The query is <c>non-recursive term UNION [ALL] recursive term</c>, with no ORDER BY, LIMIT
    /// or OFFSET on the UNION.</item>
    /// <item>The non-recursive term does not read the CTE.</item>
    /// <item>The recursive term reads it one time, in a FROM: not in a subquery of an expression, not on
    /// the side of an outer join that can be NULL, not in INTERSECT ALL, and not in EXCEPT (the right
    /// side, or the left side of EXCEPT ALL). In each of those places a row of the term would depend on
    /// rows that a later step adds, so a step that sees only the rows of the step before gives a wrong
    /// answer.</item>
    /// </list>
    /// The binder refuses an aggregate over the reference: an aggregate can be a function of an installed
    /// extension, and the parser does not know the database.
    /// </summary>
    private static class RecursiveTerm
    {
        /// <summary>Where the walk found a reference: a place that allows it, or the first place that refuses it.</summary>
        private enum Context { Allowed, Subquery, OuterJoin, Intersect, Except }

        private readonly record struct Found(NodeAst Parent, int Slot, NodeAst Item, Context Context);

        public static void Prepare(NodeAst definition)
        {
            string name = definition.leftAst!.yytext!;
            NodeAst query = definition.rightAst!;

            if (!SetOperationAst.IsSetOperationSelect(query) || SetOperationAst.KindOf(query.rightAst!) != SetOperationAst.Union)
                throw Invalid($"Recursive query '{name}' does not have the form non-recursive-term UNION [ALL] recursive-term");

            // The ORDER BY, LIMIT and OFFSET of the whole UNION.
            if (query.extendedTwo is not null)
                throw NotSupported($"ORDER BY in a recursive query is not supported: '{name}'");

            if (query.extendedThree is not null)
                throw NotSupported($"LIMIT in a recursive query is not supported: '{name}'");

            if (query.extendedFour is not null)
                throw NotSupported($"OFFSET in a recursive query is not supported: '{name}'");

            NodeAst union = query.rightAst!;

            if (Find(union.leftAst!, name).Count > 0)
                throw Invalid($"The recursive reference to query '{name}' must not appear within its non-recursive term");

            List<Found> found = Find(union.rightAst!, name);

            if (found.Count == 0)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"The recursive term of query '{name}' has no reference to it");

            if (found.Count > 1)
                throw Invalid($"The recursive reference to query '{name}' must not appear more than once");

            Found reference = found[0];

            string? place = reference.Context switch
            {
                Context.Subquery => "within a subquery",
                Context.OuterJoin => "within an outer join",
                Context.Intersect => "within INTERSECT ALL",
                Context.Except => "within EXCEPT",
                _ => null,
            };

            if (place is not null)
                throw Invalid($"The recursive reference to query '{name}' must not appear {place}");

            SetChild(reference.Parent, reference.Slot, WorkTable(reference.Item, name));
            union.extendedOne = new NodeAst(NodeType.RecursiveCte, definition.extendedOne, null, null, null, null, null, null, name);
        }

        /// <summary>Every relation slot under <paramref name="tree"/> that names <paramref name="name"/>, with the place it is in.</summary>
        private static List<Found> Find(NodeAst tree, string name)
        {
            List<Found> found = [];
            Stack<(NodeAst Node, Context Context)> pending = new();
            pending.Push((tree, Context.Allowed));

            while (pending.TryPop(out (NodeAst Node, Context Context) entry))
            {
                NodeAst node = entry.Node;

                for (int i = 0; i < SlotCount; i++)
                {
                    NodeAst? child = Child(node, i);
                    if (child is null)
                        continue;

                    Context context = ChildContext(node, i, entry.Context);

                    if (IsRelationSlot(node, i) && string.Equals(RelationName(child), name, StringComparison.OrdinalIgnoreCase))
                    {
                        found.Add(new Found(node, i, child, context));
                        continue;
                    }

                    pending.Push((child, context));
                }
            }

            return found;
        }

        /// <summary>
        /// The place of slot <paramref name="slot"/> of <paramref name="node"/>. A place that refuses the
        /// reference keeps its refusal for everything under it, so the first refusal on the path names
        /// the error, as in PostgreSQL.
        /// </summary>
        private static Context ChildContext(NodeAst node, int slot, Context context)
        {
            if (context != Context.Allowed)
                return context;

            switch (node.nodeType)
            {
                case NodeType.Select:
                    // The FROM is a relation; every other slot is an expression, where a query is a subquery.
                    return slot == 1 ? Context.Allowed : Context.Subquery;

                case NodeType.Join:
                {
                    JoinAstKind.Kind kind = JoinAstKind.Read(node);
                    return slot switch
                    {
                        0 => kind == JoinAstKind.Kind.Right ? Context.OuterJoin : Context.Allowed,
                        1 => kind == JoinAstKind.Kind.Left ? Context.OuterJoin : Context.Allowed,
                        _ => Context.Subquery,
                    };
                }

                case NodeType.SetOperationReference when slot <= 1:
                {
                    bool all = SetOperationAst.IsAll(node);
                    return SetOperationAst.KindOf(node) switch
                    {
                        SetOperationAst.Intersect when all => Context.Intersect,
                        SetOperationAst.Except when slot == 1 || all => Context.Except,
                        _ => Context.Allowed,
                    };
                }

                default:
                    return context;
            }
        }

        /// <summary>The work-table reference that replaces <paramref name="item"/>. A hint is refused: a work table has no index and no cache.</summary>
        private static NodeAst WorkTable(NodeAst item, string name)
        {
            NodeAst? hint = item.nodeType switch
            {
                NodeType.TableReference => item.extendedOne,
                NodeType.IdentifierWithOpts => item,
                _ => null,
            };

            if (hint is not null)
                throw Invalid($"A hint cannot be applied to the recursive reference to query '{name}'");

            NodeAst? alias = item.nodeType == NodeType.TableReference ? item.rightAst : null;
            NodeAst identifier = new(NodeType.Identifier, null, null, null, null, null, null, null, name);

            return new NodeAst(NodeType.WorkTableReference, identifier, alias, null, null, null, null, null, null);
        }

        private static CamusDBException Invalid(string message) => new(CamusDBErrorCodes.InvalidInput, message);

        private static CamusDBException NotSupported(string message) => new(CamusDBErrorCodes.FeatureNotSupported, message);
    }

    /// <summary>
    /// True when a relation slot under <paramref name="tree"/> names <paramref name="name"/> as a plain
    /// relation: a table, a view or a CTE reference not yet written out. A work-table reference is not
    /// one. The view-body renderer uses it before it writes a recursive CTE back as a WITH.
    /// </summary>
    internal static bool ReadsRelationNamed(NodeAst tree, string name)
    {
        Stack<NodeAst> pending = new();
        pending.Push(tree);

        while (pending.TryPop(out NodeAst? node))
        {
            for (int i = 0; i < SlotCount; i++)
            {
                NodeAst? child = Child(node, i);
                if (child is null)
                    continue;

                if (IsRelationSlot(node, i) && string.Equals(RelationName(child), name, StringComparison.OrdinalIgnoreCase))
                    return true;

                pending.Push(child);
            }
        }

        return false;
    }

    /// <summary>The definitions of a WITH list in written order. The list is left-deep, so the walk follows the left spine.</summary>
    private static List<NodeAst> Flatten(NodeAst list)
    {
        List<NodeAst> definitions = [];
        NodeAst node = list;

        while (node.nodeType == NodeType.CommonTableExpressionList)
        {
            definitions.Add(node.rightAst!);
            node = node.leftAst!;
        }

        definitions.Add(node);
        definitions.Reverse();
        return definitions;
    }

    /// <summary>Adds one to the count of each CTE that a relation slot under <paramref name="tree"/> names.</summary>
    private static void CountReferences(NodeAst tree, Dictionary<string, int> indexByName, int visible, int[] references)
    {
        Stack<NodeAst> pending = new();
        pending.Push(tree);

        while (pending.TryPop(out NodeAst? node))
        {
            for (int i = 0; i < SlotCount; i++)
            {
                NodeAst? child = Child(node, i);
                if (child is null)
                    continue;

                if (IsRelationSlot(node, i) && ReferencedIndex(child, indexByName, visible) is { } index)
                {
                    references[index]++;
                    continue;
                }

                pending.Push(child);
            }
        }
    }

    /// <summary>
    /// True when slot <paramref name="slot"/> of <paramref name="node"/> holds a relation: the FROM of a
    /// SELECT, an operand of a join, an item of a comma list, the FROM list of an UPDATE and the USING
    /// list of a DELETE (see <see cref="DmlJoinSlots"/>). The target of a write is never one.
    /// </summary>
    private static bool IsRelationSlot(NodeAst node, int slot) => node.nodeType switch
    {
        NodeType.Select => slot == 1,
        NodeType.Join or NodeType.CommaJoin or NodeType.CommaJoinTableList => slot is 0 or 1,
        NodeType.Update => slot == 5,
        NodeType.Delete => slot == 4,
        _ => false,
    };

    /// <summary>The name of the relation that a FROM item names, or null when the item is not a plain name.</summary>
    private static string? RelationName(NodeAst item) => item.nodeType switch
    {
        NodeType.TableReference => item.leftAst?.yytext,
        NodeType.Identifier => item.yytext,
        NodeType.IdentifierWithOpts => item.leftAst?.yytext,
        _ => null,
    };

    /// <summary>The index of the visible CTE that <paramref name="item"/> names, or null.</summary>
    private static int? ReferencedIndex(NodeAst item, Dictionary<string, int> indexByName, int visible) =>
        RelationName(item) is { } name && indexByName.TryGetValue(name, out int index) && index < visible
            ? index
            : null;

    /// <summary>The state of one expansion: the definitions, their markers and the count of copied nodes.</summary>
    private sealed class Expansion(List<NodeAst> definitions, NodeAst?[] markers)
    {
        private long copiedNodes;

        /// <summary>
        /// Replaces each relation slot under <paramref name="tree"/> that names a visible CTE by a derived
        /// table. The walk does not enter a new derived table: its copy was written out already, and a
        /// name left in it must not read a CTE that its own query cannot see.
        /// </summary>
        public void Substitute(NodeAst tree, Dictionary<string, int> indexByName, int visible)
        {
            Stack<NodeAst> pending = new();
            pending.Push(tree);

            while (pending.TryPop(out NodeAst? node))
            {
                for (int i = 0; i < SlotCount; i++)
                {
                    NodeAst? child = Child(node, i);
                    if (child is null)
                        continue;

                    if (IsRelationSlot(node, i) && ReferencedIndex(child, indexByName, visible) is { } index)
                    {
                        SetChild(node, i, Reference(child, index));
                        continue;
                    }

                    pending.Push(child);
                }
            }
        }

        /// <summary>
        /// The derived table that replaces <paramref name="item"/>, a reference to CTE <paramref name="index"/>.
        /// A cache hint on the reference moves onto the derived table, as for a view. An index hint is
        /// refused: a CTE has no index, and the hint would apply to a relation the statement does not name.
        /// </summary>
        private NodeAst Reference(NodeAst item, int index)
        {
            NodeAst definition = definitions[index];
            string name = definition.leftAst!.yytext!;

            NodeAst? hint = item.nodeType switch
            {
                NodeType.TableReference => item.extendedOne,
                NodeType.IdentifierWithOpts => item,
                _ => null,
            };

            if (hint is not null && hint.nodeType != NodeType.CacheHint)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"An index hint cannot be applied to WITH query '{name}': a WITH query has no indexes");

            NodeAst alias = item.nodeType == NodeType.TableReference && item.rightAst is { } written
                ? written
                : new NodeAst(NodeType.Identifier, null, null, null, null, null, null, null, RelationName(item));

            NodeAst query = Copy(definition.rightAst!);
            NodeAst? columns = definition.extendedOne is { } list ? Copy(list) : null;

            return new NodeAst(NodeType.DerivedTableReference, query, alias, hint, null, columns, markers[index], null, null);
        }

        /// <summary>A copy of <paramref name="source"/> in which every node with a child is new and every leaf is shared.</summary>
        private NodeAst Copy(NodeAst source)
        {
            Count();

            if (IsLeaf(source))
                return source;

            NodeAst root = Shallow(source);
            Stack<NodeAst> pending = new();
            pending.Push(root);

            while (pending.TryPop(out NodeAst? node))
            {
                for (int i = 0; i < SlotCount; i++)
                {
                    NodeAst? child = Child(node, i);
                    if (child is null)
                        continue;

                    Count();

                    if (IsLeaf(child))
                        continue;

                    NodeAst copy = Shallow(child);
                    SetChild(node, i, copy);
                    pending.Push(copy);
                }
            }

            return root;
        }

        private void Count()
        {
            if (++copiedNodes > MaxExpandedNodes)
                throw new CamusDBException(
                    CamusDBErrorCodes.StatementTooDeeplyNested,
                    $"The WITH queries of the statement write out more than {MaxExpandedNodes} nodes. " +
                    "A WITH query that is read many times, or that reads an earlier one many times, " +
                    "multiplies the size of the statement");
        }
    }

    private static bool IsLeaf(NodeAst node) =>
        node.leftAst is null && node.rightAst is null && node.extendedOne is null && node.extendedTwo is null
        && node.extendedThree is null && node.extendedFour is null && node.extendedFive is null
        && node.extendedSix is null && node.extendedSeven is null;

    private static NodeAst Shallow(NodeAst node) =>
        new(node.nodeType, node.leftAst, node.rightAst, node.extendedOne, node.extendedTwo, node.extendedThree,
            node.extendedFour, node.extendedFive, node.yytext, node.extendedSix, node.extendedSeven);

    private static NodeAst? Child(NodeAst node, int slot) => slot switch
    {
        0 => node.leftAst,
        1 => node.rightAst,
        2 => node.extendedOne,
        3 => node.extendedTwo,
        4 => node.extendedThree,
        5 => node.extendedFour,
        6 => node.extendedFive,
        7 => node.extendedSix,
        _ => node.extendedSeven,
    };

    private static void SetChild(NodeAst node, int slot, NodeAst value)
    {
        switch (slot)
        {
            case 0: node.leftAst = value; break;
            case 1: node.rightAst = value; break;
            case 2: node.extendedOne = value; break;
            case 3: node.extendedTwo = value; break;
            case 4: node.extendedThree = value; break;
            case 5: node.extendedFour = value; break;
            case 6: node.extendedFive = value; break;
            case 7: node.extendedSix = value; break;
            default: node.extendedSeven = value; break;
        }
    }
}
