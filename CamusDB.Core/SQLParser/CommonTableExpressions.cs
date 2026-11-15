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

    /// <summary>A WITH node over <paramref name="statement"/>.</summary>
    public static NodeAst With(NodeAst definitions, NodeAst statement) =>
        new(NodeType.With, definitions, statement, null, null, null, null, null, null);

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

        // The query of CTE k sees the CTEs before it; the statement sees all of them.
        int[] references = new int[definitions.Count];
        for (int k = 0; k < definitions.Count; k++)
            CountReferences(definitions[k].rightAst!, indexByName, visible: k, references);

        CountReferences(statement, indexByName, visible: definitions.Count, references);

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

        for (int k = 0; k < definitions.Count; k++)
            expansion.Substitute(definitions[k].rightAst!, indexByName, visible: k);

        expansion.Substitute(statement, indexByName, visible: definitions.Count);
        return statement;
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
