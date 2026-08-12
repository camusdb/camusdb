
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// Resolves the relation references in a stored view body — <c>__camus_rel_{id}</c> tokens — to the
/// names those relations currently answer to.
///
/// <para>This is the one place that turns a body's durable binding into something the rest of the
/// engine can resolve, and it runs at every site that parses a stored body: query-time expansion,
/// a materialized-view rebuild, and <c>SHOW CREATE</c>. A body that names its relations directly
/// passes through untouched, so bodies written before ids were stored keep working.</para>
///
/// <para><b>Why this exists at all.</b> Storing a name inside a body makes every rename a rewrite of
/// every dependent definition, and correctness then depends on that rewrite reaching all of them.
/// Storing the id makes a name presentation: a rename changes nothing durable, and there is nothing
/// to reach. See <see cref="StoredRelationRef"/> for the token form.</para>
/// </summary>
internal static class StoredBodyBinder
{
    /// <summary>
    /// Returns <paramref name="body"/> with every FROM-position reference resolved to the
    /// referenced relation's current name, or the same instance when it holds no references.
    /// </summary>
    /// <remarks>
    /// <para>Two rules are inherited from the rename rewriter this replaces, and both matter.
    /// Only <b>FROM positions</b> are touched, so a column or a string literal that happens to look
    /// like a token is left alone. And an unchanged tree is returned as the <b>same instance</b>: a
    /// parsed body may be shared from the parser cache and <see cref="NodeAst"/> is immutable, so a
    /// resolution that changes nothing must allocate nothing.</para>
    ///
    /// <para>The result is deliberately not cached alongside the parsed body. The body's text is
    /// rename-stable now, which is what lets a cache entry survive a rename, but the names this
    /// produces come from the current schema and have to be recomputed against it.</para>
    /// </remarks>
    /// <param name="viewName">Only used to name the view in the error below.</param>
    /// <exception cref="CamusDBException">
    /// When a reference resolves to no live relation. This fails closed rather than falling back to
    /// a name lookup: <c>DROP … RESTRICT</c> refuses to orphan a view and <c>CASCADE</c> removes it,
    /// so an unresolvable id means the catalog itself is inconsistent — and because grants are keyed
    /// by the same ids, guessing here would be guessing about access.
    /// </exception>
    /// <summary>
    /// Turns a stored body into one the rest of the engine can bind: relations and columns both
    /// resolved to the names they currently answer to.
    /// </summary>
    /// <remarks>
    /// Relations first, deliberately. The column pass builds its scope by looking each FROM entry up
    /// by name, so it must run when the FROM clause holds names rather than relation tokens.
    /// </remarks>
    /// <summary>
    /// Whether a stored body carries any reference at all, and so is worth a tree walk. A body
    /// written before ids were stored carries none.
    /// </summary>
    public static bool MayContainReferences(string? sql) =>
        StoredRelationRef.MayContainReference(sql) || StoredColumnRef.MayContainReference(sql);

    public static NodeAst ResolveStoredForm(Schema schema, NodeAst body, string viewName)
        => StoredBodyColumns.Resolve(schema, Resolve(schema, body, viewName), viewName);

    /// <summary>
    /// Turns a bound body into the form that gets stored: relations and columns both replaced by
    /// their immutable ids.
    /// </summary>
    /// <remarks>
    /// Columns first, the mirror of the order above and for the same reason — the column pass needs
    /// the FROM clause still naming its relations.
    /// </remarks>
    public static NodeAst BindStoredForm(Schema schema, NodeAst body)
        => Bind(schema, StoredBodyColumns.Bind(schema, body));

    public static NodeAst ResolveRelationIds(Schema schema, NodeAst body, string viewName)
        => Resolve(schema, body, viewName);

    /// <summary>
    /// Returns <paramref name="body"/> with every FROM-position relation name replaced by a
    /// reference to that relation's immutable id. This is what gets rendered and stored.
    /// </summary>
    /// <remarks>
    /// <para><b>Every reference keeps an alias, and the unaliased case gets one.</b> A body's own
    /// qualified column references (<c>orders.total</c>) resolve through whatever the FROM clause
    /// calls the relation, and that has to stay fixed for the life of the definition — an id is not
    /// something the body's columns can be qualified by. So a bare <c>FROM orders</c> is stored as
    /// <c>FROM __camus_rel_A0 AS orders</c>: the binding moves to the id, the name the body uses
    /// internally does not move at all. The rename rewriter reached the same conclusion for the same
    /// reason, which is why it also aliases a relation that had no alias.</para>
    ///
    /// <para>A name that resolves to no relation is left alone. The binder has already rejected
    /// genuinely unknown relations, so anything unresolved here is not a stored relation.</para>
    /// </remarks>
    public static NodeAst BindRelationIds(Schema schema, NodeAst body) => Bind(schema, body);

    private static NodeAst Bind(Schema schema, NodeAst node)
    {
        NodeAst? left = node.leftAst;
        NodeAst? right = node.rightAst;
        NodeAst? one = node.extendedOne;
        NodeAst? two = node.extendedTwo;
        NodeAst? three = node.extendedThree;
        NodeAst? four = node.extendedFour;
        NodeAst? five = node.extendedFive;
        NodeAst? six = node.extendedSix;

        if (node.nodeType == NodeType.Select && right is not null)
            right = BindFrom(schema, right);
        else
            right = Recurse(right);

        left = Recurse(left);
        one = Recurse(one);
        two = Recurse(two);
        three = Recurse(three);
        four = Recurse(four);
        five = Recurse(five);
        six = Recurse(six);

        if (ReferenceEquals(left, node.leftAst) && ReferenceEquals(right, node.rightAst) &&
            ReferenceEquals(one, node.extendedOne) && ReferenceEquals(two, node.extendedTwo) &&
            ReferenceEquals(three, node.extendedThree) && ReferenceEquals(four, node.extendedFour) &&
            ReferenceEquals(five, node.extendedFive) && ReferenceEquals(six, node.extendedSix))
            return node;

        return new NodeAst(
            node.nodeType, left, right, one, two, three, four, five, node.yytext, six, node.extendedSeven);

        NodeAst? Recurse(NodeAst? child) => child is null ? null : Bind(schema, child);
    }

    private static NodeAst BindFrom(Schema schema, NodeAst from)
    {
        switch (from.nodeType)
        {
            case NodeType.Identifier:
                {
                    if (from.yytext is null || !TryGetRelationId(schema, from.yytext, out string relationId))
                        return from;

                    return new NodeAst(
                        NodeType.TableReference,
                        Identifier(StoredRelationRef.Format(relationId)),
                        Identifier(from.yytext),
                        null, null, null, null, null, null);
                }

            case NodeType.TableReference:
                {
                    string? name = from.leftAst?.yytext;

                    if (name is null || !TryGetRelationId(schema, name, out string relationId))
                        return from;

                    return new NodeAst(
                        NodeType.TableReference,
                        Identifier(StoredRelationRef.Format(relationId)),
                        from.rightAst ?? Identifier(name),
                        from.extendedOne, null, null, null, null, null);
                }

            case NodeType.DerivedTableReference:
                {
                    NodeAst inner = Bind(schema, from.leftAst!);
                    return ReferenceEquals(inner, from.leftAst)
                        ? from
                        : new NodeAst(NodeType.DerivedTableReference, inner, from.rightAst, null, null, null, null, null, null);
                }

            case NodeType.Join:
                {
                    NodeAst l = BindFrom(schema, from.leftAst!);
                    NodeAst r = BindFrom(schema, from.rightAst!);
                    NodeAst? on = from.extendedOne is null ? null : Bind(schema, from.extendedOne);

                    return ReferenceEquals(l, from.leftAst) && ReferenceEquals(r, from.rightAst) && ReferenceEquals(on, from.extendedOne)
                        ? from
                        : new NodeAst(NodeType.Join, l, r, on, null, null, null, null, null);
                }

            case NodeType.CommaJoin:
            case NodeType.CommaJoinTableList:
                {
                    NodeAst l = BindFrom(schema, from.leftAst!);
                    NodeAst r = BindFrom(schema, from.rightAst!);

                    return ReferenceEquals(l, from.leftAst) && ReferenceEquals(r, from.rightAst)
                        ? from
                        : new NodeAst(from.nodeType, l, r, null, null, null, null, null, null);
                }

            default:
                return from;
        }
    }

    private static bool TryGetRelationId(Schema schema, string name, out string relationId)
    {
        if (schema.TryResolveRelation(name, out TableSchema? table, out ViewSchema? view))
        {
            string? id = table?.Id ?? view?.Id;

            if (id is { Length: > 0 })
            {
                relationId = id;
                return true;
            }
        }

        relationId = "";
        return false;
    }

    private static NodeAst Resolve(Schema schema, NodeAst node, string viewName)
    {
        NodeAst? left = node.leftAst;
        NodeAst? right = node.rightAst;
        NodeAst? one = node.extendedOne;
        NodeAst? two = node.extendedTwo;
        NodeAst? three = node.extendedThree;
        NodeAst? four = node.extendedFour;
        NodeAst? five = node.extendedFive;
        NodeAst? six = node.extendedSix;

        // Only the FROM slot of a SELECT names a relation. Everything else is walked so that
        // subqueries in WHERE, in a projection, or inside a derived table are reached too.
        if (node.nodeType == NodeType.Select && right is not null)
            right = ResolveFrom(schema, right, viewName);
        else
            right = Recurse(right);

        left = Recurse(left);
        one = Recurse(one);
        two = Recurse(two);
        three = Recurse(three);
        four = Recurse(four);
        five = Recurse(five);
        six = Recurse(six);

        if (ReferenceEquals(left, node.leftAst) && ReferenceEquals(right, node.rightAst) &&
            ReferenceEquals(one, node.extendedOne) && ReferenceEquals(two, node.extendedTwo) &&
            ReferenceEquals(three, node.extendedThree) && ReferenceEquals(four, node.extendedFour) &&
            ReferenceEquals(five, node.extendedFive) && ReferenceEquals(six, node.extendedSix))
            return node;

        return new NodeAst(
            node.nodeType, left, right, one, two, three, four, five, node.yytext, six, node.extendedSeven);

        NodeAst? Recurse(NodeAst? child) => child is null ? null : Resolve(schema, child, viewName);
    }

    private static NodeAst ResolveFrom(Schema schema, NodeAst from, string viewName)
    {
        switch (from.nodeType)
        {
            case NodeType.Identifier:
                {
                    if (!StoredRelationRef.TryGetRelationId(from.yytext, out string relationId))
                        return from;

                    // A reference with no alias resolves to a bare name, so any qualified column
                    // reference in the body would follow the relation's current name. Bodies are
                    // written with an explicit alias precisely so that cannot happen; this arm
                    // covers an unaliased single-relation body, where there is nothing to qualify.
                    return Identifier(RequireName(schema, relationId, viewName));
                }

            case NodeType.TableReference:
                {
                    if (!StoredRelationRef.TryGetRelationId(from.leftAst?.yytext, out string relationId))
                        return from;

                    string current = RequireName(schema, relationId, viewName);

                    // An alias that repeats the relation's current name says nothing — a bare
                    // reference already binds columns to that name — so it is dropped. That is not
                    // cosmetic: every reference is stored with an alias, and echoing it back would
                    // turn every ordinary view's rendered body into "FROM orders AS orders". The
                    // alias reappears exactly when it carries information, which is after a rename.
                    if (from.extendedOne is null &&
                        string.Equals(from.rightAst?.yytext, current, StringComparison.OrdinalIgnoreCase))
                        return Identifier(current);

                    // Otherwise the alias is what the body's own column references resolve through,
                    // so it is carried across untouched. That is the whole point of storing one: the
                    // name the body uses internally is fixed at creation and a rename cannot disturb it.
                    return new NodeAst(
                        NodeType.TableReference,
                        Identifier(current),
                        from.rightAst,
                        from.extendedOne, null, null, null, null, null);
                }

            case NodeType.DerivedTableReference:
                {
                    NodeAst inner = Resolve(schema, from.leftAst!, viewName);
                    return ReferenceEquals(inner, from.leftAst)
                        ? from
                        : new NodeAst(NodeType.DerivedTableReference, inner, from.rightAst, null, null, null, null, null, null);
                }

            case NodeType.Join:
                {
                    NodeAst l = ResolveFrom(schema, from.leftAst!, viewName);
                    NodeAst r = ResolveFrom(schema, from.rightAst!, viewName);
                    NodeAst? on = from.extendedOne is null ? null : Resolve(schema, from.extendedOne, viewName);

                    return ReferenceEquals(l, from.leftAst) && ReferenceEquals(r, from.rightAst) && ReferenceEquals(on, from.extendedOne)
                        ? from
                        : new NodeAst(NodeType.Join, l, r, on, null, null, null, null, null);
                }

            case NodeType.CommaJoin:
            case NodeType.CommaJoinTableList:
                {
                    NodeAst l = ResolveFrom(schema, from.leftAst!, viewName);
                    NodeAst r = ResolveFrom(schema, from.rightAst!, viewName);

                    return ReferenceEquals(l, from.leftAst) && ReferenceEquals(r, from.rightAst)
                        ? from
                        : new NodeAst(from.nodeType, l, r, null, null, null, null, null, null);
                }

            default:
                return from;
        }
    }

    private static NodeAst Identifier(string name) =>
        new(NodeType.Identifier, null, null, null, null, null, null, null, name);

    private static string RequireName(Schema schema, string relationId, string viewName)
        => schema.TryGetRelationNameById(relationId, out string? name) && name is not null
            ? name
            : throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"The definition of view '{viewName}' reads a relation that no longer exists (id '{relationId}')");
}
