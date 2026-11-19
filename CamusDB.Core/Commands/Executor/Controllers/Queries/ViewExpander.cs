
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using System.Runtime.CompilerServices;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Rewrites references to non-materialized views into derived tables, before the query is bound.
///
/// <para><b>Why rewrite the AST rather than teach the planner about views.</b> CamusDB already has
/// derived tables (<c>FROM (SELECT …) AS a</c>) wired through every stage: the planner, the join
/// executor, correlated-subquery rewriting, aggregation, spill, the cost model, <c>EXPLAIN</c>, and
/// — the part that is easy to forget — serializable range/predicate-lock acquisition on the
/// underlying base tables. A view <i>is</i> a named derived table. Expanding it into one before
/// binding means every one of those behaviors is inherited exactly, with no second code path that
/// could drift from the first. There is deliberately no "view scan" plan node: the plan shows what
/// actually runs.</para>
///
/// <para><b>Materialized views are not expanded.</b> They are real relations and resolve to a
/// <c>TableDescriptor</c> like any table; only the populated check applies to them.</para>
/// </summary>
internal static class ViewExpander
{
    /// <summary>
    /// Returns <paramref name="ast"/> with every view reference replaced by a derived table, or the
    /// same instance when the statement references no views.
    /// </summary>
    /// <remarks>
    /// Returning the original instance unchanged matters beyond allocation: a parsed AST may be
    /// shared across concurrent executions of the same SQL text from the parser cache, and the
    /// immutability invariant on <see cref="NodeAst"/> means a rewrite must build new nodes rather
    /// than mutate. Not rewriting at all is the safest possible version of that.
    /// </remarks>
    /// <param name="authorize">
    /// Invoked for every view this expansion resolves, before its body is substituted. This is the
    /// only moment a read is aware a view was named at all — expansion replaces it with a derived
    /// table, so nothing downstream can check the caller against the view object. Passing null skips
    /// the check and is for callers that have already made it.
    /// </param>
    /// <param name="maxNodes">
    /// Most syntax-tree nodes the expansion may produce, counting each body as often as it is expanded;
    /// <c>&lt;= 0</c> for no limit. See <see cref="CamusDBOptions.MaxViewExpandedNodes"/>.
    /// </param>
    /// <param name="maxRelations">
    /// Most view references the expansion may substitute; <c>&lt;= 0</c> for no limit. See
    /// <see cref="CamusDBOptions.MaxViewExpandedRelations"/>.
    /// </param>
    public static NodeAst Expand(
        Schema schema,
        NodeAst ast,
        int maxDepth,
        Func<string, NodeAst> parseBody,
        Action<string, ViewSchema>? authorize = null,
        long maxNodes = 0,
        long maxRelations = 0)
    {
        // The overwhelmingly common case is a statement with no views at all; a whole-tree walk that
        // allocates nothing when there is nothing to do keeps this off the hot path's conscience.
        if (schema.Views.Count == 0)
            return ast;

        return Rewrite(schema, ast, new Expansion(maxDepth, parseBody, authorize, maxNodes, maxRelations));
    }

    /// <summary>
    /// The state of one statement's expansion: the views being expanded right now (for the cycle and
    /// depth checks), and what the expansion has cost so far against its budget.
    /// </summary>
    /// <remarks>
    /// <para><b>Budget.</b> Depth bounds nesting only. A graph in which
    /// each level reads the level below twice expands 2^n copies of the bottom view at depth n, so a
    /// handful of short definitions can turn one later read into unbounded CPU and memory. The budget
    /// counts every substitution and every node it copies, however the graph is shaped.</para>
    ///
    /// <para><b>Charged before the copy.</b> A view is charged its body's node count before the body
    /// is rewritten — the rewrite is what allocates — so a statement over budget fails before it spends
    /// the memory.</para>
    /// </remarks>
    private sealed class Expansion(
        int maxDepth,
        Func<string, NodeAst> parseBody,
        Action<string, ViewSchema>? authorize,
        long maxNodes,
        long maxRelations)
    {
        public readonly List<string> Stack = [];

        public readonly int MaxDepth = maxDepth;

        public readonly Func<string, NodeAst> ParseBody = parseBody;

        public readonly Action<string, ViewSchema>? Authorize = authorize;

        private long nodes;

        private long relations;

        public void Charge(string viewName, long bodyNodes)
        {
            relations++;
            if (maxRelations > 0 && relations > maxRelations)
                throw new CamusDBException(
                    CamusDBErrorCodes.ViewComplexityLimitExceeded,
                    $"Expanding the views of this statement substitutes more than {maxRelations} view references " +
                    $"(max_view_expanded_relations), at view '{viewName}'. A view graph in which views read " +
                    "other views more than once multiplies at every level; simplify the views, or raise the setting.");

            nodes += bodyNodes;
            if (maxNodes > 0 && nodes > maxNodes)
                throw new CamusDBException(
                    CamusDBErrorCodes.ViewComplexityLimitExceeded,
                    $"Expanding the views of this statement produces more than {maxNodes} syntax-tree nodes " +
                    $"(max_view_expanded_nodes), at view '{viewName}'. Simplify the views, or raise the setting.");
        }
    }

    /// <summary>
    /// The node count of each parsed body. A parsed body is shared from the parser cache and never
    /// changes, so its count is computed once per instance and reused by every expansion of it.
    /// </summary>
    private static readonly ConditionalWeakTable<NodeAst, StrongBox<long>> bodyNodeCounts = new();

    /// <summary>
    /// The stored form of each parsed body resolved against a schema, reused by every reference to the
    /// same view until the schema changes. Safe to share: a <see cref="NodeAst"/> is immutable, and
    /// the expansion builds new nodes around it rather than editing it. Keyed by the body instance, and
    /// checked against the schema instance and version, because one SQL text can be a body in two
    /// databases, and a rename changes what its ids resolve to.
    /// </summary>
    private static readonly ConditionalWeakTable<NodeAst, ResolvedBody> resolvedBodies = new();

    private sealed record ResolvedBody(WeakReference<Schema> Schema, long SchemaVersion, string ViewName, NodeAst Resolved);

    private static long CountBodyNodes(NodeAst body)
        => bodyNodeCounts.GetValue(body, static b => new StrongBox<long>(SQLParser.NodeAstWalk.Count(b))).Value;

    private static NodeAst ResolveBody(Schema schema, NodeAst body, string viewName)
    {
        if (resolvedBodies.TryGetValue(body, out ResolvedBody? cached)
            && cached.SchemaVersion == schema.SchemaVersion
            && string.Equals(cached.ViewName, viewName, StringComparison.Ordinal)
            && cached.Schema.TryGetTarget(out Schema? cachedSchema)
            && ReferenceEquals(cachedSchema, schema))
            return cached.Resolved;

        // Runs without the schema lock, so a rename can apply while the ids are turned into names. The
        // version is read first and the result is cached only if it did not move: a body that holds
        // the old names must not be stored under the new version, where every later read would use it.
        long version = schema.SchemaVersion;
        NodeAst resolved = DDL.StoredBodyBinder.ResolveStoredForm(schema, body, viewName);

        AfterResolveForTesting?.Invoke(viewName);

        if (schema.SchemaVersion == version)
            resolvedBodies.AddOrUpdate(body, new ResolvedBody(new WeakReference<Schema>(schema), version, viewName, resolved));

        return resolved;
    }

    /// <summary>
    /// Test-only seam: invoked with the view's name after its body is resolved and before the result
    /// is cached, so a test can apply a rename inside that window. Null in production.
    /// </summary>
    internal static Action<string>? AfterResolveForTesting { get; set; }

    private static NodeAst Rewrite(Schema schema, NodeAst node, Expansion expansion)
    {
        NodeAst? left = node.leftAst;
        NodeAst? right = node.rightAst;
        NodeAst? one = node.extendedOne;
        NodeAst? two = node.extendedTwo;
        NodeAst? three = node.extendedThree;
        NodeAst? four = node.extendedFour;
        NodeAst? five = node.extendedFive;
        NodeAst? six = node.extendedSix;

        // Only the FROM slot of a SELECT can name a relation; rewriting there (rather than at every
        // identifier) is what keeps a column that happens to share a view's name from being rewritten.
        if (node.nodeType == NodeType.Select && right is not null)
            right = RewriteFrom(schema, right, expansion);

        left = RewriteChild(schema, left, expansion);
        one = RewriteChild(schema, one, expansion);
        two = RewriteChild(schema, two, expansion);
        three = RewriteChild(schema, three, expansion);
        four = RewriteChild(schema, four, expansion);
        five = RewriteChild(schema, five, expansion);
        six = RewriteChild(schema, six, expansion);

        if (node.nodeType != NodeType.Select)
            right = RewriteChild(schema, right, expansion);

        if (ReferenceEquals(left, node.leftAst) && ReferenceEquals(right, node.rightAst) &&
            ReferenceEquals(one, node.extendedOne) && ReferenceEquals(two, node.extendedTwo) &&
            ReferenceEquals(three, node.extendedThree) && ReferenceEquals(four, node.extendedFour) &&
            ReferenceEquals(five, node.extendedFive) && ReferenceEquals(six, node.extendedSix))
            return node;

        return new NodeAst(
            node.nodeType, left, right, one, two, three, four, five, node.yytext, six, node.extendedSeven);
    }

    private static NodeAst? RewriteChild(Schema schema, NodeAst? child, Expansion expansion)
        => child is null ? null : Rewrite(schema, child, expansion);

    /// <summary>
    /// Rewrites the FROM clause: substitutes any relation name that resolves to a view, and recurses
    /// through joins and derived tables.
    /// </summary>
    private static NodeAst RewriteFrom(Schema schema, NodeAst from, Expansion expansion)
    {
        switch (from.nodeType)
        {
            case NodeType.Identifier:
                // A bare identifier in FROM position carries no alias of its own, so the view's name
                // becomes the alias — that is what keeps `SELECT v.col FROM v` resolving.
                return TrySubstitute(schema, from.yytext, from.yytext, expansion) ?? from;

            case NodeType.TableReference:
                {
                    string? name = from.leftAst?.yytext;
                    string? alias = from.rightAst?.yytext ?? name;

                    NodeAst? substituted = TrySubstitute(schema, name, alias, expansion);
                    if (substituted is not null)
                    {
                        // The two hint kinds are not alike, and treating them alike refused something
                        // legitimate. An INDEX hint names an index of the relation it is attached to;
                        // a view has none of its own, so applying it would silently pin a plan on a
                        // relation the statement does not name — that stays refused. A CACHE hint
                        // names a result-cache bucket for the whole query and says nothing about any
                        // relation's indexes, so there is nothing about a view that makes it invalid.
                        if (from.extendedOne is not null && from.extendedOne.nodeType != NodeType.CacheHint)
                            throw new CamusDBException(
                                CamusDBErrorCodes.InvalidInput,
                                $"An index hint cannot be applied to view '{name}': a view has no indexes " +
                                "of its own, and the hint would silently target a relation the statement " +
                                "does not name");

                        // Carried onto the derived table the view became, so the response can report
                        // what became of it rather than the hint vanishing between parse and execution.
                        return from.extendedOne is null
                            ? substituted
                            : new NodeAst(
                                NodeType.DerivedTableReference,
                                substituted.leftAst, substituted.rightAst, from.extendedOne,
                                // extendedTwo carries the owner; dropping it here would silently make
                                // a hinted view reference run as the caller instead of its owner.
                                substituted.extendedTwo, null, null, null, null);
                    }

                    return from;
                }

            case NodeType.DerivedTableReference:
                {
                    NodeAst inner = Rewrite(schema, from.leftAst!, expansion);
                    return ReferenceEquals(inner, from.leftAst)
                        ? from
                        : new NodeAst(NodeType.DerivedTableReference, inner, from.rightAst, from.extendedOne, from.extendedTwo, from.extendedThree, from.extendedFour, null, from.yytext);
                }

            case NodeType.SetOperationReference:
                {
                    // Each operand is a complete SELECT with a FROM of its own, so a view named in
                    // either one is expanded, and checked against the caller, like a view in any FROM.
                    NodeAst l = Rewrite(schema, from.leftAst!, expansion);
                    NodeAst r = Rewrite(schema, from.rightAst!, expansion);

                    return ReferenceEquals(l, from.leftAst) && ReferenceEquals(r, from.rightAst)
                        ? from
                        : new NodeAst(NodeType.SetOperationReference, l, r, from.extendedOne, null, null, null, null, from.yytext);
                }

            case NodeType.Join:
                {
                    NodeAst l = RewriteFrom(schema, from.leftAst!, expansion);
                    NodeAst r = RewriteFrom(schema, from.rightAst!, expansion);
                    NodeAst? on = RewriteChild(schema, from.extendedOne, expansion);

                    return ReferenceEquals(l, from.leftAst) && ReferenceEquals(r, from.rightAst) && ReferenceEquals(on, from.extendedOne)
                        ? from
                        : new NodeAst(NodeType.Join, l, r, on, null, null, null, null, from.yytext);
                }

            case NodeType.CommaJoin:
            case NodeType.CommaJoinTableList:
                {
                    NodeAst l = RewriteFrom(schema, from.leftAst!, expansion);
                    NodeAst r = RewriteFrom(schema, from.rightAst!, expansion);

                    return ReferenceEquals(l, from.leftAst) && ReferenceEquals(r, from.rightAst)
                        ? from
                        : new NodeAst(from.nodeType, l, r, null, null, null, null, null, null);
                }

            default:
                return from;
        }
    }

    /// <summary>
    /// Builds the derived-table node that replaces a view reference, or null when the name is not a
    /// view (a table, a materialized view, or unknown — all of which resolve later, as they always did).
    /// </summary>
    private static NodeAst? TrySubstitute(Schema schema, string? name, string? alias, Expansion expansion)
    {
        if (name is null || !schema.Views.TryGetValue(name, out ViewSchema? view))
            return null;

        // Only views the CALLER named are checked against the caller. A view referenced from inside
        // another view's body belongs to that view's owner, not to whoever is reading — checking it
        // here would make every nested view require a grant to the reader, which is precisely the
        // encapsulation a view exists to provide.
        //
        // The nested reference is checked too, on every read, but by the binder: it is checked against
        // the enclosing view's owner, and only the binder knows that principal, because only it enters
        // the owner's scope. Checked before the body is read, so a caller with no grant learns nothing
        // about what the view selects from, not even through a differing error.
        if (expansion.Stack.Count == 0)
            expansion.Authorize?.Invoke(name, view);

        if (view.Definition?.Sql is not { Length: > 0 } sql || view.Id is null)
            throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"View '{name}' has no stored definition");

        // The DDL-time acyclicity check is the real defense; this catches a cycle that reached the
        // stored schema anyway (a hand-edited checkpoint, a partially-applied log) rather than
        // recursing until the stack gives out.
        if (expansion.Stack.Contains(view.Id, StringComparer.Ordinal))
            throw new CamusDBException(
                CamusDBErrorCodes.ViewRecursionDetected,
                $"Infinite recursion detected in the definition of view '{name}'");

        if (expansion.Stack.Count >= expansion.MaxDepth)
            throw new CamusDBException(
                CamusDBErrorCodes.ViewRecursionDetected,
                $"View expansion for '{name}' exceeded the maximum nesting depth of {expansion.MaxDepth}");

        // The parser cache returns the same instance for the same text, so a view referenced many
        // times is parsed once; its node count is memoized on that instance.
        NodeAst body = expansion.ParseBody(sql);

        // Charged before anything is copied: the rewrite below is what allocates.
        expansion.Charge(name, CountBodyNodes(body));

        // The stored body names its relations by immutable id, so the ids are resolved against the
        // current schema before anything binds against them. Skipped outright for a body that
        // carries none, which is every body written before ids were stored. Memoized per body and
        // schema version, so repeated references to one view do not resolve it again.
        if (DDL.StoredBodyBinder.MayContainReferences(sql))
            body = ResolveBody(schema, body, name);

        expansion.Stack.Add(view.Id);
        try
        {
            NodeAst expandedBody = Rewrite(schema, body, expansion);

            // The owner rides with the expansion. It has to: by the time anything opens a relation the
            // view's name is gone, so this derived table is the only remaining evidence that these
            // sources are being read on somebody else's behalf rather than the caller's.
            //
            // So does the view's own identity (rightAst: yytext = id, leftAst = name). The binder
            // re-checks a view reached from inside another view's body against the principal that body
            // runs as — the enclosing view's owner — on every read, as PostgreSQL does. Without it, a
            // grant revoked from that owner after the enclosing view was created would never be seen.
            NodeAst viewIdentity = new(
                NodeType.Identifier,
                new NodeAst(NodeType.Identifier, null, null, null, null, null, null, null, name),
                null, null, null, null, null, null, view.Id);

            NodeAst ownerNode = new(
                NodeType.Identifier,
                view.Definition?.OwnerId is { Length: > 0 }
                    ? new NodeAst(NodeType.Identifier, null, null, null, null, null, null, null, view.Definition.Owner)
                    : null,
                viewIdentity, null, null, null, null, null,
                view.Definition?.OwnerId is { Length: > 0 } ownerId ? ownerId : null);

            return new NodeAst(
                NodeType.DerivedTableReference,
                expandedBody,
                new NodeAst(NodeType.Identifier, null, null, null, null, null, null, null, alias ?? name),
                null, ownerNode, null, null, null, null);
        }
        finally
        {
            expansion.Stack.RemoveAt(expansion.Stack.Count - 1);
        }
    }
}
