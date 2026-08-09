
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
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
    public static NodeAst Expand(Schema schema, NodeAst ast, int maxDepth, Func<string, NodeAst> parseBody)
    {
        // The overwhelmingly common case is a statement with no views at all; a whole-tree walk that
        // allocates nothing when there is nothing to do keeps this off the hot path's conscience.
        if (schema.Views.Count == 0)
            return ast;

        return Rewrite(schema, ast, [], maxDepth, parseBody);
    }

    private static NodeAst Rewrite(
        Schema schema,
        NodeAst node,
        List<string> expansionStack,
        int maxDepth,
        Func<string, NodeAst> parseBody)
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
            right = RewriteFrom(schema, right, expansionStack, maxDepth, parseBody);

        left = RewriteChild(schema, left, expansionStack, maxDepth, parseBody);
        one = RewriteChild(schema, one, expansionStack, maxDepth, parseBody);
        two = RewriteChild(schema, two, expansionStack, maxDepth, parseBody);
        three = RewriteChild(schema, three, expansionStack, maxDepth, parseBody);
        four = RewriteChild(schema, four, expansionStack, maxDepth, parseBody);
        five = RewriteChild(schema, five, expansionStack, maxDepth, parseBody);
        six = RewriteChild(schema, six, expansionStack, maxDepth, parseBody);

        if (node.nodeType != NodeType.Select)
            right = RewriteChild(schema, right, expansionStack, maxDepth, parseBody);

        if (ReferenceEquals(left, node.leftAst) && ReferenceEquals(right, node.rightAst) &&
            ReferenceEquals(one, node.extendedOne) && ReferenceEquals(two, node.extendedTwo) &&
            ReferenceEquals(three, node.extendedThree) && ReferenceEquals(four, node.extendedFour) &&
            ReferenceEquals(five, node.extendedFive) && ReferenceEquals(six, node.extendedSix))
            return node;

        return new NodeAst(
            node.nodeType, left, right, one, two, three, four, five, node.yytext, six, node.extendedSeven);
    }

    private static NodeAst? RewriteChild(
        Schema schema, NodeAst? child, List<string> expansionStack, int maxDepth, Func<string, NodeAst> parseBody)
        => child is null ? null : Rewrite(schema, child, expansionStack, maxDepth, parseBody);

    /// <summary>
    /// Rewrites the FROM clause: substitutes any relation name that resolves to a view, and recurses
    /// through joins and derived tables.
    /// </summary>
    private static NodeAst RewriteFrom(
        Schema schema, NodeAst from, List<string> expansionStack, int maxDepth, Func<string, NodeAst> parseBody)
    {
        switch (from.nodeType)
        {
            case NodeType.Identifier:
                // A bare identifier in FROM position carries no alias of its own, so the view's name
                // becomes the alias — that is what keeps `SELECT v.col FROM v` resolving.
                return TrySubstitute(schema, from.yytext, from.yytext, expansionStack, maxDepth, parseBody) ?? from;

            case NodeType.TableReference:
                {
                    string? name = from.leftAst?.yytext;
                    string? alias = from.rightAst?.yytext ?? name;

                    NodeAst? substituted = TrySubstitute(schema, name, alias, expansionStack, maxDepth, parseBody);
                    if (substituted is not null)
                    {
                        // An index hint targets a physical table's indexes. A view has none of its
                        // own, and silently applying the hint to whatever the body happens to scan
                        // would pin a plan the user never asked for on a relation they cannot see.
                        if (from.extendedOne is not null)
                            throw new CamusDBException(
                                CamusDBErrorCodes.InvalidInput,
                                $"An index or cache hint cannot be applied to view '{name}': a view has no " +
                                "indexes of its own, and the hint would silently target a relation the " +
                                "statement does not name");

                        return substituted;
                    }

                    return from;
                }

            case NodeType.DerivedTableReference:
                {
                    NodeAst inner = Rewrite(schema, from.leftAst!, expansionStack, maxDepth, parseBody);
                    return ReferenceEquals(inner, from.leftAst)
                        ? from
                        : new NodeAst(NodeType.DerivedTableReference, inner, from.rightAst, null, null, null, null, null, null);
                }

            case NodeType.Join:
                {
                    NodeAst l = RewriteFrom(schema, from.leftAst!, expansionStack, maxDepth, parseBody);
                    NodeAst r = RewriteFrom(schema, from.rightAst!, expansionStack, maxDepth, parseBody);
                    NodeAst? on = RewriteChild(schema, from.extendedOne, expansionStack, maxDepth, parseBody);

                    return ReferenceEquals(l, from.leftAst) && ReferenceEquals(r, from.rightAst) && ReferenceEquals(on, from.extendedOne)
                        ? from
                        : new NodeAst(NodeType.Join, l, r, on, null, null, null, null, null);
                }

            case NodeType.CommaJoin:
            case NodeType.CommaJoinTableList:
                {
                    NodeAst l = RewriteFrom(schema, from.leftAst!, expansionStack, maxDepth, parseBody);
                    NodeAst r = RewriteFrom(schema, from.rightAst!, expansionStack, maxDepth, parseBody);

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
    private static NodeAst? TrySubstitute(
        Schema schema,
        string? name,
        string? alias,
        List<string> expansionStack,
        int maxDepth,
        Func<string, NodeAst> parseBody)
    {
        if (name is null || !schema.Views.TryGetValue(name, out ViewSchema? view))
            return null;

        if (view.Definition?.Sql is not { Length: > 0 } sql || view.Id is null)
            throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"View '{name}' has no stored definition");

        // The DDL-time acyclicity check is the real defense; this catches a cycle that reached the
        // stored schema anyway (a hand-edited checkpoint, a partially-applied log) rather than
        // recursing until the stack gives out.
        if (expansionStack.Contains(view.Id, StringComparer.Ordinal))
            throw new CamusDBException(
                CamusDBErrorCodes.ViewRecursionDetected,
                $"Infinite recursion detected in the definition of view '{name}'");

        if (expansionStack.Count >= maxDepth)
            throw new CamusDBException(
                CamusDBErrorCodes.ViewRecursionDetected,
                $"View expansion for '{name}' exceeded the maximum nesting depth of {maxDepth}");

        NodeAst body = parseBody(sql);

        expansionStack.Add(view.Id);
        try
        {
            NodeAst expandedBody = Rewrite(schema, body, expansionStack, maxDepth, parseBody);

            return new NodeAst(
                NodeType.DerivedTableReference,
                expandedBody,
                new NodeAst(NodeType.Identifier, null, null, null, null, null, null, null, alias ?? name),
                null, null, null, null, null, null);
        }
        finally
        {
            expansionStack.Remove(view.Id);
        }
    }
}
