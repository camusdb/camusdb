
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// Keeps stored view bodies valid across changes to the relations they read: rewrites dependent
/// views when a base relation is renamed, and refuses a drop that would orphan one.
///
/// <para><b>Why a rewrite is needed at all.</b> PostgreSQL stores a view as a parse tree keyed by
/// OID, so renaming a table leaves every dependent view working and merely changes what
/// <c>pg_get_viewdef</c> prints. CamusDB stores a view as text, so the same rename would leave the
/// text naming something that no longer exists and the view would fail at its next read. Producing
/// PostgreSQL's behavior therefore has to be explicit: find the dependents by id — ids are immutable,
/// which is the whole reason dependencies are recorded as ids — re-parse each body, swap the
/// identifier in the AST, and re-render.</para>
///
/// <para>The swap is an <b>AST edit</b>, never a string substitution. A textual replace would
/// happily rewrite a matching word inside a string literal, a column name, or an unrelated table's
/// alias, and the damage would not surface until someone read the view.</para>
/// </summary>
internal static class ViewDependencyMaintainer
{
    /// <summary>
    /// Refuses a drop that would leave a view reading a relation that no longer exists.
    /// </summary>
    /// <remarks>
    /// This makes <c>DROP TABLE</c> stricter than it was before views existed, which is deliberate
    /// and matches PostgreSQL: the alternative is a table drop that silently converts every dependent
    /// view into a delayed error for whoever reads it next. <c>CASCADE</c> is the escape hatch, and it
    /// is spelled out in the message.
    /// </remarks>
    public static void RequireNoDependentViews(Schema schema, string relationName, string relationId, bool cascade)
    {
        if (cascade)
            return;

        List<string> dependents = ViewDependencyGraph.DirectDependentsOfTable(schema, relationId);

        if (dependents.Count == 0)
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.DependentObjectsExist,
            $"Cannot drop table '{relationName}' because other objects depend on it: " +
            $"{string.Join(", ", dependents)}. Use DROP TABLE ... CASCADE to drop them too.");
    }

    /// <summary>
    /// Rewrites every view that reads <paramref name="relationId"/> so its body names
    /// <paramref name="newName"/> instead of <paramref name="oldName"/>.
    /// </summary>
    /// <remarks>
    /// <para>Each rewrite is proposed as its own <c>SetViewDefinition</c> delta, applied after the
    /// rename itself. That leaves a window in which the rename has landed but a given view's rewrite
    /// has not, during which reading that view fails with "table does not exist". The window is small
    /// and — this is the part that matters — it <b>cannot produce a wrong answer</b>: the stale body
    /// names a relation that is simply gone, so the read errors rather than silently resolving to
    /// something else. Folding the rename and every rewrite into one delta would close the window
    /// entirely and is the better end state; it is not required for correctness of the data.</para>
    ///
    /// <para>Idempotent: a body that already names <paramref name="newName"/> renders identically and
    /// is skipped, so a replay or a partially-completed rewrite can be re-run safely.</para>
    /// </remarks>
    /// <summary>
    /// Builds the rewritten body of every view that reads <paramref name="relationId"/>, so a rename
    /// can carry them in its own delta. Returns null when nothing depends on the relation.
    /// </summary>
    /// <remarks>
    /// Computed against one schema snapshot and applied with the rename as a single transition. The
    /// alternative — rename first, rewrite afterwards — leaves a window in which a stale body names a
    /// relation that no longer exists; and because the old name is free the moment the rename commits,
    /// anything that creates a relation under it during that window makes the stale body resolve to
    /// <em>that</em> relation and return its rows. A wrong answer, not an outage.
    /// </remarks>
    public static Dictionary<string, ViewDefinition>? BuildRenameRewrites(
        Schema schema,
        string relationId,
        string oldName,
        string newName,
        Func<string, NodeAst> parse)
    {
        Dictionary<string, ViewDefinition>? rewrites = null;

        // Both edge kinds. A dependent records the relation it reads in DependsOnTableIds or in
        // DependsOnViewIds depending on what that relation is, and a rename has to reach the
        // dependents either way — consulting only one list silently skips every view that reads a
        // renamed *view*, which is exactly the case the stale body would then resolve wrongly.
        IEnumerable<string> dependents = ViewDependencyGraph
            .DirectDependentsOfTable(schema, relationId)
            .Concat(ViewDependencyGraph.DirectDependentsOfView(schema, relationId))
            .Distinct(StringComparer.OrdinalIgnoreCase);

        foreach (string viewName in dependents)
        {
            if (!schema.Views.TryGetValue(viewName, out ViewSchema? view) || view.Definition is null)
                continue;

            NodeAst body = parse(view.Definition.Sql);
            NodeAst rewritten = RenameRelationInFrom(body, oldName, newName);

            if (ReferenceEquals(rewritten, body))
                continue;

            rewrites ??= new(StringComparer.OrdinalIgnoreCase);
            rewrites[viewName] = new ViewDefinition
            {
                Sql = ViewBodyRenderer.RenderSelect(rewritten),
                Columns = view.Definition.Columns,
                DependsOnTableIds = view.Definition.DependsOnTableIds,
                DependsOnViewIds = view.Definition.DependsOnViewIds,
                CheckOption = view.Definition.CheckOption,
                Owner = view.Definition.Owner,
                // Carried explicitly: dropping it would silently strip the view's ownership, and an
                // owner that no longer resolves fails every read of the view.
                OwnerId = view.Definition.OwnerId,
            };
        }

        return rewrites;
    }

    /// <summary>
    /// Returns the tree with every FROM-position reference to <paramref name="oldName"/> renamed, or
    /// the same instance when nothing matched.
    /// </summary>
    /// <remarks>
    /// Only FROM positions are touched. A column that happens to share the table's name, and a string
    /// literal containing it, are left alone — which is exactly what a textual rewrite would get
    /// wrong.
    /// </remarks>
    internal static NodeAst RenameRelationInFrom(NodeAst node, string oldName, string newName)
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
            right = RenameInFromClause(right, oldName, newName);
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

        NodeAst? Recurse(NodeAst? child) => child is null ? null : RenameRelationInFrom(child, oldName, newName);
    }

    private static NodeAst RenameInFromClause(NodeAst from, string oldName, string newName)
    {
        switch (from.nodeType)
        {
            case NodeType.Identifier:
                return string.Equals(from.yytext, oldName, StringComparison.OrdinalIgnoreCase)
                    ? new NodeAst(NodeType.Identifier, null, null, null, null, null, null, null, newName)
                    : from;

            case NodeType.TableReference:
                {
                    if (!string.Equals(from.leftAst?.yytext, oldName, StringComparison.OrdinalIgnoreCase))
                        return from;

                    // The alias is preserved deliberately. Every qualified column reference in the
                    // body resolves through the alias, so changing it here would break the very body
                    // this rewrite exists to keep working. A table with no alias gets one — its old
                    // name — for the same reason.
                    NodeAst alias = from.rightAst
                        ?? new NodeAst(NodeType.Identifier, null, null, null, null, null, null, null, oldName);

                    return new NodeAst(
                        NodeType.TableReference,
                        new NodeAst(NodeType.Identifier, null, null, null, null, null, null, null, newName),
                        alias,
                        from.extendedOne, null, null, null, null, null);
                }

            case NodeType.DerivedTableReference:
                {
                    NodeAst inner = RenameRelationInFrom(from.leftAst!, oldName, newName);
                    return ReferenceEquals(inner, from.leftAst)
                        ? from
                        : new NodeAst(NodeType.DerivedTableReference, inner, from.rightAst, null, null, null, null, null, null);
                }

            case NodeType.Join:
                {
                    NodeAst l = RenameInFromClause(from.leftAst!, oldName, newName);
                    NodeAst r = RenameInFromClause(from.rightAst!, oldName, newName);
                    NodeAst? on = from.extendedOne is null ? null : RenameRelationInFrom(from.extendedOne, oldName, newName);

                    return ReferenceEquals(l, from.leftAst) && ReferenceEquals(r, from.rightAst) && ReferenceEquals(on, from.extendedOne)
                        ? from
                        : new NodeAst(NodeType.Join, l, r, on, null, null, null, null, null);
                }

            case NodeType.CommaJoin:
            case NodeType.CommaJoinTableList:
                {
                    NodeAst l = RenameInFromClause(from.leftAst!, oldName, newName);
                    NodeAst r = RenameInFromClause(from.rightAst!, oldName, newName);

                    return ReferenceEquals(l, from.leftAst) && ReferenceEquals(r, from.rightAst)
                        ? from
                        : new NodeAst(from.nodeType, l, r, null, null, null, null, null, null);
                }

            default:
                return from;
        }
    }
}
