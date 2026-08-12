
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
/// The column half of a stored body's binding: rewrites table-column references to
/// <see cref="StoredColumnRef"/> tokens on the way in, and back to current names on the way out.
///
/// <para>Both directions run over a body whose FROM clauses name their relations, never one whose
/// relations are still tokens. <see cref="StoredBodyBinder"/> orders the two passes to guarantee
/// that — columns are bound before relations are, and relations are resolved before columns are —
/// so the scope built here is always a map from an alias to a relation it can look up by name.</para>
///
/// <para><b>What is deliberately not bound.</b> A reference this cannot attribute to exactly one
/// relation in scope is left as a name, and so is any reference in <c>ORDER BY</c>, <c>GROUP BY</c>
/// or <c>HAVING</c> that matches one of the select's own output names — those slots may legally name
/// an output alias rather than a column, and binding an alias to whatever base column happens to
/// share its name would change what the body means the next time that column is renamed. A
/// reference left as a name still behaves exactly as it did before any of this existed, and is
/// still covered by the rename refusal, so the conservative path is never the unsafe one.</para>
/// </summary>
internal static class StoredBodyColumns
{
    /// <summary>Rewrites resolvable table-column references to id tokens.</summary>
    public static NodeAst Bind(Schema schema, NodeAst body) => Walk(schema, body, null, viewName: null);

    /// <summary>
    /// Rewrites id tokens back to the names their columns currently answer to.
    /// </summary>
    /// <exception cref="CamusDBException">
    /// When a token names a column no relation in scope has. Failing closed rather than leaving the
    /// token in place: the token would otherwise reach the binder as an unknown column and produce an
    /// error about engine-internal text that no user wrote.
    /// </exception>
    public static NodeAst Resolve(Schema schema, NodeAst body, string viewName)
        => Walk(schema, body, null, viewName);

    /// <summary>
    /// One <c>SELECT</c>'s visible relations, plus the names it publishes. Aliases bound to a derived
    /// table or a view are recorded as non-tables so a reference through them is left alone.
    /// </summary>
    private sealed class Scope
    {
        public Scope(Scope? outer) => Outer = outer;

        public Scope? Outer { get; }

        public Dictionary<string, TableSchema> Tables { get; } = new(StringComparer.OrdinalIgnoreCase);

        public HashSet<string> NonTables { get; } = new(StringComparer.OrdinalIgnoreCase);

        /// <summary>Output names of this select, which its own ORDER BY / GROUP BY / HAVING may name.</summary>
        public HashSet<string> OutputNames { get; } = new(StringComparer.OrdinalIgnoreCase);
    }

    private static NodeAst Walk(Schema schema, NodeAst node, Scope? outer, string? viewName)
    {
        if (node.nodeType != NodeType.Select)
            return Rewrite(schema, node, outer, viewName, aliasSlot: false);

        Scope scope = new(outer);

        if (node.rightAst is not null)
            CollectSources(schema, node.rightAst, scope);

        CollectOutputNames(node.leftAst, scope);

        NodeAst? left = Rewrite(schema, node.leftAst, scope, viewName, aliasSlot: false);
        NodeAst? from = node.rightAst is null ? null : RewriteFrom(schema, node.rightAst, scope, viewName);
        NodeAst? where = Rewrite(schema, node.extendedOne, scope, viewName, aliasSlot: false);
        NodeAst? orderBy = Rewrite(schema, node.extendedTwo, scope, viewName, aliasSlot: true);
        NodeAst? limit = Rewrite(schema, node.extendedThree, scope, viewName, aliasSlot: false);
        NodeAst? offset = Rewrite(schema, node.extendedFour, scope, viewName, aliasSlot: false);
        NodeAst? groupBy = Rewrite(schema, node.extendedFive, scope, viewName, aliasSlot: true);
        NodeAst? having = Rewrite(schema, node.extendedSix, scope, viewName, aliasSlot: true);

        if (ReferenceEquals(left, node.leftAst) && ReferenceEquals(from, node.rightAst) &&
            ReferenceEquals(where, node.extendedOne) && ReferenceEquals(orderBy, node.extendedTwo) &&
            ReferenceEquals(limit, node.extendedThree) && ReferenceEquals(offset, node.extendedFour) &&
            ReferenceEquals(groupBy, node.extendedFive) && ReferenceEquals(having, node.extendedSix))
            return node;

        return new NodeAst(
            NodeType.Select, left, from, where, orderBy, limit, offset, groupBy, node.yytext,
            having, node.extendedSeven);
    }

    /// <summary>
    /// Registers what each alias in a FROM clause refers to. Derived tables are walked separately so
    /// their own body is rewritten in its own scope.
    /// </summary>
    private static void CollectSources(Schema schema, NodeAst from, Scope scope)
    {
        switch (from.nodeType)
        {
            case NodeType.Identifier:
                Register(schema, from.yytext, from.yytext, scope);
                return;

            case NodeType.IdentifierWithOpts:
                Register(schema, from.leftAst?.yytext, from.leftAst?.yytext, scope);
                return;

            case NodeType.TableReference:
                Register(schema, from.leftAst?.yytext, from.rightAst?.yytext ?? from.leftAst?.yytext, scope);
                return;

            case NodeType.DerivedTableReference:
                if (from.rightAst?.yytext is { Length: > 0 } alias)
                    scope.NonTables.Add(alias);
                return;

            case NodeType.Join:
            case NodeType.CommaJoin:
            case NodeType.CommaJoinTableList:
                if (from.leftAst is not null) CollectSources(schema, from.leftAst, scope);
                if (from.rightAst is not null) CollectSources(schema, from.rightAst, scope);
                return;
        }
    }

    private static void Register(Schema schema, string? relationName, string? alias, Scope scope)
    {
        if (relationName is null || alias is null)
            return;

        if (schema.Tables.TryGetValue(relationName, out TableSchema? table))
            scope.Tables[alias] = table;
        else
            scope.NonTables.Add(alias);
    }

    private static void CollectOutputNames(NodeAst? projections, Scope scope)
    {
        if (projections is null)
            return;

        if (projections.nodeType is NodeType.IdentifierList or NodeType.ExprList)
        {
            CollectOutputNames(projections.leftAst, scope);
            CollectOutputNames(projections.rightAst, scope);
            return;
        }

        if (projections.nodeType == NodeType.ExprAlias && projections.rightAst?.yytext is { Length: > 0 } alias)
            scope.OutputNames.Add(alias);
    }

    /// <summary>
    /// Rewrites the FROM clause: only a derived table has anything to rewrite, and it becomes its own
    /// scope with this one as its outer so a correlated reference still resolves.
    /// </summary>
    private static NodeAst RewriteFrom(Schema schema, NodeAst from, Scope scope, string? viewName)
    {
        switch (from.nodeType)
        {
            case NodeType.DerivedTableReference:
                {
                    NodeAst inner = Walk(schema, from.leftAst!, scope, viewName);
                    return ReferenceEquals(inner, from.leftAst)
                        ? from
                        : new NodeAst(NodeType.DerivedTableReference, inner, from.rightAst, null, null, null, null, null, null);
                }

            case NodeType.Join:
                {
                    NodeAst l = RewriteFrom(schema, from.leftAst!, scope, viewName);
                    NodeAst r = RewriteFrom(schema, from.rightAst!, scope, viewName);
                    NodeAst? on = Rewrite(schema, from.extendedOne, scope, viewName, aliasSlot: false);

                    return ReferenceEquals(l, from.leftAst) && ReferenceEquals(r, from.rightAst) && ReferenceEquals(on, from.extendedOne)
                        ? from
                        : new NodeAst(NodeType.Join, l, r, on, null, null, null, null, null);
                }

            case NodeType.CommaJoin:
            case NodeType.CommaJoinTableList:
                {
                    NodeAst l = RewriteFrom(schema, from.leftAst!, scope, viewName);
                    NodeAst r = RewriteFrom(schema, from.rightAst!, scope, viewName);

                    return ReferenceEquals(l, from.leftAst) && ReferenceEquals(r, from.rightAst)
                        ? from
                        : new NodeAst(from.nodeType, l, r, null, null, null, null, null, null);
                }

            default:
                return from;
        }
    }

    /// <summary>
    /// Rewrites an expression tree. <paramref name="aliasSlot"/> marks the clauses that may name one
    /// of the select's own output columns instead of a relation's.
    /// </summary>
    private static NodeAst? Rewrite(Schema schema, NodeAst? node, Scope? scope, string? viewName, bool aliasSlot)
    {
        if (node is null)
            return null;

        // A nested select is a scope of its own, with this one as its outer.
        if (node.nodeType == NodeType.Select)
            return Walk(schema, node, scope, viewName);

        if (node.nodeType == NodeType.Identifier && scope is not null && node.yytext is { Length: > 0 } identifier)
        {
            string? rewritten = RewriteIdentifier(identifier, scope, viewName, aliasSlot);

            if (rewritten is not null)
                return new NodeAst(NodeType.Identifier, null, null, null, null, null, null, null, rewritten);

            return node;
        }

        // The right side of an alias names the output, not a column of any relation.
        if (node.nodeType == NodeType.ExprAlias)
        {
            NodeAst? inner = Rewrite(schema, node.leftAst, scope, viewName, aliasSlot);

            return ReferenceEquals(inner, node.leftAst)
                ? node
                : new NodeAst(NodeType.ExprAlias, inner, node.rightAst, null, null, null, null, null, null);
        }

        NodeAst? left = Rewrite(schema, node.leftAst, scope, viewName, aliasSlot);
        NodeAst? right = Rewrite(schema, node.rightAst, scope, viewName, aliasSlot);
        NodeAst? one = Rewrite(schema, node.extendedOne, scope, viewName, aliasSlot);
        NodeAst? two = Rewrite(schema, node.extendedTwo, scope, viewName, aliasSlot);
        NodeAst? three = Rewrite(schema, node.extendedThree, scope, viewName, aliasSlot);
        NodeAst? four = Rewrite(schema, node.extendedFour, scope, viewName, aliasSlot);
        NodeAst? five = Rewrite(schema, node.extendedFive, scope, viewName, aliasSlot);
        NodeAst? six = Rewrite(schema, node.extendedSix, scope, viewName, aliasSlot);

        if (ReferenceEquals(left, node.leftAst) && ReferenceEquals(right, node.rightAst) &&
            ReferenceEquals(one, node.extendedOne) && ReferenceEquals(two, node.extendedTwo) &&
            ReferenceEquals(three, node.extendedThree) && ReferenceEquals(four, node.extendedFour) &&
            ReferenceEquals(five, node.extendedFive) && ReferenceEquals(six, node.extendedSix))
            return node;

        return new NodeAst(
            node.nodeType, left, right, one, two, three, four, five, node.yytext, six, node.extendedSeven);
    }

    /// <summary>
    /// The replacement text for one identifier, or null to leave it exactly as it is.
    /// <paramref name="viewName"/> non-null selects the token-to-name direction.
    /// </summary>
    private static string? RewriteIdentifier(string identifier, Scope scope, string? viewName, bool aliasSlot)
    {
        int dot = identifier.LastIndexOf('.');
        string qualifier = dot > 0 ? identifier[..dot] : "";
        string bare = dot > 0 ? identifier[(dot + 1)..] : identifier;

        string? replacement = viewName is null
            ? BindColumn(bare, qualifier, scope, aliasSlot)
            : ResolveColumn(bare, qualifier, scope, viewName);

        if (replacement is null)
            return null;

        return dot > 0 ? qualifier + "." + replacement : replacement;
    }

    private static string? BindColumn(string columnName, string qualifier, Scope scope, bool aliasSlot)
    {
        // Already a token: a body being re-bound after a partial conversion.
        if (StoredColumnRef.TryGetColumnId(columnName, out _))
            return null;

        // In a clause that may name an output column, a match against this select's own output names
        // is left alone — it is not necessarily a relation's column at all.
        if (aliasSlot && qualifier.Length == 0 && scope.OutputNames.Contains(columnName))
            return null;

        TableColumnSchema? column = FindColumn(columnName, qualifier, scope);

        return column?.Id is { Length: > 0 } id ? StoredColumnRef.Format(id) : null;
    }

    private static string? ResolveColumn(string token, string qualifier, Scope scope, string viewName)
    {
        if (!StoredColumnRef.TryGetColumnId(token, out string columnId))
            return null;

        for (Scope? current = scope; current is not null; current = current.Outer)
        {
            // A qualified reference can only mean the relation its qualifier names.
            if (qualifier.Length > 0)
            {
                if (current.NonTables.Contains(qualifier))
                    break;

                if (current.Tables.TryGetValue(qualifier, out TableSchema? qualified))
                {
                    if (FindColumnById(qualified, columnId) is { Name: { Length: > 0 } qualifiedName })
                        return qualifiedName;

                    break;
                }

                continue;
            }

            foreach (TableSchema table in current.Tables.Values)
            {
                if (FindColumnById(table, columnId) is { Name: { Length: > 0 } name })
                    return name;
            }
        }

        throw new CamusDBException(
            CamusDBErrorCodes.SystemSpaceCorrupt,
            $"The definition of view '{viewName}' reads a column that no longer exists (id '{columnId}')");
    }

    private static TableColumnSchema? FindColumn(string columnName, string qualifier, Scope scope)
    {
        for (Scope? current = scope; current is not null; current = current.Outer)
        {
            if (qualifier.Length > 0)
            {
                if (current.NonTables.Contains(qualifier))
                    return null;

                if (current.Tables.TryGetValue(qualifier, out TableSchema? qualified))
                    return FindColumnByName(qualified, columnName);

                continue;
            }

            TableColumnSchema? match = null;

            foreach (TableSchema table in current.Tables.Values)
            {
                if (FindColumnByName(table, columnName) is not { } candidate)
                    continue;

                // Two relations in scope expose this name. The body was bound before it reached
                // here, so the binder resolved it somehow — but this analysis cannot tell how, and
                // binding it to the wrong one would silently change what the view reads.
                if (match is not null)
                    return null;

                match = candidate;
            }

            if (match is not null)
                return match;
        }

        return null;
    }

    private static TableColumnSchema? FindColumnByName(TableSchema table, string columnName)
    {
        foreach (TableColumnSchema column in table.Columns ?? [])
        {
            if (string.Equals(column.Name, columnName, StringComparison.OrdinalIgnoreCase))
                return column;
        }

        return null;
    }

    private static TableColumnSchema? FindColumnById(TableSchema table, string columnId)
    {
        foreach (TableColumnSchema column in table.Columns ?? [])
        {
            if (string.Equals(column.Id, columnId, StringComparison.Ordinal))
                return column;
        }

        return null;
    }
}
