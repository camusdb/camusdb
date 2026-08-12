
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
/// Works out which <b>table columns</b> a view body reads, as immutable column ids, so DDL that
/// would break the view can be refused instead of succeeding and leaving it broken.
///
/// <para><b>Why only table columns.</b> They are the only ones whose name can move. A view's own
/// output columns cannot be renamed — <c>CREATE OR REPLACE VIEW</c> may only append, and existing
/// names must be preserved — and a derived table's output names are internal to the body that
/// declares them. So a reference to a view or to a derived table is already stable by name and is
/// deliberately not recorded here.</para>
///
/// <para><b>Deliberately conservative.</b> A reference this cannot resolve with certainty records
/// nothing. Being wrong in that direction means a column drop that should have been refused is
/// allowed — which is exactly what happens today, so nothing regresses. Being wrong in the other
/// direction would refuse legitimate DDL over a column no view actually reads, which is worse: it
/// blocks work the user is entitled to do, and the user has no way to see why. This is a narrowing
/// of the gap, not a proof of its closure.</para>
/// </summary>
internal static class ViewColumnDependencyAnalyzer
{
    /// <summary>
    /// The ids of every table column <paramref name="body"/> reads, resolved against
    /// <paramref name="schema"/>. Empty when nothing could be resolved.
    /// </summary>
    /// <remarks>
    /// Run over the body <b>before</b> its relations are bound to ids, because resolution works from
    /// the names the FROM clause still carries.
    /// </remarks>
    public static List<string> Collect(Schema schema, NodeAst body)
    {
        HashSet<string> columnIds = new(StringComparer.Ordinal);

        Walk(schema, body, outer: null, columnIds);

        return [.. columnIds];
    }

    /// <summary>
    /// One <c>SELECT</c>'s worth of visible relations: what each alias refers to, and which aliases
    /// are derived tables (whose columns are not table columns and are therefore not recorded).
    /// </summary>
    private sealed class Scope
    {
        public Scope(Scope? outer) => Outer = outer;

        /// <summary>The enclosing query's scope, consulted for correlated references.</summary>
        public Scope? Outer { get; }

        /// <summary>Alias — or the relation's own name when it has none — to the table it names.</summary>
        public Dictionary<string, TableSchema> Tables { get; } = new(StringComparer.OrdinalIgnoreCase);

        /// <summary>Aliases bound to something that is not a table: a derived table, or a view.</summary>
        public HashSet<string> NonTables { get; } = new(StringComparer.OrdinalIgnoreCase);
    }

    private static void Walk(Schema schema, NodeAst node, Scope? outer, HashSet<string> columnIds)
    {
        if (node.nodeType != NodeType.Select)
        {
            foreach (NodeAst? child in Children(node))
            {
                if (child is not null)
                    Walk(schema, child, outer, columnIds);
            }

            return;
        }

        Scope scope = new(outer);

        if (node.rightAst is not null)
            CollectSources(schema, node.rightAst, scope, columnIds);

        // Every slot except FROM holds expressions, and an identifier in one of them is a column
        // reference. FROM was consumed above; walking it again would read relation names as columns.
        CollectColumns(schema, node.leftAst, scope, columnIds);
        CollectColumns(schema, node.extendedOne, scope, columnIds);
        CollectColumns(schema, node.extendedTwo, scope, columnIds);
        CollectColumns(schema, node.extendedThree, scope, columnIds);
        CollectColumns(schema, node.extendedFour, scope, columnIds);
        CollectColumns(schema, node.extendedFive, scope, columnIds);
        CollectColumns(schema, node.extendedSix, scope, columnIds);
    }

    /// <summary>
    /// Records what each alias in a FROM clause refers to, and walks a derived table's own query as
    /// a nested scope.
    /// </summary>
    private static void CollectSources(Schema schema, NodeAst from, Scope scope, HashSet<string> columnIds)
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
                if (from.rightAst?.yytext is { Length: > 0 } derivedAlias)
                    scope.NonTables.Add(derivedAlias);

                // The subquery is its own scope, and it may reference this query's relations.
                if (from.leftAst is not null)
                    Walk(schema, from.leftAst, scope, columnIds);
                return;

            case NodeType.Join:
                if (from.leftAst is not null) CollectSources(schema, from.leftAst, scope, columnIds);
                if (from.rightAst is not null) CollectSources(schema, from.rightAst, scope, columnIds);

                // ON is a predicate over the relations joined here, so it is resolved in this scope
                // once both sides are registered.
                CollectColumns(schema, from.extendedOne, scope, columnIds);
                return;

            case NodeType.CommaJoin:
            case NodeType.CommaJoinTableList:
                if (from.leftAst is not null) CollectSources(schema, from.leftAst, scope, columnIds);
                if (from.rightAst is not null) CollectSources(schema, from.rightAst, scope, columnIds);
                return;
        }
    }

    private static void Register(Schema schema, string? relationName, string? alias, Scope scope)
    {
        if (relationName is null || alias is null)
            return;

        // Materialized views count: they are stored relations with real, droppable columns, so a
        // body reading one has the same stake in those columns surviving as it does in a table's.
        if (schema.Tables.TryGetValue(relationName, out TableSchema? table))
            scope.Tables[alias] = table;
        else
            scope.NonTables.Add(alias);
    }

    /// <summary>
    /// Walks an expression tree, resolving each identifier against <paramref name="scope"/>. A
    /// nested <c>SELECT</c> — a scalar, EXISTS or IN subquery — becomes a scope of its own with this
    /// one as its outer, so a correlated reference still resolves.
    /// </summary>
    private static void CollectColumns(Schema schema, NodeAst? node, Scope scope, HashSet<string> columnIds)
    {
        if (node is null)
            return;

        if (node.nodeType == NodeType.Select)
        {
            Walk(schema, node, scope, columnIds);
            return;
        }

        if (node.nodeType == NodeType.Identifier && node.yytext is { Length: > 0 } identifier)
            Resolve(identifier, scope, columnIds);

        // An alias names the output, not a column of any relation, so the right side of an ExprAlias
        // is skipped. Recording it would resolve "total AS status" against a column called status.
        if (node.nodeType == NodeType.ExprAlias)
        {
            CollectColumns(schema, node.leftAst, scope, columnIds);
            return;
        }

        foreach (NodeAst? child in Children(node))
            CollectColumns(schema, child, scope, columnIds);
    }

    private static void Resolve(string identifier, Scope scope, HashSet<string> columnIds)
    {
        int dot = identifier.LastIndexOf('.');

        if (dot > 0)
        {
            string alias = identifier[..dot];
            string columnName = identifier[(dot + 1)..];

            // Qualified: exactly one relation can answer, and if the alias is a derived table or a
            // view there is no table column to record.
            for (Scope? current = scope; current is not null; current = current.Outer)
            {
                if (current.NonTables.Contains(alias))
                    return;

                if (current.Tables.TryGetValue(alias, out TableSchema? table))
                {
                    AddColumn(table, columnName, columnIds);
                    return;
                }
            }

            return;
        }

        // Unqualified: recorded only when exactly one visible table has a column of that name. The
        // body has already been bound, so a genuinely ambiguous reference would have been rejected
        // before reaching here; more than one match therefore means this analysis cannot tell which
        // relation the binder chose, and guessing would record a dependency on a column the view
        // does not read.
        for (Scope? current = scope; current is not null; current = current.Outer)
        {
            TableSchema? match = null;
            bool ambiguous = false;

            foreach (TableSchema table in current.Tables.Values)
            {
                if (FindColumn(table, identifier) is null)
                    continue;

                if (match is not null)
                {
                    ambiguous = true;
                    break;
                }

                match = table;
            }

            if (ambiguous)
                return;

            if (match is not null)
            {
                AddColumn(match, identifier, columnIds);
                return;
            }

            // Not in this scope: a derived table's or a view's output column, or an outer reference.
            // Only the outer-reference case can resolve, and the loop continues for it.
        }
    }

    private static void AddColumn(TableSchema table, string columnName, HashSet<string> columnIds)
    {
        if (FindColumn(table, columnName) is { Id: { Length: > 0 } id })
            columnIds.Add(id);
    }

    private static TableColumnSchema? FindColumn(TableSchema table, string columnName)
    {
        foreach (TableColumnSchema column in table.Columns ?? [])
        {
            if (string.Equals(column.Name, columnName, StringComparison.OrdinalIgnoreCase))
                return column;
        }

        return null;
    }

    private static IEnumerable<NodeAst?> Children(NodeAst node)
    {
        yield return node.leftAst;
        yield return node.rightAst;
        yield return node.extendedOne;
        yield return node.extendedTwo;
        yield return node.extendedThree;
        yield return node.extendedFour;
        yield return node.extendedFive;
        yield return node.extendedSix;
    }
}
