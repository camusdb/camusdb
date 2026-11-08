/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// The names one query level declares: the alias of each source and the column names those sources
/// have. Only names, so a level of a subquery can be checked without a binding that can run.
/// </summary>
internal sealed class NameScope
{
    public static readonly NameScope Empty = new([], []);

    private readonly HashSet<string> aliases = new(StringComparer.OrdinalIgnoreCase);

    private readonly HashSet<string> columns = new(StringComparer.OrdinalIgnoreCase);

    public NameScope(IReadOnlyList<BoundTableSource> tables, IReadOnlyList<BoundDerivedTableSource> derived)
    {
        foreach (BoundTableSource source in tables)
        {
            aliases.Add(source.Alias);

            foreach (TableColumnSchema column in source.Table.Schema.Columns ?? [])
                if (SchemaElementStateRules.IsReadable(column))
                    columns.Add(column.Name);
        }

        foreach (BoundDerivedTableSource source in derived)
        {
            aliases.Add(source.Alias);

            foreach (DerivedColumnSchema column in source.Columns)
                columns.Add(column.Name);
        }
    }

    public bool HasAlias(string alias) => aliases.Contains(alias);

    public bool HasColumn(string name) => columns.Contains(name);
}

/// <summary>
/// The query around a subquery, as <see cref="SubqueryScopeAnalyzer"/> reads it. The names come from
/// sources that are bound already, or from a schema-only bind of a source that is not bound yet. That
/// bind runs once, and only when a name of a subquery is not in the subquery: most subqueries name only
/// their own columns and never need it.
/// </summary>
internal sealed class OuterNameScope
{
    /// <summary>A statement position with no query around it, such as INSERT VALUES.</summary>
    public static readonly OuterNameScope None = new(NameScope.Empty);

    private readonly QueryBinder? binder;

    private readonly DatabaseDescriptor? database;

    private readonly QuerySource? source;

    private NameScope? scope;

    private OuterNameScope(NameScope scope) => this.scope = scope;

    private OuterNameScope(QueryBinder binder, DatabaseDescriptor database, QuerySource source)
    {
        this.binder = binder;
        this.database = database;
        this.source = source;
    }

    /// <summary>The names of sources that are bound already.</summary>
    public static OuterNameScope Of(IReadOnlyList<BoundTableSource> tables, IReadOnlyList<BoundDerivedTableSource> derived) =>
        new(new NameScope(tables, derived));

    /// <summary>The names of <paramref name="source"/>, bound for its schema when a name needs them.</summary>
    public static OuterNameScope Deferred(QueryBinder binder, DatabaseDescriptor database, QuerySource source) =>
        new(binder, database, source);

    public async ValueTask<NameScope> GetAsync()
    {
        if (scope is not null)
            return scope;

        scope = await binder!.BindNameScopeAsync(database!, source!).ConfigureAwait(false);
        return scope;
    }
}

/// <summary>
/// Finds the names of a subquery that resolve to the query around it. A name resolves to the nearest
/// query that has it, as in PostgreSQL: first the level of the subquery where the name is written, then
/// each level of the subquery around that one, and last the outer query. So a column that both the
/// subquery and the outer query have is the column of the subquery, and a column that only the outer
/// query has is an outer reference, also when the name has no alias. A subquery with an outer reference
/// is correlated: it cannot run once for the whole statement.
///
/// <para>A qualified name needs only the aliases of each level, which the statement text gives. A bare
/// name needs the columns of each level, so the analyzer binds the sources of that level for their
/// schema. That bind opens the tables and reads no row. It runs only for a level that has a bare name.</para>
///
/// <para>A name that no level has and the outer query does not have either is not an outer reference.
/// The bind of the subquery reports it, with the error that it always gave.</para>
///
/// <para>The analyzer does not enter a derived table of a subquery, so an outer reference inside a
/// derived table is not found. The bind of that derived table reports it as an unknown name.</para>
/// </summary>
internal sealed class SubqueryScopeAnalyzer
{
    private readonly QueryBinder binder;

    private readonly SelectQueryCreator selectQueryCreator = new();

    public SubqueryScopeAnalyzer(QueryBinder binder)
    {
        this.binder = binder;
    }

    /// <summary>The names of <paramref name="source"/>, which bind for their schema when a name needs them.</summary>
    public OuterNameScope DeferredScope(DatabaseDescriptor database, QuerySource source) =>
        OuterNameScope.Deferred(binder, database, source);

    /// <summary>
    /// One query level of the subquery tree: its query, the aliases that its FROM declares, and the
    /// names of its sources once a bare name needed them.
    /// </summary>
    private sealed class Level
    {
        public Level(SelectQuery query, Level? parent)
        {
            Query = query;
            Parent = parent;
            Aliases = DeclaredAliases(query.Source);
        }

        /// <summary>
        /// The aliases that a FROM declares. A set operation or a table function as the whole source
        /// declares none here: its names are its output columns, which a bare name finds.
        /// </summary>
        private static HashSet<string> DeclaredAliases(QuerySource source)
        {
            HashSet<string> aliases = new(StringComparer.OrdinalIgnoreCase);
            Stack<QuerySource> pending = new();
            pending.Push(source);

            while (pending.TryPop(out QuerySource? current))
            {
                switch (current)
                {
                    case TableSource table:
                        aliases.Add(table.Alias ?? table.TableName);
                        break;

                    case DerivedTableSource derived:
                        aliases.Add(derived.Alias);
                        break;

                    case JoinSource join:
                        pending.Push(join.Left);
                        pending.Push(join.Right);
                        break;
                }
            }

            return aliases;
        }

        public SelectQuery Query { get; }

        public Level? Parent { get; }

        public HashSet<string> Aliases { get; }

        public NameScope? Names { get; set; }
    }

    /// <summary>
    /// Returns the identifier nodes of <paramref name="subquery"/>, in any clause and in any subquery
    /// inside it, that resolve to <paramref name="outer"/>. An empty list means that the subquery is not
    /// correlated. The walk keeps its own stack, so a deep expression does not grow the call stack.
    /// </summary>
    public async ValueTask<IReadOnlyList<NodeAst>> FindOuterReferencesAsync(
        DatabaseDescriptor database,
        SelectQuery subquery,
        OuterNameScope outer)
    {
        List<NodeAst>? found = null;
        Stack<Level> levels = new();
        levels.Push(new Level(subquery, parent: null));

        List<(NodeAst Node, bool OutputNameIsLocal)> identifiers = [];
        List<NodeAst> nested = [];

        while (levels.TryPop(out Level? level))
        {
            identifiers.Clear();
            nested.Clear();
            CollectNames(level.Query, identifiers, nested);

            HashSet<string>? outputNames = null;

            foreach ((NodeAst node, bool outputNameIsLocal) in identifiers)
            {
                string name = node.yytext!;

                if (outputNameIsLocal && !name.Contains('.'))
                {
                    outputNames ??= OutputNames(level.Query);
                    if (outputNames.Contains(name))
                        continue;
                }

                if (await ResolvesOutsideAsync(database, level, name, outer).ConfigureAwait(false))
                    (found ??= []).Add(node);
            }

            foreach (NodeAst select in nested)
                levels.Push(new Level(selectQueryCreator.CreateSelectQuery(select), level));
        }

        return found ?? (IReadOnlyList<NodeAst>)[];
    }

    /// <summary>
    /// True when <paramref name="name"/>, written at <paramref name="level"/>, resolves to no level of
    /// the subquery and to the outer query.
    /// </summary>
    private async ValueTask<bool> ResolvesOutsideAsync(DatabaseDescriptor database, Level level, string name, OuterNameScope outer)
    {
        int dot = name.IndexOf('.');

        if (dot > 0 && dot < name.Length - 1)
        {
            string alias = name[..dot];

            for (Level? current = level; current is not null; current = current.Parent)
                if (current.Aliases.Contains(alias))
                    return false;

            return (await outer.GetAsync().ConfigureAwait(false)).HasAlias(alias);
        }

        for (Level? current = level; current is not null; current = current.Parent)
        {
            current.Names ??= await binder.BindNameScopeAsync(database, current.Query.Source).ConfigureAwait(false);

            if (current.Names.HasColumn(name))
                return false;
        }

        return (await outer.GetAsync().ConfigureAwait(false)).HasColumn(name);
    }

    /// <summary>The output names of the select list, which a bare name in ORDER BY or GROUP BY can name.</summary>
    private static HashSet<string> OutputNames(SelectQuery query)
    {
        HashSet<string> names = new(StringComparer.OrdinalIgnoreCase);

        foreach (ProjectionItem projection in query.Projections)
            if (projection.OutputName is { Length: > 0 } outputName)
                names.Add(outputName);

        return names;
    }

    /// <summary>
    /// Collects the identifier nodes that one level writes, and the SELECT nodes of the subqueries in its
    /// expressions. The clauses are the select list, WHERE, GROUP BY, HAVING, ORDER BY and each JOIN ON.
    /// A bare name of ORDER BY or GROUP BY can also name an output column, so those are marked.
    /// </summary>
    private static void CollectNames(SelectQuery query, List<(NodeAst, bool)> identifiers, List<NodeAst> nested)
    {
        foreach (ProjectionItem projection in query.Projections)
            CollectNames(projection.Expression, identifiers, nested, outputNameIsLocal: false);

        if (query.Where is not null)
            CollectNames(query.Where.Expression, identifiers, nested, outputNameIsLocal: false);

        if (query.GroupBy is not null)
            foreach (NodeAst expression in query.GroupBy)
                CollectNames(expression, identifiers, nested, outputNameIsLocal: true);

        if (query.Having is not null)
            CollectNames(query.Having.Expression, identifiers, nested, outputNameIsLocal: false);

        if (query.OrderBy is not null)
            foreach (OrderByItem item in query.OrderBy)
                CollectNames(item.Expression, identifiers, nested, outputNameIsLocal: true);

        Stack<QuerySource> sources = new();
        sources.Push(query.Source);

        while (sources.TryPop(out QuerySource? source))
        {
            if (source is not JoinSource join)
                continue;

            CollectNames(join.OnPredicate, identifiers, nested, outputNameIsLocal: false);
            sources.Push(join.Left);
            sources.Push(join.Right);
        }
    }

    /// <summary>
    /// The walk of one expression. It follows the slots that hold expressions: a function call keeps its
    /// name in <c>leftAst</c>, an alias keeps its output name in <c>rightAst</c>, a cast keeps its
    /// target type in <c>rightAst</c> and a quantified comparison keeps its operator in
    /// <c>extendedOne</c>, and none of those is a column.
    /// </summary>
    private static void CollectNames(NodeAst root, List<(NodeAst, bool)> identifiers, List<NodeAst> nested, bool outputNameIsLocal)
    {
        Stack<NodeAst> pending = new();
        pending.Push(root);

        while (pending.TryPop(out NodeAst? node))
        {
            switch (node.nodeType)
            {
                case NodeType.Identifier:
                    if (node.yytext is { Length: > 0 })
                        identifiers.Add((node, outputNameIsLocal));
                    continue;

                case NodeType.Select:
                    nested.Add(node);
                    continue;

                case NodeType.ExprFuncCall:
                    if (node.rightAst is not null)
                        pending.Push(node.rightAst);
                    continue;

                case NodeType.ExprAlias:
                case NodeType.ExprCast:
                    if (node.leftAst is not null)
                        pending.Push(node.leftAst);
                    continue;

                // The operator marker in extendedOne holds no name.
                case NodeType.ExprQuantifiedComparison:
                    if (node.leftAst is not null)
                        pending.Push(node.leftAst);
                    if (node.rightAst is not null)
                        pending.Push(node.rightAst);
                    continue;
            }

            foreach (NodeAst? child in (ReadOnlySpan<NodeAst?>)[
                node.leftAst, node.rightAst,
                node.extendedOne, node.extendedTwo, node.extendedThree, node.extendedFour,
                node.extendedFive, node.extendedSix, node.extendedSeven])
            {
                if (child is not null)
                    pending.Push(child);
            }
        }
    }
}
