
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

internal static class DerivedTableSchemaBuilder
{
    // -------------------------------------------------------------------------
    // Fixed schemas for SHOW commands — column order matches SchemaQuerier output.
    // -------------------------------------------------------------------------

    internal static readonly IReadOnlyList<DerivedColumnSchema> ShowDatabasesSchema =
    [
        new("Database", ColumnType.String),
    ];

    internal static readonly IReadOnlyList<DerivedColumnSchema> ShowTablesSchema =
    [
        new("tables", ColumnType.String),
    ];

    internal static readonly IReadOnlyList<DerivedColumnSchema> ShowViewsSchema =
    [
        new("views", ColumnType.String),
    ];

    /// <summary>
    /// Materialized views report more than their name because the two things a reader needs to know
    /// about one — whether it holds data at all, and how stale that data is — are invisible from a
    /// plain listing and are not answerable by querying it.
    /// </summary>
    internal static readonly IReadOnlyList<DerivedColumnSchema> ShowMaterializedViewsSchema =
    [
        new("materialized_views", ColumnType.String),
        new("populated", ColumnType.Bool),
        new("refreshed_at", ColumnType.String),
    ];

    internal static readonly IReadOnlyList<DerivedColumnSchema> ShowCreateViewSchema =
    [
        new("view", ColumnType.String),
        new("create view", ColumnType.String),
    ];

    internal static readonly IReadOnlyList<DerivedColumnSchema> ShowCreateMaterializedViewSchema =
    [
        new("materialized_view", ColumnType.String),
        new("create materialized view", ColumnType.String),
    ];

    internal static readonly IReadOnlyList<DerivedColumnSchema> ShowColumnsSchema =
    [
        new("Field",   ColumnType.String),
        new("Type",    ColumnType.String),
        new("Null",    ColumnType.String),
        new("Key",     ColumnType.String),
        new("Default", ColumnType.String),
        new("Extra",   ColumnType.String),
    ];

    internal static readonly IReadOnlyList<DerivedColumnSchema> ShowIndexesSchema =
    [
        new("Table",      ColumnType.String),
        new("Non_unique", ColumnType.String),
        new("Key_name",   ColumnType.String),
        new("Columns",    ColumnType.String),
        new("Include",    ColumnType.String),
        new("Index_type", ColumnType.String),
    ];

    /// <summary>
    /// One row per statistics target, discriminated by <c>kind</c>, so the table-level counters, the
    /// per-column estimates, the composite-key estimates and the per-index entry counts arrive as one
    /// result instead of four statements. Columns that do not apply to a row's <c>kind</c> are NULL —
    /// as are those a value has simply never been collected for, which is why <c>last_analyzed</c>
    /// is worth reading before trusting any of it.
    /// </summary>
    internal static readonly IReadOnlyList<DerivedColumnSchema> ShowStatisticsSchema =
    [
        new("table",             ColumnType.String),
        new("kind",              ColumnType.String),
        new("target",            ColumnType.String),
        new("estimated_rows",    ColumnType.Integer64),
        new("distinct_count",    ColumnType.Integer64),
        new("min_value",         ColumnType.String),
        new("max_value",         ColumnType.String),
        new("histogram_buckets", ColumnType.Integer64),
        new("last_analyzed",     ColumnType.String),
        new("stale_mutations",   ColumnType.Integer64),
    ];

    internal static readonly IReadOnlyList<DerivedColumnSchema> ShowCreateTableSchema =
    [
        new("Table",        ColumnType.String),
        new("Create Table", ColumnType.String),
    ];

    internal static readonly IReadOnlyList<DerivedColumnSchema> ShowDatabaseSchema =
    [
        new("database", ColumnType.String),
        new("comment", ColumnType.String),
    ];

    internal static readonly IReadOnlyList<DerivedColumnSchema> ShowGrantsSchema =
    [
        new("user",       ColumnType.String),
        new("object",     ColumnType.String),
        new("privileges", ColumnType.String),
    ];

    internal static readonly IReadOnlyList<DerivedColumnSchema> ShowBranchesSchema =
    [
        new("database",       ColumnType.String),
        new("id",             ColumnType.String),
        new("depth",          ColumnType.Integer64),
        new("parent",         ColumnType.String),
        new("fork_timestamp", ColumnType.String),
    ];

    internal static readonly IReadOnlyList<DerivedColumnSchema> ShowAncestorsSchema =
    [
        new("database",       ColumnType.String),
        new("id",             ColumnType.String),
        new("depth",          ColumnType.Integer64),
        new("fork_timestamp", ColumnType.String),
    ];

    internal static readonly IReadOnlyList<DerivedColumnSchema> ShowOrphanDatabasesSchema =
    [
        new("id",          ColumnType.String),
        new("former_name", ColumnType.String),
        new("dropped_at",  ColumnType.String),
        new("expires_at",  ColumnType.String),
    ];

    // SHOW ENGINE STATS: one row per aggregated instrument + tag-set from the embedded Kommander and
    // Kahuna meters. total/min/max/last are NULL where the metric kind does not define them — a
    // counter has no min, a gauge has no sum.
    internal static readonly IReadOnlyList<DerivedColumnSchema> ShowEngineStatsSchema =
    [
        new("node",   ColumnType.String),
        new("source", ColumnType.String),
        new("metric", ColumnType.String),
        new("tags",   ColumnType.String),
        new("kind",   ColumnType.String),
        new("count",  ColumnType.Integer64),
        new("total",  ColumnType.Float64),
        new("min",    ColumnType.Float64),
        new("max",    ColumnType.Float64),
        new("last",   ColumnType.Float64),
    ];

    // SHOW VARIABLES: one row per configuration setting this node resolved at startup. `value` and
    // `default` are rendered as strings rather than typed columns because the settings are a mix of
    // bool/int/double/string/enum and a single result set has one type per column; `type` carries the
    // real one. Both are NULL for a setting that is genuinely unset, which is distinct from empty.
    internal static readonly IReadOnlyList<DerivedColumnSchema> ShowVariablesSchema =
    [
        new("variable",   ColumnType.String),
        new("value",      ColumnType.String),
        new("type",       ColumnType.String),
        new("default",    ColumnType.String),
        new("source",     ColumnType.String),
        new("mutability", ColumnType.String),
        new("scope",      ColumnType.String),
    ];

    // SHOW CLUSTER SETTINGS: one row per overlay entry the cluster currently carries — the keys a
    // SET CLUSTER SETTING changed and no RESET has dropped. Values are the scalar text the overlay
    // stores (the same spelling config.yml would use); each key's effective per-node value, with
    // provenance, is SHOW VARIABLES' job.
    internal static readonly IReadOnlyList<DerivedColumnSchema> ShowClusterSettingsSchema =
    [
        new("setting", ColumnType.String),
        new("value",   ColumnType.String),
    ];

    // SHOW ORPHAN TABLES: one row per recoverable retained key-space. `kind` distinguishes the two
    // things that can be retained, which look identical without it — a relation that was dropped, and
    // one contents generation a still-live relation stopped reading after a TRUNCATE. For the second,
    // `former_name` is the name of a table that still exists, so an operator reading the list without
    // `kind` would take it for a dropped table.
    internal static readonly IReadOnlyList<DerivedColumnSchema> ShowOrphanTablesSchema =
    [
        new("id",          ColumnType.String),
        new("kind",        ColumnType.String),
        new("former_name", ColumnType.String),
        new("dropped_at",  ColumnType.String),
        new("expires_at",  ColumnType.String),
    ];

    // EXPLAIN / EXPLAIN (LOGICAL|PHYSICAL): one row per plan node. Column order and names/types must
    // match the dictionary keys emitted by ExplainExecutor.ExplainQuery so the positional response
    // resolves each cell by name. Trailing rows (plan-info, cache) omit the metric columns; the
    // positional writer maps a missing key to null, so a shorter row is fine.
    internal static readonly IReadOnlyList<DerivedColumnSchema> ExplainSchema =
    [
        new("stage",          ColumnType.String),
        new("node",           ColumnType.String),
        new("detail",         ColumnType.String),
        new("estimated_rows", ColumnType.Integer64),
        new("estimated_cost", ColumnType.Float64),
    ];

    // EXPLAIN (ANALYZE): the plain EXPLAIN schema plus the measured runtime columns emitted by
    // ExplainExecutor.ExplainAnalyzeQuery.
    internal static readonly IReadOnlyList<DerivedColumnSchema> ExplainAnalyzeSchema =
    [
        new("stage",           ColumnType.String),
        new("node",            ColumnType.String),
        new("detail",          ColumnType.String),
        new("estimated_rows",  ColumnType.Integer64),
        new("estimated_cost",  ColumnType.Float64),
        new("actual_rows",     ColumnType.Integer64),
        new("rows_read",       ColumnType.Integer64),
        new("actual_time_ms",  ColumnType.Float64),
        new("kv_lookups",      ColumnType.Integer64),
        new("kv_scan_entries", ColumnType.Integer64),
    ];

    // ANALYZE TABLE: a single summary row. Column names/types must match the dictionary keys
    // emitted by TableAnalyzer.AnalyzeAsync so the positional response resolves each cell by name.
    internal static readonly IReadOnlyList<DerivedColumnSchema> AnalyzeTableSchema =
    [
        new("table",   ColumnType.String),
        new("status",  ColumnType.String),
        new("rows",    ColumnType.Integer64),
        new("columns", ColumnType.Integer64),
    ];

    // -------------------------------------------------------------------------
    // Build — derives schema from a bound SELECT query.
    // ExprAllFields (*) is expanded to all readable columns from every source.
    // -------------------------------------------------------------------------

    public static IReadOnlyList<DerivedColumnSchema> Build(SelectQuery query, BoundSelectQuery innerBound)
    {
        List<DerivedColumnSchema> columns = new(query.Projections.Count);
        QueryRowNameResolver innerResolver = new(innerBound.Sources, innerBound.DerivedSources);

        for (int i = 0; i < query.Projections.Count; i++)
        {
            ProjectionItem projection = query.Projections[i];
            NodeAst target = QueryExpressionClassifier.UnwrapAlias(projection.Expression);

            if (target.nodeType == NodeType.ExprAllFields)
            {
                ExpandAllFields(innerBound, innerResolver, columns);
                continue;
            }

            string name = QueryProjectionResolver.GetOutputNameFromProjectionExpression(projection.Expression, i);
            ColumnType type = InferType(projection.Expression, innerBound, innerResolver);
            columns.Add(new DerivedColumnSchema(name, type));
        }

        return columns;
    }

    /// <summary>
    /// Expands a <c>SELECT *</c> into one output column per readable source column, matching the
    /// exact keys the row cursor produces so positional encoding can look each value up by name.
    /// <para>
    /// A single-source query streams rows keyed by bare column name, so columns stay bare. A
    /// multi-source query (any join, or extra table alongside a derived/subquery source) is emitted by
    /// the join executor, which qualifies every column as <c>{alias}.{column}</c> via
    /// <see cref="QueryRowNameResolver.FormatQualifiedKey"/>; the schema must qualify identically or
    /// a by-name lookup against the qualified row keys would silently miss and encode null.
    /// </para>
    /// <para>
    /// The discriminator is <see cref="QueryRowNameResolver.UsesQualifiedRowKeys"/> — the convention the
    /// rows are actually keyed by — and deliberately not <see cref="BoundSelectQuery.IsMultiSource"/>,
    /// which routes execution to the join path but is also true for the single-derived-source shape a
    /// view expands into. That shape keys its rows bare, so qualifying its schema named columns no row
    /// had and sent every cell of <c>SELECT * FROM a_view</c> as null over the wire while the in-process
    /// cursor was correct. Derived sources are included either way so their columns are not dropped
    /// from a join's output schema.
    /// </para>
    /// Column order follows source declaration order (table sources first, then derived sources),
    /// which matches the left-to-right merge order of a left-deep join; value correctness does not
    /// depend on this order because encoding resolves each column by name.
    /// </summary>
    private static void ExpandAllFields(
        BoundSelectQuery innerBound, QueryRowNameResolver resolver, List<DerivedColumnSchema> columns)
    {
        bool qualify = resolver.UsesQualifiedRowKeys();

        foreach (BoundTableSource source in innerBound.Sources)
        {
            foreach (TableColumnSchema col in source.Table.Schema.Columns ?? [])
            {
                if (!SchemaElementStateRules.IsReadable(col))
                    continue;

                string name = qualify
                    ? QueryRowNameResolver.FormatQualifiedKey(source.Alias, col.Name)
                    : col.Name;

                columns.Add(new DerivedColumnSchema(name, col.Type));
            }
        }

        foreach (BoundDerivedTableSource derived in innerBound.DerivedSources)
        {
            foreach (DerivedColumnSchema col in derived.Columns)
            {
                string name = qualify
                    ? QueryRowNameResolver.FormatQualifiedKey(derived.Alias, col.Name)
                    : col.Name;

                columns.Add(new DerivedColumnSchema(name, col.Type));
            }
        }
    }

    /// <summary>
    /// Builds the output schema for a FROM-less SELECT from its projection AST nodes.
    /// Each projection's inferred type is derived using the same rules as <see cref="InferType"/>
    /// with an empty bound query (no table sources).
    /// </summary>
    public static IReadOnlyList<DerivedColumnSchema> BuildFromless(
        IReadOnlyList<NodeAst> projections,
        IReadOnlyDictionary<string, ColumnValue> evaluatedRow)
    {
        List<DerivedColumnSchema> columns = new(projections.Count);

        for (int i = 0; i < projections.Count; i++)
        {
            NodeAst projection = projections[i];
            string name = QueryProjectionResolver.GetOutputNameFromProjectionExpression(projection, i);

            // Infer from the evaluated value where possible; fall back to String for unknowns.
            ColumnType type = evaluatedRow.TryGetValue(name, out ColumnValue? val)
                ? (val.Type == ColumnType.Null ? ColumnType.String : val.Type)
                : ColumnType.String;

            columns.Add(new DerivedColumnSchema(name, type));
        }

        return columns;
    }

    private static ColumnType InferType(
        NodeAst expression,
        BoundSelectQuery innerBound,
        QueryRowNameResolver innerResolver)
    {
        NodeAst target = QueryExpressionClassifier.UnwrapAlias(expression);

        // Compound aggregate check is hoisted before nodeType dispatch so arithmetic-topped
        // compounds (ExprAdd, ExprDiv, …) are also handled — not just ExprFuncCall-wrapped ones.
        if (QueryExpressionClassifier.IsCompoundAggregateProjection(target))
            return InferCompoundAggregateType(target, innerBound, innerResolver);

        if (target.nodeType == NodeType.Identifier && target.yytext is not null)
        {
            string lookupKey = innerResolver.ResolveRowLookupKey(target.yytext);
            return ResolveColumnType(innerBound, lookupKey, target.yytext);
        }

        if (target.nodeType == NodeType.ExprFuncCall)
        {
            if (QueryExpressionClassifier.IsAggregateProjection(target))
                return InferAggregateType(target, innerBound, innerResolver);

            string funcName = target.leftAst!.yytext!.ToLowerInvariant();

            if (ScalarFunctionEvaluator.IsRegisteredScalarFunction(funcName))
                return ScalarFunctionEvaluator.InferReturnType(funcName, []);

            return ColumnType.String;
        }

        if (target.nodeType == NodeType.ExprCast)
            return CastScalarFunctions.InferCastReturnType(target.rightAst!);

        if (target.nodeType == NodeType.ExprCase)
            return InferCaseType(target, innerBound, innerResolver);

        return ColumnType.String;
    }

    /// <summary>
    /// A CASE has no single static type — each branch is typed per row (the engine carries a
    /// <see cref="ColumnType"/> on every value). For the one place a static type is required — the
    /// derived-table / client column metadata — pick a representative type from the ELSE result, else
    /// the first WHEN's result, falling back to <see cref="ColumnType.String"/> when neither yields a
    /// known type. This never crashes and is only a declaration hint; heterogeneous per-row values
    /// still encode correctly because each carries its own type.
    /// </summary>
    private static ColumnType InferCaseType(
        NodeAst caseNode,
        BoundSelectQuery innerBound,
        QueryRowNameResolver innerResolver)
    {
        NodeAst? representative = caseNode.extendedOne; // ELSE result, if present

        if (representative is null && caseNode.rightAst is not null)
        {
            foreach (NodeAst clause in CamusDB.Core.CommandsExecutor.Controllers.DML.SQLExecutorBaseCreator
                         .EnumerateWhenClauses(caseNode.rightAst))
            {
                representative = clause.rightAst; // first WHEN's THEN result
                break;
            }
        }

        return representative is null
            ? ColumnType.String
            : InferType(representative, innerBound, innerResolver);
    }

    /// <summary>
    /// Infers the return type of a compound aggregate expression (aggregate nested inside a
    /// non-aggregate node). Two shapes are handled:
    /// <list type="bullet">
    ///   <item>Scalar function wrapper such as <c>COALESCE(SUM(x), 0)</c>: collects the inferred
    ///     type of each argument and forwards them to
    ///     <see cref="ScalarFunctionEvaluator.InferReturnType"/> so the outer function returns
    ///     the real type instead of <see cref="ColumnType.Null"/>.</item>
    ///   <item>Arithmetic binary op such as <c>SUM(a)+1</c> or <c>SUM(a)/SUM(b)</c>: infers
    ///     both operand types and returns the wider numeric type.</item>
    /// </list>
    /// </summary>
    private static ColumnType InferCompoundAggregateType(
        NodeAst target,
        BoundSelectQuery innerBound,
        QueryRowNameResolver innerResolver)
    {
        if (target.nodeType == NodeType.ExprFuncCall)
        {
            string funcName = target.leftAst!.yytext!.ToLowerInvariant();
            if (ScalarFunctionEvaluator.IsRegisteredScalarFunction(funcName))
            {
                ColumnType[] argTypes = InferArgListTypes(target.rightAst, innerBound, innerResolver);
                return ScalarFunctionEvaluator.InferReturnType(funcName, argTypes);
            }

            return ColumnType.String;
        }

        // Arithmetic binary op: return the wider numeric type of both operands.
        ColumnType left  = target.leftAst  is not null ? InferArgType(target.leftAst,  innerBound, innerResolver) : ColumnType.Null;
        ColumnType right = target.rightAst is not null ? InferArgType(target.rightAst, innerBound, innerResolver) : ColumnType.Null;
        return WiderNumericType(left, right);
    }

    private static ColumnType InferAggregateType(
        NodeAst funcCall,
        BoundSelectQuery innerBound,
        QueryRowNameResolver innerResolver)
    {
        string funcName = funcCall.leftAst!.yytext!.ToLowerInvariant();

        return funcName switch
        {
            "count" => ColumnType.Integer64,
            "sum" or "avg" => InferAggregateArgumentType(funcCall, innerBound, innerResolver, fallback: ColumnType.Float64),
            "min" or "max" => InferAggregateArgumentType(funcCall, innerBound, innerResolver, fallback: ColumnType.String),
            _ => ColumnType.String,
        };
    }

    private static ColumnType InferAggregateArgumentType(
        NodeAst funcCall,
        BoundSelectQuery innerBound,
        QueryRowNameResolver innerResolver,
        ColumnType fallback)
    {
        if (funcCall.rightAst is null)
            return fallback;

        NodeAst argument = funcCall.rightAst.nodeType == NodeType.ExprArgumentList
            ? funcCall.rightAst.leftAst ?? funcCall.rightAst
            : funcCall.rightAst;

        if (argument.nodeType == NodeType.ExprAllFields)
            return ColumnType.Integer64;

        return InferType(argument, innerBound, innerResolver);
    }

    /// <summary>
    /// Collects the inferred <see cref="ColumnType"/> for each argument in a function's argument
    /// list. Used to pass real argument types to <see cref="ScalarFunctionEvaluator.InferReturnType"/>
    /// for compound-aggregate expressions so the outer function (e.g. COALESCE) does not receive an
    /// empty list and fall back to <see cref="ColumnType.Null"/>.
    /// </summary>
    private static ColumnType[] InferArgListTypes(
        NodeAst? argList,
        BoundSelectQuery innerBound,
        QueryRowNameResolver innerResolver)
    {
        if (argList is null)
            return [];

        List<ColumnType> types = new();
        CollectArgTypes(argList, innerBound, innerResolver, types);
        return types.ToArray();
    }

    private static void CollectArgTypes(
        NodeAst argNode,
        BoundSelectQuery innerBound,
        QueryRowNameResolver innerResolver,
        List<ColumnType> types)
    {
        if (argNode.nodeType == NodeType.ExprArgumentList)
        {
            if (argNode.leftAst is not null)
                CollectArgTypes(argNode.leftAst, innerBound, innerResolver, types);

            if (argNode.rightAst is not null)
                CollectArgTypes(argNode.rightAst, innerBound, innerResolver, types);

            return;
        }

        types.Add(InferArgType(argNode, innerBound, innerResolver));
    }

    /// <summary>
    /// Infers the <see cref="ColumnType"/> of a single expression node when it appears as a
    /// function argument or arithmetic operand. Fast-paths for literals and identifiers avoid
    /// re-entering the compound-aggregate logic; the default case delegates to
    /// <see cref="InferType"/> so nested compounds (e.g. COALESCE inside another COALESCE)
    /// and bare aggregate calls are resolved correctly.
    /// </summary>
    private static ColumnType InferArgType(
        NodeAst arg,
        BoundSelectQuery innerBound,
        QueryRowNameResolver innerResolver)
    {
        return arg.nodeType switch
        {
            NodeType.Integer    => ColumnType.Integer64,
            NodeType.Float      => ColumnType.Float64,
            NodeType.String     => ColumnType.String,
            NodeType.Bool       => ColumnType.Bool,
            NodeType.Null       => ColumnType.Null,
            NodeType.Identifier => arg.yytext is not null
                ? ResolveColumnType(innerBound, innerResolver.ResolveRowLookupKey(arg.yytext), arg.yytext)
                : ColumnType.String,
            // All other nodes (bare aggregates, compounds, scalar funcs, casts) go through
            // InferType so type inference is consistent and recursive.
            _ => InferType(arg, innerBound, innerResolver),
        };
    }

    /// <summary>
    /// Returns the wider of two types for arithmetic inference: Float64 &gt; Float32 &gt;
    /// Integer64. Falls back to String for non-numeric or mismatched operands.
    /// </summary>
    private static ColumnType WiderNumericType(ColumnType a, ColumnType b)
    {
        if (a == ColumnType.Float64 || b == ColumnType.Float64) return ColumnType.Float64;
        if (a == ColumnType.Float32 || b == ColumnType.Float32) return ColumnType.Float32;
        if (a == ColumnType.Integer64 || b == ColumnType.Integer64) return ColumnType.Integer64;
        return ColumnType.String;
    }

    /// <summary>
    /// Resolves the declared <see cref="ColumnType"/> for an identifier projection. <paramref name="lookupKey"/>
    /// is the resolver's authoritative row-key form: the bare column name for a single-source query, and the
    /// <c>alias.column</c> qualified form for any join (see <see cref="QueryRowNameResolver.UsesQualifiedRowKeys"/>).
    /// The physical-source match must therefore compare against <em>both</em> the bare name and the
    /// alias-qualified name — comparing only the bare <c>column.Name</c> makes every join-projected column fall
    /// through to <see cref="ColumnType.String"/>, which silently mis-declares non-self-describing wire forms
    /// (uuid, bytes) and breaks type-driven client decoding even though the row payload is correct.
    /// </summary>
    private static ColumnType ResolveColumnType(BoundSelectQuery innerBound, string lookupKey, string originalIdentifier)
    {
        foreach (BoundTableSource source in innerBound.Sources)
        {
            foreach (TableColumnSchema column in source.Table.Schema.Columns ?? [])
            {
                if (!SchemaElementStateRules.IsReadable(column))
                    continue;

                if (lookupKey == column.Name || MatchesQualifiedKey(lookupKey, source.Alias, column.Name))
                    return column.Type;
            }
        }

        if (innerBound.DerivedSources.Count > 0)
        {
            bool isQualified = TrySplitQualified(originalIdentifier, out _, out string bareName);

            foreach (BoundDerivedTableSource source in innerBound.DerivedSources)
            {
                if (source.HasColumn(lookupKey))
                    return source.GetColumnType(lookupKey);

                if (isQualified && source.HasColumn(bareName))
                    return source.GetColumnType(bareName);
            }
        }

        return ColumnType.String;
    }

    /// <summary>
    /// Allocation-free equivalent of <c>lookupKey == QueryRowNameResolver.FormatQualifiedKey(alias, columnName)</c>:
    /// an ordinal comparison against the <c>{alias}.{column}</c> form without materializing the qualified
    /// string. This runs once per scanned source column for every identifier projection on every query, so
    /// building the candidate key just to compare it dominated the schema-derivation allocation profile.
    /// </summary>
    private static bool MatchesQualifiedKey(string lookupKey, string alias, string columnName)
    {
        if (lookupKey.Length != alias.Length + 1 + columnName.Length)
            return false;

        return lookupKey[alias.Length] == '.'
            && lookupKey.AsSpan(0, alias.Length).SequenceEqual(alias)
            && lookupKey.AsSpan(alias.Length + 1).SequenceEqual(columnName);
    }

    private static bool TrySplitQualified(string identifier, out string alias, out string columnName)
    {
        int dotIndex = identifier.IndexOf('.');

        if (dotIndex <= 0 || dotIndex >= identifier.Length - 1)
        {
            alias = "";
            columnName = identifier;
            return false;
        }

        alias = identifier[..dotIndex];
        columnName = identifier[(dotIndex + 1)..];
        return true;
    }
}
