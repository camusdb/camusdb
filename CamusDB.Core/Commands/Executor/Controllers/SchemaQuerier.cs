
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using System.Text;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Config;
using CamusDB.Core.Diagnostics;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Statistics.Models;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// This controller allows querying the information_schema. The tables of the information_schema
/// are simulated from the internal structures.
/// </summary>
internal sealed class SchemaQuerier
{
    private readonly CatalogsManager catalogs;

    /// <summary>Configuration for this engine; injected, never ambient.</summary>
    private CamusDBOptions options;

    /// <summary>
    /// Swaps in a newly published configuration snapshot. Reference assignment is atomic and the
    /// record itself stays immutable; readers pin the field once at the top of an operation, so an
    /// in-flight operation keeps the snapshot it started with and a change takes effect at the
    /// next operation boundary.
    /// </summary>
    internal void ApplyOptions(CamusDBOptions next) => options = next;

    public SchemaQuerier(CatalogsManager catalogsManager, Microsoft.Extensions.Logging.ILogger<ICamusDB> logger, CamusDBOptions options)
    {
        this.catalogs = catalogsManager;
        this.options = options;
    }

    /// <summary>
    /// Lists the tables of <paramref name="database"/>, optionally narrowed by a LIKE
    /// <paramref name="pattern"/>.
    ///
    /// <para>When <paramref name="principal"/> is non-null (authentication is on) a table is listed
    /// only if the caller holds at least one privilege on it — a table id the caller has no grant for
    /// is omitted rather than rejected, so the statement never reveals the existence of a table the
    /// caller cannot touch. A null principal (authentication disabled) lists everything.</para>
    /// </summary>
    internal async IAsyncEnumerable<QueryResultRow> ShowTables(DatabaseDescriptor database, string? pattern = null, Principal? principal = null)
    {
        await Task.CompletedTask;

        foreach (KeyValuePair<string, TableSchema> table in database.Schema.Tables)
        {
            // Tables only. A materialized view is stored as a relation but is not one to a user, and
            // it has SHOW MATERIALIZED VIEWS of its own; a staging relation is engine bookkeeping that
            // exists for the duration of a refresh and answers to a name no client can even type.
            if (table.Value.IsMaterializedView || MaterializedViewNaming.IsStagingRelation(table.Key))
                continue;

            if (pattern is not null && !LikeMatch(table.Key, pattern))
                continue;

            if (principal is not null && !principal.HasAnyPrivilege(database.Id, table.Value.Id))
                continue;

            yield return new QueryResultRow(default, new Dictionary<string, ColumnValue>(StringComparer.OrdinalIgnoreCase)
            {
                { "tables", new ColumnValue(ColumnType.String, table.Key) }
            });
        }
    }

    /// <summary>
    /// Lists the non-materialized views of <paramref name="database"/>, optionally narrowed by a
    /// LIKE <paramref name="pattern"/>. Materialized views are deliberately excluded — they are
    /// relations and have their own statement — matching how <c>SHOW TABLES</c> lists only tables.
    /// </summary>
    internal async IAsyncEnumerable<QueryResultRow> ShowViews(
        DatabaseDescriptor database, string? pattern = null, Principal? principal = null)
    {
        await Task.CompletedTask;

        foreach (KeyValuePair<string, ViewSchema> view in database.Schema.Views)
        {
            // Omitted rather than refused, exactly as SHOW TABLES treats a table the caller cannot
            // reach: the name itself is the disclosure, so erroring would leak it just as well.
            if (!ViewAuthorization.IsVisible(database, view.Value, principal))
                continue;

            if (pattern is not null && !LikeMatch(view.Key, pattern))
                continue;

            yield return new QueryResultRow(default, new Dictionary<string, ColumnValue>(StringComparer.OrdinalIgnoreCase)
            {
                { "views", new ColumnValue(ColumnType.String, view.Key) }
            });
        }
    }

    /// <summary>
    /// Lists the materialized views, with the populated flag and the snapshot each one's contents are
    /// consistent as of. The timestamp is the refresh's <em>source read</em> HLC, not the wall time
    /// the refresh finished, because that is the value that answers "how stale is this".
    /// </summary>
    internal async IAsyncEnumerable<QueryResultRow> ShowMaterializedViews(
        DatabaseDescriptor database, string? pattern = null, Principal? principal = null)
    {
        await Task.CompletedTask;

        foreach (KeyValuePair<string, TableSchema> relation in database.Schema.Tables)
        {
            if (!relation.Value.IsMaterializedView)
                continue;

            if (pattern is not null && !LikeMatch(relation.Key, pattern))
                continue;

            if (principal is not null && !principal.HasAnyPrivilege(database.Id, relation.Value.Id))
                continue;

            yield return new QueryResultRow(default, new Dictionary<string, ColumnValue>(StringComparer.OrdinalIgnoreCase)
            {
                { "materialized_views", new ColumnValue(ColumnType.String, relation.Key) },
                { "populated", new ColumnValue(ColumnType.Bool, relation.Value.IsPopulated) },
                {
                    "refreshed_at",
                    new ColumnValue(ColumnType.String, relation.Value.RefreshedAt?.ToString() ?? "")
                }
            });
        }
    }

    /// <summary>
    /// Renders <c>SHOW CREATE VIEW</c> from the stored normalized definition.
    /// </summary>
    /// <remarks>
    /// The output is the normalized body, not the text the user typed — a view is stored re-rendered
    /// so renames can rewrite it as a targeted AST edit. PostgreSQL's <c>pg_get_viewdef</c> behaves
    /// the same way for the same reason, so normalized output is expected rather than surprising.
    /// </remarks>
    internal async IAsyncEnumerable<QueryResultRow> ShowCreateView(Schema schema, string viewName, ViewSchema view)
    {
        await Task.CompletedTask;

        ViewDefinition definition = view.Definition
            ?? throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"View '{viewName}' has no stored definition");

        StringBuilder sql = new();
        sql.Append("CREATE VIEW `").Append(view.Name).Append('`');

        // The column list is emitted only when it renames something the body does not already
        // produce; echoing it unconditionally would add noise to every ordinary view.
        sql.Append(" AS ").Append(RenderStoredBody(schema, definition, view.Name ?? viewName));

        if (definition.CheckOption == CheckOptionKind.Local)
            sql.Append(" WITH LOCAL CHECK OPTION");
        else if (definition.CheckOption == CheckOptionKind.Cascaded)
            sql.Append(" WITH CASCADED CHECK OPTION");

        yield return new QueryResultRow(default, new Dictionary<string, ColumnValue>(StringComparer.OrdinalIgnoreCase)
        {
            { "view", new ColumnValue(ColumnType.String, view.Name ?? viewName) },
            { "create view", new ColumnValue(ColumnType.String, sql.ToString()) }
        });
    }

    /// <summary>
    /// Renders <c>SHOW CREATE MATERIALIZED VIEW</c> from the stored definition.
    /// </summary>
    /// <remarks>
    /// An unpopulated materialized view renders <c>WITH NO DATA</c> — not as decoration, but because
    /// that is the statement which reproduces it. Re-running the output of this statement has to give
    /// back the same object, and <c>WITH DATA</c> would give back a populated one.
    /// </remarks>
    internal async IAsyncEnumerable<QueryResultRow> ShowCreateMaterializedView(Schema schema, TableSchema view)
    {
        await Task.CompletedTask;

        ViewDefinition definition = view.ViewDefinition
            ?? throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt, $"Materialized view '{view.Name}' has no stored definition");

        StringBuilder sql = new();
        sql.Append("CREATE MATERIALIZED VIEW `").Append(view.Name).Append("` AS ")
           .Append(RenderStoredBody(schema, definition, view.Name ?? ""));

        if (!view.IsPopulated)
            sql.Append(" WITH NO DATA");

        yield return new QueryResultRow(default, new Dictionary<string, ColumnValue>(StringComparer.OrdinalIgnoreCase)
        {
            { "materialized_view", new ColumnValue(ColumnType.String, view.Name ?? "") },
            { "create materialized view", new ColumnValue(ColumnType.String, sql.ToString()) }
        });
    }

    /// <summary>
    /// The stored body as a user should read it: relation references resolved to the names those
    /// relations currently answer to.
    /// </summary>
    /// <remarks>
    /// <para>A body binds its sources by immutable id, so the stored text is not printable as-is —
    /// a name is presentation, produced here. This is why no second, pre-rendered copy of the body
    /// is kept: one authoritative form that renders on the way out cannot drift from itself, and
    /// two copies is exactly the hazard binding by id exists to remove.</para>
    ///
    /// <para>Costs a parse and a re-render per statement, which is acceptable for <c>SHOW</c> and
    /// happens only for a body that actually carries a reference — a body written before ids were
    /// stored is returned untouched, character for character.</para>
    /// </remarks>
    private static string RenderStoredBody(Schema schema, ViewDefinition definition, string viewName)
    {
        if (!DDL.StoredBodyBinder.MayContainReferences(definition.Sql))
            return definition.Sql;

        NodeAst body = SQLParserProcessor.Parse(definition.Sql);

        return DDL.ViewBodyRenderer.RenderSelect(
            DDL.StoredBodyBinder.ResolveStoredForm(schema, body, viewName));
    }

    /// <summary>
    /// <c>SHOW COLUMNS FROM &lt;view&gt;</c>, answered from the view's frozen column list.
    /// </summary>
    /// <remarks>
    /// Deliberately the stored list rather than a re-derivation of the body: the stored list is what
    /// the view actually publishes, and re-deriving would report a shape that a base-table column add
    /// had since widened — the exact drift freezing the shape exists to prevent.
    ///
    /// <para>The same six columns as the table form, so a client can consume either without
    /// branching. <c>Null</c>, <c>Key</c>, <c>Default</c> and <c>Extra</c> are blank because a view
    /// column has none of them: nullability, keys and defaults belong to the base table, and
    /// reporting the base table's would describe a constraint that does not apply to reads through
    /// the view.</para>
    /// </remarks>
    internal async IAsyncEnumerable<QueryResultRow> ShowViewColumns(ViewSchema view)
    {
        await Task.CompletedTask;

        foreach (ViewColumnSchema column in view.Definition?.Columns ?? [])
        {
            yield return new QueryResultRow(default, new Dictionary<string, ColumnValue>(StringComparer.OrdinalIgnoreCase)
            {
                { "Field", new ColumnValue(ColumnType.String, column.Name) },
                { "Type", new ColumnValue(ColumnType.String, column.Type.ToString().ToLowerInvariant()) },
                { "Null", new ColumnValue(ColumnType.String, "") },
                { "Key", new ColumnValue(ColumnType.String, "") },
                { "Default", new ColumnValue(ColumnType.String, "") },
                { "Extra", new ColumnValue(ColumnType.String, "") },
            });
        }
    }

    internal async IAsyncEnumerable<QueryResultRow> ShowColumns(TableDescriptor table)
    {
        await Task.CompletedTask;

        foreach (TableColumnSchema column in table.Schema.Columns!)
        {
            if (!SchemaElementStateRules.IsReadable(column))
                continue;

            yield return new QueryResultRow(default, new Dictionary<string, ColumnValue>(StringComparer.OrdinalIgnoreCase)
            {
                { "Field", new ColumnValue(ColumnType.String, column.Name) },
                { "Type", new ColumnValue(ColumnType.String, GetSQLType(column)) },
                { "Null", new ColumnValue(ColumnType.String, column.NotNull ? "NO" : "YES") },
                { "Key", new ColumnValue(ColumnType.String, IsPrimary(column.Name, table.Indexes) ? "PRI" : "") },
                { "Default", GetDefaultValue(column) },
                { "Extra", new ColumnValue(ColumnType.String, "") },
            });
        }
    }

    private static bool IsPrimary(string name, Dictionary<string, TableIndexSchema> indexes)
    {
        foreach (KeyValuePair<string, TableIndexSchema> kv in indexes)
        {
            if (kv.Key == CamusDBConstants.PrimaryKeyInternalName && kv.Value.Columns.Contains(name))
                return true;
        }
        return false;
    }

    private static ColumnValue GetDefaultValue(TableColumnSchema column)
    {
        if (column.DefaultFunction is not null)
            return new ColumnValue(ColumnType.String, column.DefaultFunction + "()");

        if (column.DefaultValue is null)
            return new ColumnValue(ColumnType.String, "NULL");

        return column.DefaultValue.Type switch
        {
            ColumnType.Null => new ColumnValue(ColumnType.String, "NULL"),
            ColumnType.Id => new ColumnValue(ColumnType.String, column.DefaultValue.StrValue!),
            ColumnType.String => new ColumnValue(ColumnType.String, column.DefaultValue.StrValue!),
            ColumnType.Bool => new ColumnValue(ColumnType.String, column.DefaultValue.BoolValue.ToString()),
            ColumnType.Integer64 => new ColumnValue(ColumnType.String, column.DefaultValue.LongValue.ToString(CultureInfo.InvariantCulture)),
            ColumnType.Float64 => new ColumnValue(ColumnType.String, column.DefaultValue.FloatValue.ToString(CultureInfo.InvariantCulture)),
            ColumnType.Float32 => new ColumnValue(ColumnType.String, ((float)column.DefaultValue.FloatValue).ToString(CultureInfo.InvariantCulture)),
            // Date/DateTime render as their ISO-8601 form (yyyy-MM-dd / round-trip "o").
            ColumnType.Date or ColumnType.DateTime => new ColumnValue(ColumnType.String, column.DefaultValue.IsoValue!),
            // Bytes render as an X'…' hex-string literal, matching the SQL bytes-literal syntax.
            // (A bare 0x… form would read back as an integer literal, not bytes.)
            ColumnType.Bytes => new ColumnValue(ColumnType.String, SqlStringLiteral.QuoteBytes(column.DefaultValue.BytesValue ?? [])),
            ColumnType.Uuid => new ColumnValue(ColumnType.String, column.DefaultValue.UuidValue!),
            _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Unknown default type :" + column.DefaultValue.Type),
        };
    }

    internal async IAsyncEnumerable<QueryResultRow> ShowIndexes(TableDescriptor table)
    {
        await Task.CompletedTask;

        foreach (KeyValuePair<string, TableIndexSchema> index in table.Indexes)
        {
            if (!SchemaElementStateRules.IsReadableIndex(table.Schema, index.Value))
                continue;

            yield return new QueryResultRow(default, new Dictionary<string, ColumnValue>(StringComparer.OrdinalIgnoreCase)
            {
                { "Table", new ColumnValue(ColumnType.String, table.Name) },
                { "Non_unique", new ColumnValue(ColumnType.String, index.Value.Type == IndexType.Unique ? "0" : "1") },
                { "Key_name", new ColumnValue(ColumnType.String, index.Key) },
                { "Columns", new ColumnValue(ColumnType.String, string.Join(",", index.Value.Columns)) },
                { "Include", new ColumnValue(ColumnType.String, string.Join(",", index.Value.IncludeColumns)) },
                { "Index_type", new ColumnValue(ColumnType.String, "ORDERED") }
            });
        }
    }

    /// <summary>
    /// Renders <c>SHOW STATISTICS FOR &lt;table&gt;</c> from a snapshot the caller already took.
    ///
    /// <para>The snapshot is passed in rather than fetched here so this class keeps its narrow
    /// dependencies (catalogs and options) and the statistics read stays where it can be awaited
    /// before the first row is yielded — a failure surfaces as a statement error rather than
    /// mid-stream.</para>
    ///
    /// <para>The <c>table</c> row is emitted even when <paramref name="view"/> is null. A table that
    /// has never been written to or analyzed genuinely has no statistics, and an all-NULL row says
    /// so; returning no rows at all would be indistinguishable from a filter that matched nothing.</para>
    ///
    /// <para>Schema elements still being built (a column or index not yet public) are skipped, for
    /// the same reason <see cref="ShowColumns"/> and <see cref="ShowIndexes"/> skip them: an element
    /// no query can use yet must not become visible through a side channel.</para>
    ///
    /// <para><c>last_analyzed</c> renders the physical component of the recorded HLC as UTC
    /// ISO-8601, not the raw <c>HLC(n:l:c)</c> form. The value answers "how old are these
    /// statistics?", which a reader judges against wall-clock time; the logical component carries
    /// no meaning for that question. It is a display-only rendering — nothing consumes it as a
    /// timestamp the way <c>fork_timestamp</c> is consumed, which is why that column stays HLC.</para>
    /// </summary>
    internal async IAsyncEnumerable<QueryResultRow> ShowStatistics(TableDescriptor table, TableStatisticsView? view)
    {
        await Task.CompletedTask;

        ColumnValue tableName = new(ColumnType.String, table.Name);
        ColumnValue lastAnalyzed = view is null || view.LastAnalyzedAt.IsNull()
            ? ColumnValue.Null
            : new ColumnValue(ColumnType.String, IsoFromUnixMs(view.LastAnalyzedAt.L));
        ColumnValue staleMutations = view is null
            ? ColumnValue.Null
            : new ColumnValue(ColumnType.Integer64, view.MutationsSinceAnalyze);

        yield return StatisticsRow(
            tableName, "table", ColumnValue.Null,
            estimatedRows: OptionalCount(view?.RowCount),
            distinctCount: ColumnValue.Null,
            minValue: ColumnValue.Null,
            maxValue: ColumnValue.Null,
            histogramBuckets: ColumnValue.Null,
            lastAnalyzed, staleMutations);

        if (view is null)
            yield break;

        foreach (TableColumnSchema column in table.Schema.Columns!)
        {
            if (!SchemaElementStateRules.IsReadable(column))
                continue;

            bool hasMinMax = view.ColumnStats.TryGetValue(column.Name, out ColumnMinMax? minMax);
            bool hasNdv = view.ColumnNdv.TryGetValue(column.Name, out long ndv);
            bool hasHistogram = view.Histograms.TryGetValue(column.Name, out ColumnHistogram? histogram);

            // A column nothing has been observed for produces no row: an all-NULL row per column
            // would bury the columns that do carry estimates.
            if (!hasMinMax && !hasNdv && !hasHistogram)
                continue;

            yield return StatisticsRow(
                tableName, "column", new ColumnValue(ColumnType.String, column.Name),
                estimatedRows: ColumnValue.Null,
                distinctCount: hasNdv ? new ColumnValue(ColumnType.Integer64, ndv) : ColumnValue.Null,
                minValue: RenderBound(minMax?.Min),
                maxValue: RenderBound(minMax?.Max),
                histogramBuckets: hasHistogram
                    ? new ColumnValue(ColumnType.Integer64, histogram!.Buckets.Count)
                    : ColumnValue.Null,
                lastAnalyzed, staleMutations);
        }

        foreach (string signature in view.KeyNdv.Keys.OrderBy(static k => k, StringComparer.Ordinal))
        {
            yield return StatisticsRow(
                tableName, "key", new ColumnValue(ColumnType.String, signature),
                estimatedRows: ColumnValue.Null,
                distinctCount: new ColumnValue(ColumnType.Integer64, view.KeyNdv[signature]),
                minValue: ColumnValue.Null,
                maxValue: ColumnValue.Null,
                histogramBuckets: ColumnValue.Null,
                lastAnalyzed, staleMutations);
        }

        foreach (KeyValuePair<string, TableIndexSchema> index in table.Indexes.OrderBy(static i => i.Key, StringComparer.Ordinal))
        {
            if (!SchemaElementStateRules.IsReadableIndex(table.Schema, index.Value))
                continue;

            yield return StatisticsRow(
                tableName, "index", new ColumnValue(ColumnType.String, index.Key),
                estimatedRows: view.IndexEntryCounts.TryGetValue(index.Key, out long entries)
                    ? OptionalCount(entries)
                    : ColumnValue.Null,
                distinctCount: ColumnValue.Null,
                minValue: ColumnValue.Null,
                maxValue: ColumnValue.Null,
                histogramBuckets: ColumnValue.Null,
                lastAnalyzed, staleMutations);
        }
    }

    private static QueryResultRow StatisticsRow(
        ColumnValue table,
        string kind,
        ColumnValue target,
        ColumnValue estimatedRows,
        ColumnValue distinctCount,
        ColumnValue minValue,
        ColumnValue maxValue,
        ColumnValue histogramBuckets,
        ColumnValue lastAnalyzed,
        ColumnValue staleMutations)
        => new(default, new Dictionary<string, ColumnValue>(StringComparer.OrdinalIgnoreCase)
        {
            { "table", table },
            { "kind", new ColumnValue(ColumnType.String, kind) },
            { "target", target },
            { "estimated_rows", estimatedRows },
            { "distinct_count", distinctCount },
            { "min_value", minValue },
            { "max_value", maxValue },
            { "histogram_buckets", histogramBuckets },
            { "last_analyzed", lastAnalyzed },
            { "stale_mutations", staleMutations },
        });

    /// <summary>A negative or absent count means "never recorded", which renders as NULL, not as a number.</summary>
    private static ColumnValue OptionalCount(long? count)
        => count is null || count < 0 ? ColumnValue.Null : new ColumnValue(ColumnType.Integer64, count.Value);

    /// <summary>
    /// Renders a statistics bound as the SQL literal that would have produced it, reusing
    /// <see cref="ColumnValue"/>'s own formatters for dates and UUIDs so this never drifts from how
    /// the same value prints elsewhere.
    ///
    /// <para>Types with no ordering (<c>Bool</c>) and types whose payload a bound does not carry
    /// (<c>Bytes</c>, arrays) render as NULL: min/max is only tracked for ordered scalars, so a
    /// bound of those types holds no value to show.</para>
    /// </summary>
    private static ColumnValue RenderBound(ScalarBound? bound)
    {
        if (bound is null)
            return ColumnValue.Null;

        return bound.Type switch
        {
            ColumnType.Integer64 => new ColumnValue(ColumnType.String, bound.LongValue.ToString(CultureInfo.InvariantCulture)),
            ColumnType.Float64 => new ColumnValue(ColumnType.String, bound.FloatValue.ToString(CultureInfo.InvariantCulture)),
            ColumnType.Float32 => new ColumnValue(ColumnType.String, ((float)bound.FloatValue).ToString(CultureInfo.InvariantCulture)),
            ColumnType.String or ColumnType.Id => bound.StrValue is null
                ? ColumnValue.Null
                : new ColumnValue(ColumnType.String, bound.StrValue),
            ColumnType.Date or ColumnType.DateTime => new ColumnValue(
                ColumnType.String, new ColumnValue(bound.Type, bound.LongValue).IsoValue!),
            ColumnType.Uuid => new ColumnValue(
                ColumnType.String, new ColumnValue(ColumnType.Uuid, bound.UuidHigh, bound.LongValue).UuidValue!),
            _ => ColumnValue.Null,
        };
    }

    internal async IAsyncEnumerable<QueryResultRow> ShowCreateTable(TableDescriptor table)
    {
        await Task.CompletedTask;

        StringBuilder createTableSql = new();

        createTableSql.Append("CREATE TABLE `" + table.Name + "` (");

        var columns = table.Schema.Columns!;

        int i = 0;
        foreach (TableColumnSchema column in columns)
        {
            if (!SchemaElementStateRules.IsReadable(column))
                continue;

            createTableSql.Append(' ');
            createTableSql.Append('`');
            createTableSql.Append(column.Name);
            createTableSql.Append('`');
            createTableSql.Append(' ');
            createTableSql.Append(GetSQLType(column));
            createTableSql.Append(' ');
            createTableSql.Append(GetSQLConstraint(column));
            createTableSql.Append(GetSQLDefault(column));
            createTableSql.Append(GetSQLComment(column.Comment));
            createTableSql.Append(',');
            i++;
        }

        foreach (KeyValuePair<string, TableIndexSchema> kv in table.Indexes)
        {
            if (!SchemaElementStateRules.IsReadableIndex(table.Schema, kv.Value))
                continue;

            string cols = string.Join(", ", kv.Value.Columns.Select(c => "`" + c + "`"));

            // Covering indexes render their stored/payload columns as a trailing INCLUDE (...) clause,
            // matching the CREATE INDEX syntax so SHOW CREATE TABLE round-trips through re-parse.
            string include = kv.Value.IncludeColumns.Length > 0
                ? " INCLUDE (" + string.Join(", ", kv.Value.IncludeColumns.Select(c => "`" + c + "`")) + ")"
                : "";

            // The PRIMARY KEY line has no inline COMMENT form to round-trip through, which is why
            // COMMENT ON INDEX rejects the primary index outright.
            string indexComment = GetSQLComment(kv.Value.Comment);

            if (kv.Key == CamusDBConstants.PrimaryKeyInternalName)
                createTableSql.Append(" PRIMARY KEY (" + cols + "),");
            else if (kv.Value.Type == IndexType.Unique)
                createTableSql.Append(" UNIQUE KEY `" + kv.Key + "` (" + cols + ")" + include + indexComment + ",");
            else
                createTableSql.Append(" KEY `" + kv.Key + "` (" + cols + ")" + include + indexComment + ",");
        }

        if (table.Schema.CheckConstraints is { Count: > 0 } checks)
        {
            foreach (CheckConstraintSchema cc in checks)
                createTableSql.Append($" CONSTRAINT `{cc.Name}` CHECK ({cc.Expression}),");
        }

        // Remove trailing comma and close
        if (createTableSql[^1] == ',')
            createTableSql.Length--;

        createTableSql.Append(')');
        createTableSql.Append(GetSQLComment(table.Schema.Comment));
        createTableSql.Append(GetSQLSettings(table.Schema.Settings));
        createTableSql.Append(';');

        yield return new QueryResultRow(default, new Dictionary<string, ColumnValue>(StringComparer.OrdinalIgnoreCase)
        {
            { "Table", new ColumnValue(ColumnType.String, table.Name) },
            { "Create Table", new ColumnValue(ColumnType.String, createTableSql.ToString()) }
        });
    }

    /// <summary>
    /// One row describing the current database. <paramref name="comment"/> comes from the registry
    /// entry (the descriptor does not carry it). Unlike <see cref="ShowCreateTable"/>, this surface
    /// cannot distinguish an unset comment from an empty one — both render as an empty string,
    /// because the row shape is fixed and every column is a plain String value.
    /// </summary>
    internal async IAsyncEnumerable<QueryResultRow> ShowDatabase(DatabaseDescriptor database, string? comment = null)
    {
        await Task.CompletedTask;

        yield return new QueryResultRow(default, new Dictionary<string, ColumnValue>(StringComparer.OrdinalIgnoreCase)
        {
            { "database", new ColumnValue(ColumnType.String, database.Name) },
            { "comment", new ColumnValue(ColumnType.String, comment ?? "") }
        });
    }

    /// <summary>
    /// Returns one row per stored grant for <paramref name="user"/>: the display object
    /// (<c>*.*</c> / <c>db.*</c> / <c>db.table</c>) and the comma-joined privilege names. These are the
    /// grants as stored, not the effective (broader-scope-expanded) set. A user with no grants yields
    /// no rows.
    /// </summary>
    internal async IAsyncEnumerable<QueryResultRow> ShowGrants(string user, IReadOnlyList<GrantRecord> grants)
    {
        await Task.CompletedTask;

        foreach (GrantRecord grant in grants)
        {
            yield return new QueryResultRow(default, new Dictionary<string, ColumnValue>(StringComparer.OrdinalIgnoreCase)
            {
                { "user", new ColumnValue(ColumnType.String, user) },
                { "object", new ColumnValue(ColumnType.String, grant.Scope.DisplayObject()) },
                { "privileges", new ColumnValue(ColumnType.String, FormatPrivileges(grant.Privileges)) }
            });
        }
    }

    /// <summary>Renders a privilege bitmask as an uppercase, comma-separated list (<c>ALL PRIVILEGES</c> when complete).</summary>
    private static string FormatPrivileges(Privilege privileges)
    {
        if (privileges == Privilege.All)
            return "ALL PRIVILEGES";

        List<string> names = [];
        if (privileges.HasFlag(Privilege.Select)) names.Add("SELECT");
        if (privileges.HasFlag(Privilege.Insert)) names.Add("INSERT");
        if (privileges.HasFlag(Privilege.Update)) names.Add("UPDATE");
        if (privileges.HasFlag(Privilege.Delete)) names.Add("DELETE");
        if (privileges.HasFlag(Privilege.CreateTable)) names.Add("CREATE TABLE");
        if (privileges.HasFlag(Privilege.Drop)) names.Add("DROP");
        if (privileges.HasFlag(Privilege.Alter)) names.Add("ALTER");
        if (privileges.HasFlag(Privilege.Index)) names.Add("INDEX");
        if (privileges.HasFlag(Privilege.Create)) names.Add("CREATE");
        return string.Join(", ", names);
    }

    /// <summary>
    /// Returns one row per transitive descendant of <paramref name="target"/> found in
    /// <paramref name="allEntries"/>: depth 1 = direct children, 2 = grandchildren, etc.
    /// An entry is a descendant when <paramref name="target"/>'s id appears in its
    /// <see cref="DatabaseRegistryEntry.Ancestors"/> list. Rows are ordered depth-ascending then
    /// database-name ascending.
    ///
    /// <para>When <paramref name="principal"/> is non-null (authentication is on) a descendant is listed
    /// only if the caller holds a grant reaching into it, and the <c>parent</c> column is blanked when
    /// the parent database itself is not visible — a branch the caller may use must not disclose the
    /// name of one it may not. Depths stay relative to <paramref name="target"/> in the real tree, so a
    /// filtered-out intermediate branch leaves a gap in the depth sequence rather than renumbering the
    /// rows around it.</para>
    /// </summary>
    internal async IAsyncEnumerable<QueryResultRow> ShowBranches(
        IReadOnlyList<DatabaseRegistryEntry> allEntries,
        DatabaseRegistryEntry target,
        Principal? principal = null)
    {
        await Task.CompletedTask;

        // Build id→name map for resolving parent names.
        Dictionary<string, string> idToName = new(allEntries.Count, StringComparer.Ordinal);
        foreach (DatabaseRegistryEntry e in allEntries)
            idToName[e.Id] = e.Name;

        // Collect (depth, entry) for every entry that descends from target.
        List<(int depth, DatabaseRegistryEntry entry)> descendants = new();
        foreach (DatabaseRegistryEntry e in allEntries)
        {
            for (int i = 0; i < e.Ancestors.Count; i++)
            {
                if (e.Ancestors[i].DatabaseId == target.Id)
                {
                    descendants.Add((i + 1, e));
                    break;
                }
            }
        }

        descendants.Sort((a, b) =>
        {
            int d = a.depth.CompareTo(b.depth);
            return d != 0 ? d : string.Compare(a.entry.Name, b.entry.Name, StringComparison.Ordinal);
        });

        foreach ((int depth, DatabaseRegistryEntry e) in descendants)
        {
            if (principal is not null && !principal.CanSeeDatabase(e.Id))
                continue;

            string parentName = e.Ancestors.Count > 0 && idToName.TryGetValue(e.Ancestors[0].DatabaseId, out string? pn)
                && (principal is null || principal.CanSeeDatabase(e.Ancestors[0].DatabaseId))
                ? pn : "";
            string forkTs = e.Ancestors.Count > 0
                ? e.Ancestors[0].ForkTimestamp.ToString()
                : "";

            yield return new QueryResultRow(default, new Dictionary<string, ColumnValue>(StringComparer.OrdinalIgnoreCase)
            {
                { "database",       new ColumnValue(ColumnType.String, e.Name) },
                { "id",             new ColumnValue(ColumnType.String, e.Id) },
                { "depth",          new ColumnValue(ColumnType.Integer64, depth) },
                { "parent",         new ColumnValue(ColumnType.String, parentName) },
                { "fork_timestamp", new ColumnValue(ColumnType.String, forkTs) },
            });
        }
    }

    /// <summary>
    /// Returns one row per entry in <paramref name="target"/>'s
    /// <see cref="DatabaseRegistryEntry.Ancestors"/> list, ordered by depth ascending (nearest parent
    /// first). A root database (no ancestors) returns an empty result set. Ancestor names are
    /// resolved from the id-to-name map built from <paramref name="allEntries"/>.
    ///
    /// <para>When <paramref name="principal"/> is non-null (authentication is on) an ancestor is listed
    /// only if the caller holds a grant reaching into it. Both the name and the id are withheld for a
    /// filtered ancestor — the id is a usable handle (<c>CREATE DATABASE … RELINK TO</c>), so the row is
    /// dropped whole rather than blanked. Depth remains the true position in the chain, which is why a
    /// filtered ancestor shows up as a gap in the sequence.</para>
    /// </summary>
    internal async IAsyncEnumerable<QueryResultRow> ShowAncestors(
        DatabaseRegistryEntry target,
        IReadOnlyList<DatabaseRegistryEntry> allEntries,
        Principal? principal = null)
    {
        await Task.CompletedTask;

        Dictionary<string, string> idToName = new(allEntries.Count, StringComparer.Ordinal);
        foreach (DatabaseRegistryEntry e in allEntries)
            idToName[e.Id] = e.Name;

        for (int i = 0; i < target.Ancestors.Count; i++)
        {
            DatabaseBranchAncestor anc = target.Ancestors[i];

            if (principal is not null && !principal.CanSeeDatabase(anc.DatabaseId))
                continue;

            idToName.TryGetValue(anc.DatabaseId, out string? ancestorName);

            yield return new QueryResultRow(default, new Dictionary<string, ColumnValue>(StringComparer.OrdinalIgnoreCase)
            {
                { "database",       new ColumnValue(ColumnType.String, ancestorName ?? anc.DatabaseId) },
                { "id",             new ColumnValue(ColumnType.String, anc.DatabaseId) },
                { "depth",          new ColumnValue(ColumnType.Integer64, i + 1) },
                { "fork_timestamp", new ColumnValue(ColumnType.String, anc.ForkTimestamp.ToString()) },
            });
        }
    }

    /// <summary>
    /// Lists the registered databases, optionally narrowed by a LIKE <paramref name="pattern"/>.
    ///
    /// <para>When <paramref name="principal"/> is non-null (authentication is on) a database is listed
    /// only if the caller holds a grant reaching into it — global, on the database, or on any single
    /// table inside it (see <see cref="Principal.CanSeeDatabase"/>). A caller with no grants sees an
    /// empty list instead of an error, so the statement does not disclose the names of databases it
    /// filtered out. A null principal (authentication disabled) lists everything.</para>
    /// </summary>
    internal async IAsyncEnumerable<QueryResultRow> ShowDatabases(IReadOnlyList<DatabaseRegistryEntry> entries, string? pattern = null, Principal? principal = null)
    {
        await Task.CompletedTask;

        foreach (DatabaseRegistryEntry entry in entries)
        {
            if (pattern is not null && !LikeMatch(entry.Name, pattern))
                continue;

            if (principal is not null && !principal.CanSeeDatabase(entry.Id))
                continue;

            yield return new QueryResultRow(default, new Dictionary<string, ColumnValue>(StringComparer.OrdinalIgnoreCase)
            {
                { "Database", new ColumnValue(ColumnType.String, entry.Name) }
            });
        }
    }

    /// <summary>
    /// Lists root databases that were dropped but retained as recoverable orphans. Backing data comes
    /// from the registry (scanned by the caller, since <see cref="SchemaQuerier"/> has no registry
    /// handle), mirroring <see cref="ShowDatabases"/>. Each row is the orphan's id (to feed
    /// <c>CREATE DATABASE ... RELINK TO</c>), its former name, drop time, and reclamation deadline.
    /// </summary>
    internal async IAsyncEnumerable<QueryResultRow> ShowOrphanDatabases(IReadOnlyList<OrphanDatabaseRecord> orphans)
    {
        await Task.CompletedTask;

        foreach (OrphanDatabaseRecord orphan in orphans)
            yield return OrphanRow(orphan.Id, orphan.FormerName, orphan.DroppedAt);
    }

    /// <summary>
    /// Reports the embedded Kommander/Kahuna metrics observed by this process, optionally narrowed by a
    /// LIKE <paramref name="pattern"/> on the metric name.
    ///
    /// <para>Node-local by construction: the meters belong to this process, so the rows describe
    /// <paramref name="node"/> alone and are never gathered from peers. A null
    /// <paramref name="collector"/> — metric collection turned off by configuration — contributes no
    /// meter rows rather than raising, so a script polling a fleet gets the same shape from every node
    /// whether or not that node has collection enabled.</para>
    ///
    /// <para><paramref name="engineCounters"/> carries counters the engine maintains itself rather than
    /// through a meter — the row-level TTL sweep's totals, for instance. They are merged into the same
    /// ordering as the meter rows (by source, then metric, then tags) so the output stays sorted, and
    /// they follow the same all-or-nothing rule as everything else: with collection disabled the
    /// statement reports nothing at all, so a script polling a fleet sees one consistent shape rather
    /// than a partial result that looks like a node with no activity.</para>
    /// </summary>
    internal async IAsyncEnumerable<QueryResultRow> ShowEngineStats(
        EngineMetricsCollector? collector,
        string? pattern,
        string node,
        IReadOnlyList<EngineMetricRow>? engineCounters = null)
    {
        await Task.CompletedTask;

        if (collector is null)
            yield break;

        ColumnValue nodeValue = new(ColumnType.String, node);

        List<EngineMetricRow> rows = [.. collector.Snapshot()];

        if (engineCounters is { Count: > 0 })
        {
            rows.AddRange(engineCounters);
            rows.Sort(static (a, b) =>
            {
                int cmp = string.CompareOrdinal(a.Source, b.Source);
                if (cmp != 0)
                    return cmp;

                cmp = string.CompareOrdinal(a.Metric, b.Metric);
                return cmp != 0 ? cmp : string.CompareOrdinal(a.Tags, b.Tags);
            });
        }

        foreach (EngineMetricRow metric in rows)
        {
            if (pattern is not null && !LikeMatch(metric.Metric, pattern))
                continue;

            yield return new QueryResultRow(default, new Dictionary<string, ColumnValue>(StringComparer.OrdinalIgnoreCase)
            {
                { "node",   nodeValue },
                { "source", new ColumnValue(ColumnType.String, metric.Source) },
                { "metric", new ColumnValue(ColumnType.String, metric.Metric) },
                { "tags",   new ColumnValue(ColumnType.String, metric.Tags) },
                { "kind",   new ColumnValue(ColumnType.String, metric.Kind.ToString().ToLowerInvariant()) },
                { "count",  new ColumnValue(ColumnType.Integer64, metric.Count) },
                { "total",  Float(metric.Total) },
                { "min",    Float(metric.Min) },
                { "max",    Float(metric.Max) },
                { "last",   Float(metric.Last) },
            });
        }
    }

    /// <summary>
    /// Reports the configuration this engine is running, optionally narrowed by a LIKE
    /// <paramref name="pattern"/> on the variable name.
    ///
    /// <para>The rows come from <paramref name="options"/> — the instance the engine was constructed
    /// with — and not from re-reading the configuration file. That distinction is the point of the
    /// statement: a value overridden by an environment variable or a command-line flag after the file
    /// was read differs from what the file says, and it is the resolved value the engine actually
    /// obeys. Secrets are masked by <see cref="ConfigVariableCatalog"/> before they reach here.</para>
    ///
    /// <para>Node-local: it describes the node that served the statement and is never gathered from
    /// peers, because nodes in a cluster can legitimately be configured differently and answering
    /// from the leader would hide exactly the drift an operator is looking for.</para>
    /// </summary>
    internal async IAsyncEnumerable<QueryResultRow> ShowVariables(CamusDBOptions options, string? pattern)
    {
        await Task.CompletedTask;

        foreach (ConfigVariable variable in ConfigVariableCatalog.Describe(options))
        {
            if (pattern is not null && !LikeMatch(variable.Name, pattern))
                continue;

            yield return new QueryResultRow(default, new Dictionary<string, ColumnValue>(StringComparer.OrdinalIgnoreCase)
            {
                { "variable",   new ColumnValue(ColumnType.String, variable.Name) },
                { "value",      Text(variable.Value) },
                { "type",       new ColumnValue(ColumnType.String, variable.Type) },
                { "default",    Text(variable.Default) },
                { "source",     new ColumnValue(ColumnType.String, SourceLabel(variable.Source)) },
                { "mutability", new ColumnValue(ColumnType.String, MutabilityLabel(variable.Mutability)) },
                { "scope",      new ColumnValue(ColumnType.String, ScopeLabel(variable.Scope)) },
            });
        }
    }

    /// <summary>
    /// Lists the runtime cluster-settings overlay: the entries <c>SET CLUSTER SETTING</c> put in
    /// force fleet-wide and no <c>RESET</c> has dropped. Values are the stored scalar text, so a
    /// row pastes straight back into a <c>SET</c> statement or a configuration file.
    /// </summary>
    internal async IAsyncEnumerable<QueryResultRow> ShowClusterSettings(
        IReadOnlyList<(string Key, string Value)> entries, string? pattern)
    {
        await Task.CompletedTask;

        foreach ((string key, string value) in entries)
        {
            if (pattern is not null && !LikeMatch(key, pattern))
                continue;

            yield return new QueryResultRow(default, new Dictionary<string, ColumnValue>(StringComparer.OrdinalIgnoreCase)
            {
                { "setting", new ColumnValue(ColumnType.String, key) },
                { "value",   new ColumnValue(ColumnType.String, value) },
            });
        }
    }

    /// <summary>Spells a mutability class the way an operator reads it: can this change live or not.</summary>
    private static string MutabilityLabel(ConfigMutability mutability)
        => mutability == ConfigMutability.Runtime ? "runtime" : "restart";

    /// <summary>Spells a scope the way an operator reads it: must the fleet agree, or is it per-node.</summary>
    private static string ScopeLabel(ConfigScope scope)
        => scope == ConfigScope.Cluster ? "cluster" : "node";

    /// <summary>Renders an optional string, an unset setting becoming SQL NULL rather than empty.</summary>
    private static ColumnValue Text(string? value)
        => value is null ? ColumnValue.Null : new ColumnValue(ColumnType.String, value);

    /// <summary>
    /// Spells a provenance layer the way an operator names it, rather than leaking the CLR enum
    /// spelling (<c>ConfigFile</c>, <c>CommandLine</c>) into the result set.
    /// </summary>
    private static string SourceLabel(ConfigValueSource source) => source switch
    {
        ConfigValueSource.ConfigFile => "config",
        ConfigValueSource.Environment => "env",
        ConfigValueSource.CommandLine => "cli",
        // A replicated cluster setting overrode every local layer for this key — the row an
        // operator needs to be self-explanatory on a node whose behavior contradicts its own YAML.
        ConfigValueSource.Cluster => "cluster",
        _ => "default",
    };

    /// <summary>Renders an optional metric component, absent components becoming SQL NULL.</summary>
    private static ColumnValue Float(double? value)
        => value is null ? ColumnValue.Null : new ColumnValue(ColumnType.Float64, value.Value);

    /// <summary>
    /// Lists tables in <paramref name="database"/> that were dropped but retained as recoverable
    /// orphans (scanned from the per-database meta namespace). Each row is the orphan's table id (to
    /// feed <c>CREATE TABLE ... RELINK TO</c>), its former name, drop time, and reclamation deadline.
    /// </summary>
    internal async IAsyncEnumerable<QueryResultRow> ShowOrphanTables(DatabaseDescriptor database)
    {
        foreach (OrphanTableRecord orphan in await catalogs.LoadTableOrphansAsync(database).ConfigureAwait(false))
            yield return OrphanRow(orphan.TableId, orphan.FormerName, orphan.DroppedAt);
    }

    /// <summary>
    /// Builds one <c>SHOW ORPHAN …</c> row. Timestamps are rendered as UTC ISO-8601
    /// (<c>yyyy-MM-ddTHH:mm:ss.fffZ</c>) from the HLC's physical component (Unix epoch milliseconds).
    /// <c>expires_at</c> is the advisory reclamation deadline (<c>DroppedAt + OrphanRetentionMs</c>), or
    /// the literal <c>"never"</c> when automatic reclamation is disabled
    /// (<see cref="CamusDBOptions.OrphanRetentionMs"/> &lt;= 0).
    /// </summary>
    private QueryResultRow OrphanRow(string id, string formerName, HLCTimestamp droppedAt)
    {
        long retentionMs = options.OrphanRetentionMs;
        string expiresAt = retentionMs > 0 ? IsoFromUnixMs(droppedAt.L + retentionMs) : "never";

        return new QueryResultRow(default, new Dictionary<string, ColumnValue>(StringComparer.OrdinalIgnoreCase)
        {
            { "id",          new ColumnValue(ColumnType.String, id) },
            { "former_name", new ColumnValue(ColumnType.String, formerName) },
            { "dropped_at",  new ColumnValue(ColumnType.String, IsoFromUnixMs(droppedAt.L)) },
            { "expires_at",  new ColumnValue(ColumnType.String, expiresAt) },
        });
    }

    /// <summary>Formats Unix-epoch milliseconds (the HLC physical component) as UTC ISO-8601 with millisecond precision.</summary>
    private static string IsoFromUnixMs(long unixMs) =>
        DateTimeOffset.FromUnixTimeMilliseconds(unixMs).UtcDateTime.ToString("yyyy-MM-ddTHH:mm:ss.fff'Z'", CultureInfo.InvariantCulture);

    /// <summary>
    /// SQL LIKE pattern match: '%' matches any sequence, '_' matches any single character.
    /// Matching is case-sensitive (standard SQL semantics).
    /// </summary>
    private static bool LikeMatch(string value, string pattern)
    {
        int vi = 0, pi = 0;
        int starPi = -1, starVi = -1;

        while (vi < value.Length)
        {
            if (pi < pattern.Length && (pattern[pi] == '_' || pattern[pi] == value[vi]))
            {
                vi++;
                pi++;
            }
            else if (pi < pattern.Length && pattern[pi] == '%')
            {
                starPi = pi++;
                starVi = vi;
            }
            else if (starPi != -1)
            {
                pi = starPi + 1;
                vi = ++starVi;
            }
            else
            {
                return false;
            }
        }

        while (pi < pattern.Length && pattern[pi] == '%')
            pi++;

        return pi == pattern.Length;
    }

    /// <summary>
    /// Renders a column's type as re-parseable CREATE TABLE syntax. Strings carry their explicit
    /// <c>MaxLength</c> as <c>STRING(n)</c> (bare <c>STRING</c> when unbounded/default-capped);
    /// arrays render as <c>ARRAY(element)</c>. Bytes has no sized SQL form, so it always renders
    /// bare <c>BYTES</c>.
    /// </summary>
    private static string GetSQLType(TableColumnSchema column)
    {
        return column.Type switch
        {
            ColumnType.String => column.MaxLength is int n ? $"STRING({n.ToString(CultureInfo.InvariantCulture)})" : "STRING",
            ColumnType.Array => $"ARRAY({ScalarSQLType(column.ArrayElementType ?? ColumnType.Null)})",
            _ => ScalarSQLType(column.Type),
        };
    }

    /// <summary>
    /// SQL keyword for a scalar (non-sized, non-array) type. Used directly for scalar columns and
    /// for the element type of an <c>ARRAY(...)</c>.
    /// </summary>
    private static string ScalarSQLType(ColumnType type)
    {
        return type switch
        {
            ColumnType.String => "STRING",
            ColumnType.Id => "OID",
            ColumnType.Integer64 => "INT64",
            ColumnType.Float64 => "FLOAT64",
            ColumnType.Float32 => "FLOAT32",
            ColumnType.Bool => "BOOL",
            ColumnType.Bytes => "BYTES",
            ColumnType.Date => "DATE",
            ColumnType.DateTime => "DATETIME",
            ColumnType.Uuid => "UUID",
            _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Cannot render SQL type for: " + type),
        };
    }

    private static string GetSQLConstraint(TableColumnSchema column)
    {
        if (column.NotNull)
        {
            if (column.NotNullConstraintName is not null)
                return $"CONSTRAINT `{column.NotNullConstraintName}` NOT NULL";
            return "NOT NULL";
        }

        return "NULL";
    }

    /// <summary>
    /// Renders a trailing <c>COMMENT '…'</c> clause, or an empty string when
    /// <paramref name="comment"/> is null.
    ///
    /// <para>The null check is what keeps <c>IS NULL</c> and <c>IS ''</c> observably different: an
    /// absent comment emits no clause at all, while an empty one emits <c>COMMENT ''</c>. The text is
    /// escaped so the emitted DDL re-parses to the identical comment — the round-trip through the
    /// extended CREATE TABLE grammar depends on it.</para>
    /// </summary>
    private static string GetSQLComment(string? comment)
        => comment is null ? "" : " COMMENT " + RenderStringLiteral(comment);

    /// <summary>
    /// Renders a table's storage parameters as a trailing <c>SET (key = value, ...)</c> clause, or an
    /// empty string when the table sets none. Without this, a table's TTL configuration and
    /// auto-analyze opt-out are invisible to the one statement users reach for to see how a table is
    /// defined — and a configuration that cannot be seen cannot be reviewed.
    ///
    /// <para>Keys are rendered in a stable ordinal order so the output is deterministic across nodes and
    /// reopens (the settings bag has no inherent order). Values render in the literal form the parser
    /// accepts for that key — booleans and integers bare, everything else as a quoted string — so the
    /// rendered statement re-parses and re-creates the same table.</para>
    ///
    /// <para><b>Engine-owned parameters are omitted.</b> The derived <c>ttl</c> marker describes state
    /// the engine maintains, and the settings grammar rejects it from a user. Emitting it would produce
    /// output that fails when replayed — worse than omitting it, because the marker is reconstructed
    /// from the expiration column anyway, so a replay of this statement arrives at the same state.</para>
    /// </summary>
    private static string GetSQLSettings(IReadOnlyDictionary<string, string>? settings)
    {
        if (settings is null || settings.Count == 0)
            return "";

        List<string> keys = [.. settings.Keys
            .Where(static k => !TableSettings.IsEngineOwned(k))
            .OrderBy(static k => k, StringComparer.Ordinal)];

        if (keys.Count == 0)
            return "";

        StringBuilder builder = new(" WITH (");
        bool first = true;

        foreach (string key in keys)
        {
            if (!first)
                builder.Append(", ");
            first = false;

            string value = settings[key];
            builder.Append(key);
            builder.Append(" = ");
            builder.Append(IsBareLiteral(value) ? value : RenderStringLiteral(value));
        }

        return builder.Append(')').ToString();
    }

    // A value the grammar accepts unquoted: a boolean keyword or a non-negative integer.
    private static bool IsBareLiteral(string value)
    {
        if (string.Equals(value, "true", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(value, "false", StringComparison.OrdinalIgnoreCase))
            return true;

        if (value.Length == 0)
            return false;

        foreach (char c in value)
        {
            if (!char.IsAsciiDigit(c))
                return false;
        }

        return true;
    }

    /// <summary>
    /// Renders the trailing <c>DEFAULT(...)</c> clause for a column definition in
    /// <see cref="ShowCreateTable"/>, or an empty string when the column has no default. A per-row
    /// function default (e.g. <c>gen_id</c>, <c>gen_uuid_v7</c>) round-trips as
    /// <c>DEFAULT(gen_id())</c>; a constant default is emitted as a re-parseable SQL literal so the
    /// rendered <c>CREATE TABLE</c> re-creates the same default when executed.
    /// </summary>
    private static string GetSQLDefault(TableColumnSchema column)
    {
        if (column.DefaultFunction is not null)
            return $" DEFAULT({column.DefaultFunction}())";

        if (column.DefaultValue is null || column.DefaultValue.Type == ColumnType.Null)
            return "";

        ColumnValue d = column.DefaultValue;
        string literal = d.Type switch
        {
            ColumnType.String => RenderStringLiteral(d.StrValue!),
            ColumnType.Id => "'" + d.StrValue! + "'",
            ColumnType.Uuid => "'" + d.UuidValue! + "'",
            ColumnType.Bool => d.BoolValue ? "true" : "false",
            ColumnType.Integer64 => d.LongValue.ToString(CultureInfo.InvariantCulture),
            ColumnType.Float64 => d.FloatValue.ToString(CultureInfo.InvariantCulture),
            ColumnType.Float32 => ((float)d.FloatValue).ToString(CultureInfo.InvariantCulture),
            ColumnType.Date or ColumnType.DateTime => "'" + d.IsoValue! + "'",
            ColumnType.Bytes => SqlStringLiteral.QuoteBytes(d.BytesValue ?? []),
            _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Cannot render default for type: " + d.Type),
        };
        return $" DEFAULT({literal})";
    }

    /// <summary>
    /// Renders a string value as a re-parseable single-quoted SQL string literal. Delegates to
    /// <see cref="SqlStringLiteral.Quote"/> so that every value round-trips — including one holding
    /// control characters, a trailing backslash, or both quote characters.
    /// </summary>
    private static string RenderStringLiteral(string value) => SqlStringLiteral.Quote(value);

}
