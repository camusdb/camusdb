
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
using CamusDB.Core.SQLParser;
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

    public SchemaQuerier(CatalogsManager catalogsManager, Microsoft.Extensions.Logging.ILogger<ICamusDB> logger)
    {
        this.catalogs = catalogsManager;
    }

    internal async IAsyncEnumerable<QueryResultRow> ShowTables(DatabaseDescriptor database, string? pattern = null)
    {
        await Task.CompletedTask;

        foreach (KeyValuePair<string, TableSchema> table in database.Schema.Tables)
        {
            if (pattern is not null && !LikeMatch(table.Key, pattern))
                continue;

            yield return new QueryResultRow(default, new Dictionary<string, ColumnValue>(StringComparer.OrdinalIgnoreCase)
            {
                { "tables", new ColumnValue(ColumnType.String, table.Key) }
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
            if (kv.Key == CamusDBConfig.PrimaryKeyInternalName && kv.Value.Columns.Contains(name))
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

            if (kv.Key == CamusDBConfig.PrimaryKeyInternalName)
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
    /// </summary>
    internal async IAsyncEnumerable<QueryResultRow> ShowBranches(
        IReadOnlyList<DatabaseRegistryEntry> allEntries,
        DatabaseRegistryEntry target)
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
            string parentName = e.Ancestors.Count > 0 && idToName.TryGetValue(e.Ancestors[0].DatabaseId, out string? pn)
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
    /// </summary>
    internal async IAsyncEnumerable<QueryResultRow> ShowAncestors(
        DatabaseRegistryEntry target,
        IReadOnlyList<DatabaseRegistryEntry> allEntries)
    {
        await Task.CompletedTask;

        Dictionary<string, string> idToName = new(allEntries.Count, StringComparer.Ordinal);
        foreach (DatabaseRegistryEntry e in allEntries)
            idToName[e.Id] = e.Name;

        for (int i = 0; i < target.Ancestors.Count; i++)
        {
            DatabaseBranchAncestor anc = target.Ancestors[i];
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

    internal async IAsyncEnumerable<QueryResultRow> ShowDatabases(IReadOnlyList<DatabaseRegistryEntry> entries, string? pattern = null)
    {
        await Task.CompletedTask;

        foreach (DatabaseRegistryEntry entry in entries)
        {
            if (pattern is not null && !LikeMatch(entry.Name, pattern))
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
    /// (<see cref="CamusDBConfig.OrphanRetentionMs"/> &lt;= 0).
    /// </summary>
    private static QueryResultRow OrphanRow(string id, string formerName, HLCTimestamp droppedAt)
    {
        long retentionMs = CamusDBConfig.OrphanRetentionMs;
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
