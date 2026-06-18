
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Util.ObjectIds;

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

            yield return new QueryResultRow(default, new()
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

            yield return new QueryResultRow(default, new()
            {
                { "Field", new ColumnValue(ColumnType.String, column.Name) },
                { "Type", new ColumnValue(ColumnType.String, column.Type.ToString()) },
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
        if (column.DefaultValue is null)
            return new ColumnValue(ColumnType.String, "NULL");

        return column.DefaultValue.Type switch
        {
            ColumnType.Null => new ColumnValue(ColumnType.String, "NULL"),
            ColumnType.Id => new ColumnValue(ColumnType.String, column.DefaultValue.StrValue!),
            ColumnType.String => new ColumnValue(ColumnType.String, column.DefaultValue.StrValue!),
            ColumnType.Bool => new ColumnValue(ColumnType.String, column.DefaultValue.BoolValue.ToString()),
            ColumnType.Integer64 => new ColumnValue(ColumnType.String, column.DefaultValue.LongValue.ToString()),
            ColumnType.Float64 => new ColumnValue(ColumnType.String, column.DefaultValue.FloatValue.ToString()),
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

            yield return new QueryResultRow(default, new()
            {
                { "Table", new ColumnValue(ColumnType.String, table.Name) },
                { "Non_unique", new ColumnValue(ColumnType.String, index.Value.Type == IndexType.Unique ? "0" : "1") },
                { "Key_name", new ColumnValue(ColumnType.String, index.Key) },
                { "Columns", new ColumnValue(ColumnType.String, string.Join(",", index.Value.Columns)) },
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
            createTableSql.Append(GetSQLType(column.Type));
            createTableSql.Append(' ');
            createTableSql.Append(GetSQLConstraint(column));
            createTableSql.Append(',');
            i++;
        }

        foreach (KeyValuePair<string, TableIndexSchema> kv in table.Indexes)
        {
            if (!SchemaElementStateRules.IsReadableIndex(table.Schema, kv.Value))
                continue;

            string cols = string.Join(", ", kv.Value.Columns.Select(c => "`" + c + "`"));

            if (kv.Key == CamusDBConfig.PrimaryKeyInternalName)
                createTableSql.Append(" PRIMARY KEY (" + cols + "),");
            else if (kv.Value.Type == IndexType.Unique)
                createTableSql.Append(" UNIQUE KEY `" + kv.Key + "` (" + cols + "),");
            else
                createTableSql.Append(" KEY `" + kv.Key + "` (" + cols + "),");
        }

        // Remove trailing comma and close
        if (createTableSql[^1] == ',')
            createTableSql.Length--;

        createTableSql.Append(");");

        yield return new QueryResultRow(default, new()
        {
            { "Table", new ColumnValue(ColumnType.String, table.Name) },
            { "Create Table", new ColumnValue(ColumnType.String, createTableSql.ToString()) }
        });
    }

    internal async IAsyncEnumerable<QueryResultRow> ShowDatabase(DatabaseDescriptor database)
    {
        await Task.CompletedTask;

        yield return new QueryResultRow(default, new()
        {
            { "database", new ColumnValue(ColumnType.String, database.Name) }
        });
    }

    internal async IAsyncEnumerable<QueryResultRow> ShowDatabases(IReadOnlyList<DatabaseRegistryEntry> entries, string? pattern = null)
    {
        await Task.CompletedTask;

        foreach (DatabaseRegistryEntry entry in entries)
        {
            if (pattern is not null && !LikeMatch(entry.Name, pattern))
                continue;

            yield return new QueryResultRow(default, new()
            {
                { "Database", new ColumnValue(ColumnType.String, entry.Name) }
            });
        }
    }

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

    private static string GetSQLType(ColumnType type)
    {
        return type switch
        {
            ColumnType.String => "STRING",
            ColumnType.Id => "OID",
            ColumnType.Integer64 => "INT64",
            ColumnType.Float64 => "FLOAT64",
            ColumnType.Bool => "BOOL",
            _ => throw new NotImplementedException(),
        };
    }

    private static string GetSQLConstraint(TableColumnSchema column)
    {
        //if (column.Primary)
        //    return "PRIMARY KEY";

        if (column.NotNull)
            return "NOT NULL";

        return "NULL";
    }

}
