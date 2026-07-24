
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Tests that ExecuteSQLQuery populates a QuerySchemaHolder with the correct ordered column
/// schema for every query shape: single-source SELECT, SELECT *, joins, FROM-less SELECT,
/// and SHOW commands.
/// </summary>
[NonParallelizable]
public sealed class TestPositionalResultRows : SharedNodeBaseTest
{
    private static async Task<(IReadOnlyList<DerivedColumnSchema> schema, List<QueryResultRow> rows)> ExecQuery(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        QuerySchemaHolder schemaHolder = new();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: parameters);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket, schemaOut: schemaHolder);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return (schemaHolder.Schema, rows);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Empty result — schema still present
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task EmptyTable_SchemaPresent_RowsEmpty()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction ddlTx0 = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddlTx0, dbname,
            "CREATE TABLE t (a INT64 NOT NULL, b STRING NOT NULL, PRIMARY KEY (a))", null));
        await database.Transactions.CommitAsync(ddlTx0);

        (IReadOnlyList<DerivedColumnSchema> schema, List<QueryResultRow> rows) =
            await ExecQuery(executor, database, dbname, "SELECT a, b FROM t");

        Assert.AreEqual(0, rows.Count, "rows");
        Assert.AreEqual(2, schema.Count, "schema columns");
        Assert.AreEqual("a", schema[0].Name);
        Assert.AreEqual(ColumnType.Integer64, schema[0].Type);
        Assert.AreEqual("b", schema[1].Name);
        Assert.AreEqual(ColumnType.String, schema[1].Type);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Non-empty — positional values match inserted data
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task NonEmpty_PositionalValuesMatchInserted()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction ddlTx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddlTx, dbname,
            "CREATE TABLE t (id INT64 NOT NULL, name STRING NOT NULL, PRIMARY KEY (id))", null));
        await database.Transactions.CommitAsync(ddlTx);

        KvTransaction insertTx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(insertTx, dbname,
            "INSERT INTO t (id, name) VALUES (42, \"hello\")", null));
        await database.Transactions.CommitAsync(insertTx);

        (IReadOnlyList<DerivedColumnSchema> schema, List<QueryResultRow> rows) =
            await ExecQuery(executor, database, dbname, "SELECT id, name FROM t");

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(2, schema.Count);
        Assert.AreEqual("id",   schema[0].Name);
        Assert.AreEqual(ColumnType.Integer64, schema[0].Type);
        Assert.AreEqual("name", schema[1].Name);
        Assert.AreEqual(ColumnType.String, schema[1].Type);

        // Values are in the dictionary; the schema tells us the correct key order.
        Assert.AreEqual(42L,      rows[0].Row["id"].LongValue);
        Assert.AreEqual("hello",  rows[0].Row["name"].StrValue);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Alias / expression column
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task AliasExpression_SchemaCarriesAliasName()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction ddlTx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddlTx, dbname,
            "CREATE TABLE t (x INT64 NOT NULL, PRIMARY KEY (x))", null));
        await database.Transactions.CommitAsync(ddlTx);

        KvTransaction insertTx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(insertTx, dbname,
            "INSERT INTO t (x) VALUES (10)", null));
        await database.Transactions.CommitAsync(insertTx);

        (IReadOnlyList<DerivedColumnSchema> schema, List<QueryResultRow> rows) =
            await ExecQuery(executor, database, dbname, "SELECT x + 1 AS total FROM t");

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(1, schema.Count);
        Assert.AreEqual("total", schema[0].Name);
        // Arithmetic expressions over column refs fall back to String per the spec (type
        // inference only resolves simple column refs and aggregate functions); the numeric
        // value still round-trips correctly as a JSON number.
        Assert.IsTrue(schema[0].Type is ColumnType.Integer64 or ColumnType.String);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // SELECT * — expanded to actual readable columns
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task SelectStar_EmptyTable_AllColumnsInSchema()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction ddlTx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddlTx, dbname,
            "CREATE TABLE t (id OID NOT NULL, val STRING NOT NULL, score FLOAT64, PRIMARY KEY (id))", null));
        await database.Transactions.CommitAsync(ddlTx);

        (IReadOnlyList<DerivedColumnSchema> schema, List<QueryResultRow> rows) =
            await ExecQuery(executor, database, dbname, "SELECT * FROM t");

        Assert.AreEqual(0, rows.Count);
        Assert.AreEqual(3, schema.Count);
        Assert.AreEqual("id",    schema[0].Name);
        Assert.AreEqual(ColumnType.Id,      schema[0].Type);
        Assert.AreEqual("val",   schema[1].Name);
        Assert.AreEqual(ColumnType.String,   schema[1].Type);
        Assert.AreEqual("score", schema[2].Name);
        Assert.AreEqual(ColumnType.Float64,  schema[2].Type);
    }

    [Test]
    public async Task SelectStar_NonEmpty_ValuesEncodePositionally()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction ddlTx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddlTx, dbname,
            "CREATE TABLE t (id INT64 NOT NULL, val STRING NOT NULL, PRIMARY KEY (id))", null));
        await database.Transactions.CommitAsync(ddlTx);

        KvTransaction insTx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(insTx, dbname,
            "INSERT INTO t (id, val) VALUES (7, \"seven\")", null));
        await database.Transactions.CommitAsync(insTx);

        (IReadOnlyList<DerivedColumnSchema> schema, List<QueryResultRow> rows) =
            await ExecQuery(executor, database, dbname, "SELECT * FROM t");

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(2, schema.Count);
        Assert.AreEqual("id", schema[0].Name);
        Assert.AreEqual("val", schema[1].Name);

        // Encoding the real cursor row against the derived schema must not lose values.
        object?[] encoded = CompactRowEncoder.EncodeRow(rows[0].Row, schema);
        CollectionAssert.AreEqual(new object?[] { 7L, "seven" }, encoded);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // SELECT * over a JOIN — schema names must match the qualified row keys
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task SelectStar_OverJoin_ValuesEncodePositionally()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction ddlTx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddlTx, dbname,
            "CREATE TABLE a (id INT64 NOT NULL, PRIMARY KEY (id))", null));
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddlTx, dbname,
            "CREATE TABLE b (id INT64 NOT NULL, aid INT64 NOT NULL, PRIMARY KEY (id))", null));
        await database.Transactions.CommitAsync(ddlTx);

        KvTransaction insTx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(insTx, dbname, "INSERT INTO a (id) VALUES (1)", null));
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(insTx, dbname, "INSERT INTO b (id, aid) VALUES (10, 1)", null));
        await database.Transactions.CommitAsync(insTx);

        (IReadOnlyList<DerivedColumnSchema> schema, List<QueryResultRow> rows) =
            await ExecQuery(executor, database, dbname, "SELECT * FROM a JOIN b ON a.id = b.aid");

        Assert.AreEqual(1, rows.Count);

        // Join columns are qualified so a.id and b.id are distinct positions (not a collapsed "id").
        Assert.AreEqual(3, schema.Count);
        Assert.AreEqual("a.id", schema[0].Name);
        Assert.AreEqual("b.id", schema[1].Name);
        Assert.AreEqual("b.aid", schema[2].Name);

        // Every schema name must resolve in the qualified-keyed join row — the regression this guards
        // against encoded all-null values because the schema used bare names.
        object?[] encoded = CompactRowEncoder.EncodeRow(rows[0].Row, schema);
        CollectionAssert.AreEqual(new object?[] { 1L, 10L, 1L }, encoded);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // FROM-less SELECT
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task FromlessSelect_SchemaFromExpressionTypes()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        (IReadOnlyList<DerivedColumnSchema> schema, List<QueryResultRow> rows) =
            await ExecQuery(executor, database, dbname, "SELECT 1 + 1 AS result");

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(1, schema.Count);
        Assert.AreEqual("result", schema[0].Name);
        Assert.AreEqual(ColumnType.Integer64, schema[0].Type);
    }

    [Test]
    public async Task FromlessSelect_MultipleExpressions_SchemaOrdered()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        (IReadOnlyList<DerivedColumnSchema> schema, List<QueryResultRow> rows) =
            await ExecQuery(executor, database, dbname, "SELECT 42, \"hello\", true");

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(3, schema.Count);
        Assert.AreEqual("0", schema[0].Name);
        Assert.AreEqual(ColumnType.Integer64, schema[0].Type);
        Assert.AreEqual("1", schema[1].Name);
        Assert.AreEqual(ColumnType.String, schema[1].Type);
        Assert.AreEqual("2", schema[2].Name);
        Assert.AreEqual(ColumnType.Bool, schema[2].Type);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // SHOW commands — fixed schemas
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task ShowTables_SchemaHasSingleTablesColumn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction ddlTx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddlTx, dbname,
            "CREATE TABLE mytable (id INT64 NOT NULL, PRIMARY KEY (id))", null));
        await database.Transactions.CommitAsync(ddlTx);

        (IReadOnlyList<DerivedColumnSchema> schema, List<QueryResultRow> rows) =
            await ExecQuery(executor, database, dbname, "SHOW TABLES");

        Assert.AreEqual(1, schema.Count);
        Assert.AreEqual("tables", schema[0].Name);
        Assert.AreEqual(ColumnType.String, schema[0].Type);
        Assert.IsTrue(rows.Count >= 1);
        Assert.IsTrue(rows.Any(r => r.Row["tables"].StrValue == "mytable"));
    }

    [Test]
    public async Task ShowColumns_SchemaHasMysqlCompatColumns()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction ddlTx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddlTx, dbname,
            "CREATE TABLE t (id INT64 NOT NULL, name STRING, PRIMARY KEY (id))", null));
        await database.Transactions.CommitAsync(ddlTx);

        (IReadOnlyList<DerivedColumnSchema> schema, _) =
            await ExecQuery(executor, database, dbname, "SHOW COLUMNS FROM t");

        string[] expectedNames = ["Field", "Type", "Null", "Key", "Default", "Extra"];
        Assert.AreEqual(expectedNames.Length, schema.Count);
        for (int i = 0; i < expectedNames.Length; i++)
        {
            Assert.AreEqual(expectedNames[i], schema[i].Name, $"schema[{i}].Name");
            Assert.AreEqual(ColumnType.String, schema[i].Type, $"schema[{i}].Type");
        }
    }

    [Test]
    public async Task ShowIndexes_SchemaColumns()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction ddlTx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddlTx, dbname,
            "CREATE TABLE t (id INT64 NOT NULL, PRIMARY KEY (id))", null));
        await database.Transactions.CommitAsync(ddlTx);

        (IReadOnlyList<DerivedColumnSchema> schema, _) =
            await ExecQuery(executor, database, dbname, "SHOW INDEXES FROM t");

        string[] expectedNames = ["Table", "Non_unique", "Key_name", "Columns", "Include", "Index_type"];
        Assert.AreEqual(expectedNames.Length, schema.Count);
        for (int i = 0; i < expectedNames.Length; i++)
            Assert.AreEqual(expectedNames[i], schema[i].Name, $"schema[{i}].Name");
    }

    [Test]
    public async Task ShowDatabase_SchemaHasDatabaseColumn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        (IReadOnlyList<DerivedColumnSchema> schema, List<QueryResultRow> rows) =
            await ExecQuery(executor, database, dbname, "SHOW DATABASE");

        Assert.AreEqual(1, schema.Count);
        Assert.AreEqual("database", schema[0].Name);
        Assert.AreEqual(ColumnType.String, schema[0].Type);
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(dbname, rows[0].Row["database"].StrValue);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // EXPLAIN — the column schema must be populated, otherwise the positional JSON
    // response renders empty headers and empty cells (rows present, no columns).
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task Explain_PopulatesPlanColumnSchema()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction ddlTx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddlTx, dbname,
            "CREATE TABLE orders (id INT64 NOT NULL, total FLOAT64, PRIMARY KEY (id))", null));
        await database.Transactions.CommitAsync(ddlTx);

        (IReadOnlyList<DerivedColumnSchema> schema, List<QueryResultRow> rows) =
            await ExecQuery(executor, database, dbname, "EXPLAIN SELECT * FROM orders");

        string[] expectedNames = ["stage", "node", "detail", "estimated_rows", "estimated_cost"];
        Assert.AreEqual(expectedNames.Length, schema.Count, "EXPLAIN must expose its plan column schema");
        for (int i = 0; i < expectedNames.Length; i++)
            Assert.AreEqual(expectedNames[i], schema[i].Name, $"schema[{i}].Name");

        Assert.IsTrue(rows.Count >= 1, "EXPLAIN must return at least one plan row");
        // Every plan row must resolve positionally against the schema — the regression rendered
        // all-empty rows because the schema was absent.
        object?[] encoded = CompactRowEncoder.EncodeRow(rows[0].Row, schema);
        Assert.AreEqual(schema.Count, encoded.Length);
        Assert.IsNotNull(encoded[0], "stage cell must not be null");
        Assert.IsNotNull(encoded[1], "node cell must not be null");
    }

    [Test]
    public async Task ExplainAnalyze_PopulatesRuntimeColumnSchema()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction ddlTx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddlTx, dbname,
            "CREATE TABLE orders (id INT64 NOT NULL, total FLOAT64, PRIMARY KEY (id))", null));
        await database.Transactions.CommitAsync(ddlTx);

        (IReadOnlyList<DerivedColumnSchema> schema, List<QueryResultRow> rows) =
            await ExecQuery(executor, database, dbname, "EXPLAIN (ANALYZE) SELECT * FROM orders");

        string[] expectedNames =
        [
            "stage", "node", "detail", "estimated_rows", "estimated_cost",
            "actual_rows", "rows_read", "actual_time_ms", "kv_lookups", "kv_scan_entries",
        ];
        Assert.AreEqual(expectedNames.Length, schema.Count, "EXPLAIN (ANALYZE) must expose its runtime column schema");
        for (int i = 0; i < expectedNames.Length; i++)
            Assert.AreEqual(expectedNames[i], schema[i].Name, $"schema[{i}].Name");

        Assert.IsTrue(rows.Count >= 1);
    }
}
