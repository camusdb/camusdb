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
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Verifies that the SQL emitted by SHOW CREATE TABLE is parseable and semantically correct:
/// the grammar accepts PRIMARY KEY, KEY, and UNIQUE KEY inline within the CREATE TABLE
/// parentheses, which is the format the DDL generator produces.
/// </summary>
[TestFixture]
public sealed class TestShowCreateTableRoundTrip : BaseTest
{
    // Helper: run a SELECT/SHOW SQL, return all result rows.
    private static async Task<List<QueryResultRow>> QueryAsync(
        CommandExecutor executor, DatabaseDescriptor db, string dbname, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await db.Transactions.CommitAsync(tx);
        return rows;
    }

    // Helper: run a DDL SQL statement (CREATE TABLE, etc.).
    private static async Task DdlAsync(
        CommandExecutor executor, DatabaseDescriptor db, string dbname, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        await executor.ExecuteDDLSQL(ticket);
    }

    [Test]
    public async Task ShowCreateTable_OutputIsReparseable_PrimaryKeyOnly()
    {
        // Create a table with only a primary key, capture SHOW CREATE TABLE, re-execute.
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "src",
            columns: new ColumnInfo[]
            {
                new("id",   ColumnType.Id,     notNull: true),
                new("name", ColumnType.String),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
            },
            ifNotExists: false));

        List<QueryResultRow> showRows = await QueryAsync(executor, db, dbname, "SHOW CREATE TABLE src");
        string ddl = showRows[0].Row["Create Table"].StrValue!;

        // Re-execute with a different table name (replace first occurrence of `src`).
        string ddl2 = ddl.Replace("`src`", "`src2`", System.StringComparison.Ordinal);
        await DdlAsync(executor, db, dbname, ddl2);

        // Verify the recreated table is queryable.
        List<QueryResultRow> cols = await QueryAsync(executor, db, dbname, "SHOW COLUMNS FROM src2");
        Assert.That(cols.Select(r => r.Row["Field"].StrValue), Does.Contain("id"));
        Assert.That(cols.Select(r => r.Row["Field"].StrValue), Does.Contain("name"));
    }

    [Test]
    public async Task ShowCreateTable_OutputIsReparseable_WithMultiIndex()
    {
        // Table with a non-unique secondary index — DDL emits KEY `idx` (...).
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "src",
            columns: new ColumnInfo[]
            {
                new("id",   ColumnType.Id,     notNull: true),
                new("code", ColumnType.String, notNull: true),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk",
                    new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
                new(ConstraintType.IndexMulti, "code_idx",
                    new ColumnIndexInfo[] { new("code", OrderType.Ascending) }),
            },
            ifNotExists: false));

        List<QueryResultRow> showRows = await QueryAsync(executor, db, dbname, "SHOW CREATE TABLE src");
        string ddl = showRows[0].Row["Create Table"].StrValue!;

        Assert.That(ddl, Does.Contain("KEY `code_idx`"),
            "DDL must contain the secondary index in KEY syntax");

        string ddl2 = ddl.Replace("`src`", "`src2`", System.StringComparison.Ordinal);
        await DdlAsync(executor, db, dbname, ddl2);

        List<QueryResultRow> cols = await QueryAsync(executor, db, dbname, "SHOW COLUMNS FROM src2");
        Assert.That(cols.Select(r => r.Row["Field"].StrValue), Does.Contain("code"));
    }

    [Test]
    public async Task ShowCreateTable_OutputIsReparseable_WithUniqueIndex()
    {
        // Table with a unique secondary index — DDL emits UNIQUE KEY `idx` (...).
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "src",
            columns: new ColumnInfo[]
            {
                new("id",   ColumnType.Id,     notNull: true),
                new("code", ColumnType.String, notNull: true),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey,  "~pk",
                    new ColumnIndexInfo[] { new("id",   OrderType.Ascending) }),
                new(ConstraintType.IndexUnique, "code_uk",
                    new ColumnIndexInfo[] { new("code", OrderType.Ascending) }),
            },
            ifNotExists: false));

        List<QueryResultRow> showRows = await QueryAsync(executor, db, dbname, "SHOW CREATE TABLE src");
        string ddl = showRows[0].Row["Create Table"].StrValue!;

        Assert.That(ddl, Does.Contain("UNIQUE KEY `code_uk`"),
            "DDL must contain the unique index in UNIQUE KEY syntax");

        string ddl2 = ddl.Replace("`src`", "`src2`", System.StringComparison.Ordinal);
        await DdlAsync(executor, db, dbname, ddl2);

        List<QueryResultRow> cols = await QueryAsync(executor, db, dbname, "SHOW COLUMNS FROM src2");
        Assert.That(cols.Select(r => r.Row["Field"].StrValue), Does.Contain("code"));
    }

    [Test]
    public async Task ShowCreateTable_OutputIsReparseable_FullyIndexedTable()
    {
        // Full table: PK + unique index + multi-index — the most complex DDL round-trip.
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "src",
            columns: new ColumnInfo[]
            {
                new("id",    ColumnType.Id,        notNull: true),
                new("code",  ColumnType.String,    notNull: true),
                new("score", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey,  "~pk",
                    new ColumnIndexInfo[] { new("id",    OrderType.Ascending) }),
                new(ConstraintType.IndexUnique, "code_uk",
                    new ColumnIndexInfo[] { new("code",  OrderType.Ascending) }),
                new(ConstraintType.IndexMulti,  "score_idx",
                    new ColumnIndexInfo[] { new("score", OrderType.Ascending) }),
            },
            ifNotExists: false));

        List<QueryResultRow> showRows = await QueryAsync(executor, db, dbname, "SHOW CREATE TABLE src");
        string ddl = showRows[0].Row["Create Table"].StrValue!;

        Assert.That(ddl, Does.Contain("PRIMARY KEY"),        "DDL must contain PRIMARY KEY");
        Assert.That(ddl, Does.Contain("UNIQUE KEY `code_uk`"), "DDL must contain UNIQUE KEY");
        Assert.That(ddl, Does.Contain("KEY `score_idx`"),    "DDL must contain KEY");

        // Re-execute the captured DDL.
        string ddl2 = ddl.Replace("`src`", "`src2`", System.StringComparison.Ordinal);
        await DdlAsync(executor, db, dbname, ddl2);

        // Table is functional: insert and query a row.
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(tx, dbname, "src2",
        [
            new()
            {
                ["id"]    = new(ColumnType.Id,        CamusDB.Core.Util.ObjectIds.ObjectIdGenerator.Generate().ToString()),
                ["code"]  = new(ColumnType.String,    "abc"),
                ["score"] = new(ColumnType.Integer64, 42),
            }
        ]));
        await db.Transactions.CommitAsync(tx);

        List<QueryResultRow> rows = await QueryAsync(executor, db, dbname, "SELECT code, score FROM src2");
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("abc", rows[0].Row["code"].StrValue);
        Assert.AreEqual(42,    rows[0].Row["score"].LongValue);
    }

    [Test]
    public async Task ShowCreateTable_NewTypesAndSizedString_RoundTrips()
    {
        // A table covering every new data type plus a sized string. SHOW CREATE TABLE must render
        // SQL type keywords (not enum names), carry the string length and array element type, and
        // re-parse cleanly.
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "src",
            columns: new ColumnInfo[]
            {
                new("id",      ColumnType.Id,        notNull: true),
                new("name",    ColumnType.String,    maxLength: 32),
                new("ratio",   ColumnType.Float32),
                new("payload", ColumnType.Bytes),
                new("day",     ColumnType.Date),
                new("ts",      ColumnType.DateTime),
                new("tags",    ColumnType.Array,     arrayElementType: ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
            },
            ifNotExists: false));

        List<QueryResultRow> showRows = await QueryAsync(executor, db, dbname, "SHOW CREATE TABLE src");
        string ddl = showRows[0].Row["Create Table"].StrValue!;

        Assert.That(ddl, Does.Contain("STRING(32)"),  "sized string must keep its length");
        Assert.That(ddl, Does.Contain("FLOAT32"),     "float32 must render as FLOAT32");
        Assert.That(ddl, Does.Contain("BYTES"),       "bytes must render as BYTES");
        Assert.That(ddl, Does.Contain("DATETIME"),    "datetime must render as DATETIME");
        Assert.That(ddl, Does.Contain("ARRAY(INT64)"),"array must render with its element type");
        // The 'day' DATE column: it must render as DATE, but not be confused with DATETIME.
        Assert.That(ddl, Does.Match(@"`day`\s+DATE\b"), "date must render as DATE");
        // No raw C# enum names should leak through.
        Assert.That(ddl, Does.Not.Contain("Integer64"));
        Assert.That(ddl, Does.Not.Contain("Float32"));   // enum casing (SQL is FLOAT32)
        Assert.That(ddl, Does.Not.Contain("DateTime"));  // enum casing (SQL is DATETIME)

        // Re-parse the emitted DDL under a new name.
        string ddl2 = ddl.Replace("`src`", "`src2`", System.StringComparison.Ordinal);
        await DdlAsync(executor, db, dbname, ddl2);

        // The string length and array element type survive the round-trip.
        List<QueryResultRow> cols = await QueryAsync(executor, db, dbname, "SHOW COLUMNS FROM src2");
        string NameType(string field) => cols.First(r => r.Row["Field"].StrValue == field).Row["Type"].StrValue!;
        Assert.AreEqual("STRING(32)",  NameType("name"));
        Assert.AreEqual("ARRAY(INT64)", NameType("tags"));
        Assert.AreEqual("FLOAT32",     NameType("ratio"));
        Assert.AreEqual("DATETIME",    NameType("ts"));
    }

    [Test]
    public async Task ShowColumns_RendersSqlTypeNames_NotEnumNames()
    {
        // SHOW COLUMNS / DESCRIBE must report SQL type names, not C# enum names.
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "src",
            columns: new ColumnInfo[]
            {
                new("id",    ColumnType.Id,        notNull: true),
                new("count", ColumnType.Integer64),
                new("ts",    ColumnType.DateTime),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
            },
            ifNotExists: false));

        List<QueryResultRow> cols = await QueryAsync(executor, db, dbname, "DESCRIBE src");
        string Type(string field) => cols.First(r => r.Row["Field"].StrValue == field).Row["Type"].StrValue!;

        Assert.AreEqual("OID",      Type("id"));
        Assert.AreEqual("INT64",    Type("count"));   // not "Integer64"
        Assert.AreEqual("DATETIME", Type("ts"));      // not "DateTime"
    }

    [Test]
    public async Task ShowColumns_RendersDefaultsForNewTypes_WithoutThrowing()
    {
        // GetDefaultValue previously threw for new-type defaults; verify it renders them.
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "src",
            columns: new ColumnInfo[]
            {
                new("id",    ColumnType.Id, notNull: true),
                new("ratio", ColumnType.Float32, defaultValue: new ColumnValue(ColumnType.Float32, 1.5)),
                new("day",   ColumnType.Date,    defaultValue: new ColumnValue(ColumnType.Date, new System.DateTime(2026, 6, 26, 0, 0, 0, System.DateTimeKind.Utc).Ticks)),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
            },
            ifNotExists: false));

        List<QueryResultRow> cols = await QueryAsync(executor, db, dbname, "SHOW COLUMNS FROM src");
        string Default(string field) => cols.First(r => r.Row["Field"].StrValue == field).Row["Default"].StrValue!;

        Assert.AreEqual("1.5",        Default("ratio"));
        Assert.AreEqual("2026-06-26", Default("day"));
    }

    [Test]
    public async Task ShowCreateTable_RendersCheckConstraint()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname,
            "CREATE TABLE products (id object_id PRIMARY KEY, price int64 CHECK (price > 0), name string)", null));
        await db.Transactions.CommitAsync(tx);

        List<QueryResultRow> showRows = await QueryAsync(executor, db, dbname, "SHOW CREATE TABLE products");
        string ddl = showRows[0].Row["Create Table"].StrValue!;

        Assert.IsTrue(ddl.Contains("CHECK"), $"Expected CHECK in DDL: {ddl}");
        Assert.IsTrue(ddl.Contains("price > 0"), $"Expected expression 'price > 0' in DDL: {ddl}");
        Assert.IsTrue(ddl.Contains("CONSTRAINT"), $"Expected CONSTRAINT keyword in DDL: {ddl}");
    }

    [Test]
    public async Task ShowCreateTable_CheckConstraintOutputIsReparseable()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname,
            "CREATE TABLE products (id object_id PRIMARY KEY, price int64, CONSTRAINT chk_price CHECK (price > 0))", null));
        await db.Transactions.CommitAsync(tx);

        List<QueryResultRow> showRows = await QueryAsync(executor, db, dbname, "SHOW CREATE TABLE products");
        string ddl = showRows[0].Row["Create Table"].StrValue!;

        // Replace table name and re-execute — the rendered DDL must be valid SQL.
        string ddl2 = ddl.Replace("`products`", "`products2`", System.StringComparison.Ordinal);
        await DdlAsync(executor, db, dbname, ddl2);

        // The recreated table must have the same check constraint enforced.
        KvTransaction tx2 = await db.Transactions.BeginAsync();
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx2, dbname,
                "INSERT INTO products2 (id, price) VALUES (gen_id(), -5)", null)))!;
        Assert.IsTrue(ex.Message.Contains("chk_price") || ex.Message.Contains("CHECK"));
        await db.Transactions.RollbackAsync(tx2);
    }

    [Test]
    public async Task ShowCreateTable_MultipleCheckConstraintsAllRendered()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname,
            "CREATE TABLE employees (" +
            "  id object_id PRIMARY KEY, " +
            "  salary int64, " +
            "  age int64, " +
            "  CONSTRAINT chk_salary CHECK (salary > 0), " +
            "  CONSTRAINT chk_age CHECK (age >= 18))", null));
        await db.Transactions.CommitAsync(tx);

        List<QueryResultRow> showRows = await QueryAsync(executor, db, dbname, "SHOW CREATE TABLE employees");
        string ddl = showRows[0].Row["Create Table"].StrValue!;

        Assert.IsTrue(ddl.Contains("chk_salary"), $"Expected chk_salary in DDL: {ddl}");
        Assert.IsTrue(ddl.Contains("salary > 0"), $"Expected salary > 0 in DDL: {ddl}");
        Assert.IsTrue(ddl.Contains("chk_age"), $"Expected chk_age in DDL: {ddl}");
        Assert.IsTrue(ddl.Contains("age >= 18"), $"Expected age >= 18 in DDL: {ddl}");
    }

    [Test]
    public async Task ShowCreateTable_AfterAlterAddConstraint_RendersCheck()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname,
            "CREATE TABLE items (id object_id PRIMARY KEY, qty int64)", null));
        await db.Transactions.CommitAsync(tx);

        KvTransaction tx2 = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx2, dbname,
            "ALTER TABLE items ADD CONSTRAINT chk_qty CHECK (qty >= 0)", null));
        await db.Transactions.CommitAsync(tx2);

        List<QueryResultRow> showRows = await QueryAsync(executor, db, dbname, "SHOW CREATE TABLE items");
        string ddl = showRows[0].Row["Create Table"].StrValue!;

        Assert.IsTrue(ddl.Contains("chk_qty"), $"Expected chk_qty in DDL: {ddl}");
        Assert.IsTrue(ddl.Contains("qty >= 0"), $"Expected qty >= 0 in DDL: {ddl}");
    }

    [Test]
    public async Task ShowCreateTable_AfterAlterDropConstraint_CheckNotRendered()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname,
            "CREATE TABLE items (id object_id PRIMARY KEY, qty int64, CONSTRAINT chk_qty CHECK (qty >= 0))", null));
        await db.Transactions.CommitAsync(tx);

        KvTransaction tx2 = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx2, dbname,
            "ALTER TABLE items DROP CONSTRAINT chk_qty", null));
        await db.Transactions.CommitAsync(tx2);

        List<QueryResultRow> showRows = await QueryAsync(executor, db, dbname, "SHOW CREATE TABLE items");
        string ddl = showRows[0].Row["Create Table"].StrValue!;

        Assert.IsFalse(ddl.Contains("chk_qty"), $"Dropped constraint should not appear in DDL: {ddl}");
        Assert.IsFalse(ddl.Contains("CHECK"), $"No CHECK should appear after drop: {ddl}");
    }
}
