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

    [Test]
    public async Task ShowCreateTable_RendersColumnDefaults_FunctionAndConstant_RoundTrips()
    {
        // SHOW CREATE TABLE must render both a per-row function default (gen_id()) and constant
        // defaults, and the emitted DDL must re-parse and re-apply those defaults.
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname,
            "CREATE TABLE gear (" +
            "  id oid NOT NULL DEFAULT(gen_id()), " +
            "  qty int64 DEFAULT(1), " +
            "  label string(50) DEFAULT('none'), " +
            "  enabled bool DEFAULT(true), " +
            "  PRIMARY KEY (id))", null));
        await db.Transactions.CommitAsync(tx);

        List<QueryResultRow> showRows = await QueryAsync(executor, db, dbname, "SHOW CREATE TABLE gear");
        string ddl = showRows[0].Row["Create Table"].StrValue!;

        Assert.That(ddl, Does.Contain("DEFAULT(gen_id())"), $"function default must render: {ddl}");
        Assert.That(ddl, Does.Contain("DEFAULT(1)"),        $"integer default must render: {ddl}");
        Assert.That(ddl, Does.Contain("DEFAULT('none')"),   $"string default must render: {ddl}");
        Assert.That(ddl, Does.Contain("DEFAULT(true)"),     $"bool default must render: {ddl}");

        // Re-parse the emitted DDL under a new name.
        string ddl2 = ddl.Replace("`gear`", "`gear2`", System.StringComparison.Ordinal);
        await DdlAsync(executor, db, dbname, ddl2);

        // Insert omitting every defaulted column: gen_id() must populate the PK per row and the
        // constant defaults must apply on the round-tripped table.
        KvTransaction tx3 = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx3, dbname,
            "INSERT INTO gear2 (label) VALUES ('x')", null));
        await db.Transactions.CommitAsync(tx3);

        List<QueryResultRow> rows = await QueryAsync(executor, db, dbname, "SELECT qty, label, enabled FROM gear2");
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(1L,   rows[0].Row["qty"].LongValue,  "constant int default applied on round-tripped table");
        Assert.AreEqual("x",  rows[0].Row["label"].StrValue);
        Assert.AreEqual(true, rows[0].Row["enabled"].BoolValue, "constant bool default applied");
    }

    [Test]
    public async Task ShowCreateTable_StringDefaultWithSingleQuote_RendersDoubleQuoted_RoundTrips()
    {
        // The lexer does no '' / "" doubling and no escape decoding, so a default whose value contains
        // a single quote must be rendered double-quoted to re-parse to the same value (and vice versa).
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        KvTransaction tx = await db.Transactions.BeginAsync();
        // Value provided via a double-quoted literal so it carries a real apostrophe: it's
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname,
            "CREATE TABLE notes (id oid NOT NULL DEFAULT(gen_id()), tag string(50) DEFAULT(\"it's\"), PRIMARY KEY (id))", null));
        await db.Transactions.CommitAsync(tx);

        List<QueryResultRow> showRows = await QueryAsync(executor, db, dbname, "SHOW CREATE TABLE notes");
        string ddl = showRows[0].Row["Create Table"].StrValue!;

        Assert.That(ddl, Does.Contain("DEFAULT('it''s')"),
            $"a single-quote-containing default must render single-quoted with '' doubling: {ddl}");

        // Round-trip: re-parse and confirm the default value survives exactly.
        string ddl2 = ddl.Replace("`notes`", "`notes2`", System.StringComparison.Ordinal);
        await DdlAsync(executor, db, dbname, ddl2);

        KvTransaction tx3 = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx3, dbname,
            "INSERT INTO notes2 (id) VALUES (gen_id())", null));
        await db.Transactions.CommitAsync(tx3);

        List<QueryResultRow> rows = await QueryAsync(executor, db, dbname, "SELECT tag FROM notes2");
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("it's", rows[0].Row["tag"].StrValue, "single-quote default value must survive the round-trip");
    }

    /// <summary>
    /// A bytes value written as an <c>X'…'</c> literal must reach a BYTES column with its type intact,
    /// and a bytes DEFAULT must render in the same form so the emitted DDL re-creates it. Previously
    /// bytes had no literal at all: a value travelled as a string whose text happened to start with
    /// <c>0x</c> and relied on String→Bytes coercion at the destination, so the type was not
    /// recoverable from the SQL itself.
    /// </summary>
    [Test]
    public async Task BytesLiteral_InsertsAndRoundTripsThroughShowCreateTable()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await DdlAsync(executor, db, dbname,
            "CREATE TABLE blobs (id int64 PRIMARY KEY NOT NULL, payload bytes DEFAULT(X'0102'))");

        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            txnState: tx, database: dbname,
            sql: "INSERT INTO blobs (id, payload) VALUES (1, X'DEADBEEF'), (2, X'')",
            parameters: null));
        await db.Transactions.CommitAsync(tx);

        List<QueryResultRow> rows = await QueryAsync(executor, db, dbname, "SELECT id, payload FROM blobs ORDER BY id");

        Assert.AreEqual(2, rows.Count);
        Assert.AreEqual(ColumnType.Bytes, rows[0].Row["payload"].Type, "the literal must carry its own type");
        CollectionAssert.AreEqual(new byte[] { 0xDE, 0xAD, 0xBE, 0xEF }, rows[0].Row["payload"].BytesValue);
        CollectionAssert.AreEqual(System.Array.Empty<byte>(), rows[1].Row["payload"].BytesValue);

        // The DEFAULT must re-emit as X'…' and the emitted DDL must re-execute.
        List<QueryResultRow> showRows = await QueryAsync(executor, db, dbname, "SHOW CREATE TABLE blobs");
        string ddl = showRows[0].Row["Create Table"].StrValue!;

        Assert.That(ddl, Does.Contain("X'0102'"), $"bytes default must render as a bytes literal: {ddl}");

        await DdlAsync(executor, db, dbname, ddl.Replace("`blobs`", "`blobs2`"));

        KvTransaction tx2 = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            txnState: tx2, database: dbname, sql: "INSERT INTO blobs2 (id) VALUES (9)", parameters: null));
        await db.Transactions.CommitAsync(tx2);

        List<QueryResultRow> defRows = await QueryAsync(executor, db, dbname, "SELECT payload FROM blobs2 WHERE id = 9");
        CollectionAssert.AreEqual(new byte[] { 0x01, 0x02 }, defRows[0].Row["payload"].BytesValue,
            "the re-parsed DEFAULT must reproduce the original bytes");
    }

    /// <summary>
    /// <c>ARRAY[…]</c> must reach an array column with the declared element type. Arrays previously
    /// had no SQL literal at all — a value could only be supplied as a bound parameter — so this is
    /// the first path by which array data can be written in SQL text.
    /// </summary>
    [Test]
    public async Task ArrayLiteral_InsertsAndReadsBack()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await DdlAsync(executor, db, dbname,
            "CREATE TABLE arrs (id int64 PRIMARY KEY NOT NULL, tags array(int64), names array(string))");

        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            txnState: tx, database: dbname,
            sql: "INSERT INTO arrs (id, tags, names) VALUES (1, ARRAY[1, 2, 3], ARRAY['a', 'b'])",
            parameters: null));
        await db.Transactions.CommitAsync(tx);

        List<QueryResultRow> rows = await QueryAsync(executor, db, dbname, "SELECT id, tags, names FROM arrs");

        Assert.AreEqual(1, rows.Count);
        ColumnValue tags = rows[0].Row["tags"];
        Assert.AreEqual(ColumnType.Array, tags.Type);
        Assert.AreEqual(ColumnType.Integer64, tags.ArrayElementType);
        Assert.AreEqual(3, tags.ArrayValues!.Count);
        Assert.AreEqual(2L, tags.ArrayValues[1].LongValue);

        ColumnValue names = rows[0].Row["names"];
        Assert.AreEqual(ColumnType.String, names.ArrayElementType);
        Assert.AreEqual("b", names.ArrayValues![1].StrValue);
    }

    /// <summary>
    /// An empty <c>ARRAY[]</c> has no element type of its own, and a NULL element carries none
    /// either; both must take the column's declared type rather than being stored as untyped.
    /// Integer elements must also widen into a float column, exactly as a scalar would.
    /// </summary>
    [Test]
    public async Task ArrayLiteral_EmptyNullAndWideningAdoptTheColumnElementType()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await DdlAsync(executor, db, dbname,
            "CREATE TABLE arrs2 (id int64 PRIMARY KEY NOT NULL, tags array(int64), ratios array(float64))");

        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            txnState: tx, database: dbname,
            sql: "INSERT INTO arrs2 (id, tags, ratios) VALUES (1, ARRAY[], ARRAY[1, 2]), (2, ARRAY[NULL, 5], ARRAY[1.5])",
            parameters: null));
        await db.Transactions.CommitAsync(tx);

        List<QueryResultRow> rows = await QueryAsync(executor, db, dbname, "SELECT id, tags, ratios FROM arrs2 ORDER BY id");

        Assert.AreEqual(0, rows[0].Row["tags"].ArrayValues!.Count, "ARRAY[] must store as an empty array");
        Assert.AreEqual(ColumnType.Integer64, rows[0].Row["tags"].ArrayElementType, "empty array must adopt the column element type");

        // Integer literals widen into a float64 array, as they do for a scalar float64 column.
        Assert.AreEqual(ColumnType.Float64, rows[0].Row["ratios"].ArrayElementType);
        Assert.AreEqual(1.0d, rows[0].Row["ratios"].ArrayValues![0].FloatValue, 0.0001);

        ColumnValue withNull = rows[1].Row["tags"];
        Assert.AreEqual(2, withNull.ArrayValues!.Count);
        Assert.AreEqual(ColumnType.Null, withNull.ArrayValues[0].Type, "a NULL element stays NULL");
        Assert.AreEqual(5L, withNull.ArrayValues[1].LongValue);
    }

    /// <summary>
    /// A mixed-type list has no single element type, and a nested array cannot be modelled by
    /// <c>ColumnValue</c> at all — both must fail loudly rather than storing whichever type happened
    /// to appear first.
    /// </summary>
    [TestCase("ARRAY[1, 'two']", TestName = "ArrayLiteral_MixedTypesRejected")]
    [TestCase("ARRAY[ARRAY[1]]", TestName = "ArrayLiteral_NestedRejected")]
    public async Task ArrayLiteral_InvalidShapesAreRejected(string literal)
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await DdlAsync(executor, db, dbname,
            "CREATE TABLE arrs3 (id int64 PRIMARY KEY NOT NULL, tags array(int64))");

        KvTransaction tx = await db.Transactions.BeginAsync();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                txnState: tx, database: dbname,
                sql: $"INSERT INTO arrs3 (id, tags) VALUES (1, {literal})",
                parameters: null)))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
    }

    /// <summary>
    /// The literal path and the bound-parameter path must produce the same stored value. Arrays were
    /// parameter-only before this literal existed, so the parameter path is the reference
    /// implementation — a literal that stored something subtly different (element type, ordering,
    /// NULL handling) would be a silent divergence between two ways of writing the same row.
    /// </summary>
    [Test]
    public async Task ArrayLiteral_MatchesTheBoundParameterPath()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await DdlAsync(executor, db, dbname,
            "CREATE TABLE arrs4 (id int64 PRIMARY KEY NOT NULL, tags array(int64))");

        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            txnState: tx, database: dbname,
            sql: "INSERT INTO arrs4 (id, tags) VALUES (1, ARRAY[7, NULL, 9])", parameters: null));

        ColumnValue bound = ColumnValue.FromArray(ColumnType.Integer64, new List<ColumnValue>
        {
            new(ColumnType.Integer64, 7L), ColumnValue.Null, new(ColumnType.Integer64, 9L),
        });

        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            txnState: tx, database: dbname,
            sql: "INSERT INTO arrs4 (id, tags) VALUES (2, @t)",
            parameters: new Dictionary<string, ColumnValue> { { "@t", bound } }));
        await db.Transactions.CommitAsync(tx);

        List<QueryResultRow> rows = await QueryAsync(executor, db, dbname, "SELECT id, tags FROM arrs4 ORDER BY id");

        ColumnValue viaLiteral = rows[0].Row["tags"];
        ColumnValue viaParameter = rows[1].Row["tags"];

        Assert.AreEqual(viaParameter.ArrayElementType, viaLiteral.ArrayElementType);
        Assert.AreEqual(viaParameter.ArrayValues!.Count, viaLiteral.ArrayValues!.Count);
        Assert.AreEqual(0, viaLiteral.CompareTo(viaParameter), "literal and parameter paths must store the same value");
    }
}
