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
        Assert.That(ddl, Does.Match(@"`payload`\s+BYTES\s"), "unsized bytes must render as bare BYTES");
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
        Assert.AreEqual("BYTES",       NameType("payload"));
    }

    /// <summary>
    /// A declared <c>bytes(N)</c> maximum is enforced on write, so SHOW CREATE TABLE must carry it.
    /// Rendered as bare <c>BYTES</c>, DDL taken from the output re-creates the column with the 10 MB
    /// default ceiling, a dump reload silently widens it, and a client comparing schemas sees a false
    /// difference. The round-trip must keep the size and keep enforcing it; an unsized column must
    /// still render bare.
    /// </summary>
    [Test]
    public async Task ShowCreateTable_SizedBytes_KeepsEnforcedMaximum_RoundTrips()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await DdlAsync(executor, db, dbname,
            "CREATE TABLE docs (id OID PRIMARY KEY NOT NULL, embedding BYTES(3072) STORAGE EXTERNAL, payload BYTES)");

        List<QueryResultRow> showRows = await QueryAsync(executor, db, dbname, "SHOW CREATE TABLE docs");
        string ddl = showRows[0].Row["Create Table"].StrValue!;

        Assert.That(ddl, Does.Contain("`embedding` BYTES(3072) NULL STORAGE EXTERNAL"), $"sized bytes must keep its size: {ddl}");
        Assert.That(ddl, Does.Contain("`payload` BYTES NULL"), $"unsized bytes must render bare: {ddl}");

        List<QueryResultRow> cols = await QueryAsync(executor, db, dbname, "SHOW COLUMNS FROM docs");
        string Type(List<QueryResultRow> rows, string field) => rows.First(r => r.Row["Field"].StrValue == field).Row["Type"].StrValue!;
        Assert.AreEqual("BYTES(3072)", Type(cols, "embedding"));
        Assert.AreEqual("BYTES",       Type(cols, "payload"));

        // Re-create the table from the rendered DDL.
        await DdlAsync(executor, db, dbname, ddl.Replace("`docs`", "`docs2`", System.StringComparison.Ordinal));

        List<QueryResultRow> showRows2 = await QueryAsync(executor, db, dbname, "SHOW CREATE TABLE docs2");
        Assert.AreEqual(
            ddl.Replace("`docs`", "`docs2`", System.StringComparison.Ordinal),
            showRows2[0].Row["Create Table"].StrValue,
            "the re-created table must render the same DDL");

        List<QueryResultRow> cols2 = await QueryAsync(executor, db, dbname, "SHOW COLUMNS FROM docs2");
        Assert.AreEqual("BYTES(3072)", Type(cols2, "embedding"));
        Assert.AreEqual("BYTES",       Type(cols2, "payload"));

        // The re-created column enforces the declared maximum, not the default ceiling.
        KvTransaction ok = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(ok, dbname,
            "INSERT INTO docs2 (id, embedding) VALUES (gen_id(), @v)",
            new Dictionary<string, ColumnValue> { { "@v", new ColumnValue(new byte[3072]) } }));
        await db.Transactions.CommitAsync(ok);

        KvTransaction tooLong = await db.Transactions.BeginAsync();
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tooLong, dbname,
                "INSERT INTO docs2 (id, embedding) VALUES (gen_id(), @v)",
                new Dictionary<string, ColumnValue> { { "@v", new ColumnValue(new byte[3073]) } })))!;
        Assert.AreEqual(CamusDBErrorCodes.ValueTooLong, ex.Code);
        await db.Transactions.RollbackAsync(tooLong);
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

    /// <summary>
    /// <c>SHOW CREATE TABLE … WITHOUT INDEXES</c> renders the table without its secondary indexes, so
    /// a caller that creates them separately does not get them built inline before any rows load.
    ///
    /// <para>The primary key is still rendered: it is part of the table definition and cannot be
    /// created by a later <c>CREATE INDEX</c>. That is the difference between "no secondary indexes"
    /// and "no indexes", and getting it wrong would emit DDL that does not describe a usable table.</para>
    /// </summary>
    [Test]
    public async Task ShowCreateTableWithoutIndexes_DropsSecondaryIndexes_KeepsPrimaryKey()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await DdlAsync(executor, db, dbname,
            "CREATE TABLE src (id STRING NOT NULL PRIMARY KEY, a INT64 NOT NULL, b STRING NOT NULL)");
        await DdlAsync(executor, db, dbname, "CREATE INDEX a_idx ON src (a)");
        await DdlAsync(executor, db, dbname, "CREATE UNIQUE INDEX b_uniq ON src (b)");

        List<QueryResultRow> withIndexes = await QueryAsync(executor, db, dbname, "SHOW CREATE TABLE src");
        string full = withIndexes[0].Row["Create Table"].StrValue!;

        Assert.IsTrue(full.Contains("a_idx"), "the default rendering should still carry the secondary index");
        Assert.IsTrue(full.Contains("b_uniq"), "the default rendering should still carry the unique index");

        List<QueryResultRow> withoutIndexes = await QueryAsync(executor, db, dbname, "SHOW CREATE TABLE src WITHOUT INDEXES");
        string bare = withoutIndexes[0].Row["Create Table"].StrValue!;

        Assert.IsFalse(bare.Contains("a_idx"), "WITHOUT INDEXES still rendered the secondary index");
        Assert.IsFalse(bare.Contains("b_uniq"), "WITHOUT INDEXES still rendered the unique index");
        Assert.IsTrue(bare.Contains("PRIMARY KEY"), "WITHOUT INDEXES dropped the primary key, which it must keep");
    }

    /// <summary>
    /// The index-free rendering must still be executable DDL. A caller uses it to create the table
    /// before loading rows, so a form that does not re-parse would be useless.
    /// </summary>
    [Test]
    public async Task ShowCreateTableWithoutIndexes_OutputIsReparseable()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await DdlAsync(executor, db, dbname,
            "CREATE TABLE src (id STRING NOT NULL PRIMARY KEY, a INT64 NOT NULL)");
        await DdlAsync(executor, db, dbname, "CREATE INDEX a_idx ON src (a)");

        List<QueryResultRow> rows = await QueryAsync(executor, db, dbname, "SHOW CREATE TABLE src WITHOUT INDEXES");
        string ddl = rows[0].Row["Create Table"].StrValue!.Replace("`src`", "`copy`");

        await DdlAsync(executor, db, dbname, ddl);

        List<QueryResultRow> copy = await QueryAsync(executor, db, dbname, "SHOW CREATE TABLE copy");
        string copyDdl = copy[0].Row["Create Table"].StrValue!;

        Assert.IsTrue(copyDdl.Contains("PRIMARY KEY"));
        Assert.IsFalse(copyDdl.Contains("a_idx"), "the copy was created from index-free DDL, so it must carry no secondary index");
    }

    /// <summary>
    /// A table with no secondary index renders identically with and without the clause, so the
    /// clause is never a reason for a caller to special-case its own SQL.
    /// </summary>
    [Test]
    public async Task ShowCreateTableWithoutIndexes_NoSecondaryIndexes_RendersTheSame()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await DdlAsync(executor, db, dbname,
            "CREATE TABLE src (id STRING NOT NULL PRIMARY KEY, a INT64 NOT NULL)");

        List<QueryResultRow> withClause = await QueryAsync(executor, db, dbname, "SHOW CREATE TABLE src WITHOUT INDEXES");
        List<QueryResultRow> without = await QueryAsync(executor, db, dbname, "SHOW CREATE TABLE src");

        Assert.AreEqual(without[0].Row["Create Table"].StrValue, withClause[0].Row["Create Table"].StrValue);
    }

    /// <summary>
    /// A descending index must survive <c>SHOW CREATE TABLE</c>. Before this was fixed the direction
    /// was dropped, so a dump of a descending index restored as an ascending one with no warning —
    /// a silent schema change on the documented migration path.
    ///
    /// <para><c>ASC</c> is deliberately not written: it is the default, and omitting it keeps the
    /// rendering of an all-ascending table unchanged.</para>
    /// </summary>
    [Test]
    public async Task ShowCreateTable_RendersDescendingIndexDirection()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await DdlAsync(executor, db, dbname,
            "CREATE TABLE src (id STRING NOT NULL PRIMARY KEY, a INT64 NOT NULL, b INT64 NOT NULL)");
        await DdlAsync(executor, db, dbname, "CREATE INDEX a_desc ON src (a DESC)");
        await DdlAsync(executor, db, dbname, "CREATE INDEX b_asc ON src (b)");

        List<QueryResultRow> rows = await QueryAsync(executor, db, dbname, "SHOW CREATE TABLE src");
        string ddl = rows[0].Row["Create Table"].StrValue!;

        StringAssert.Contains("`a` DESC", ddl, "the descending direction was dropped from the DDL");
        StringAssert.Contains("KEY `b_asc` (`b`)", ddl, "an ascending index should render without an explicit ASC");
    }

    /// <summary>
    /// The direction has to survive a re-parse too, not just the rendering. A composite index with
    /// mixed directions is the case a single per-index flag would get wrong, so it is the one worth
    /// round-tripping.
    /// </summary>
    [Test]
    public async Task ShowCreateTable_MixedDirectionCompositeIndex_RoundTrips()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await DdlAsync(executor, db, dbname,
            "CREATE TABLE src (id STRING NOT NULL PRIMARY KEY, a INT64 NOT NULL, b INT64 NOT NULL)");
        await DdlAsync(executor, db, dbname, "CREATE INDEX mixed ON src (a ASC, b DESC)");

        List<QueryResultRow> rows = await QueryAsync(executor, db, dbname, "SHOW CREATE TABLE src");
        string ddl = rows[0].Row["Create Table"].StrValue!.Replace("`src`", "`copy`");

        StringAssert.Contains("`a`, `b` DESC", ddl);

        await DdlAsync(executor, db, dbname, ddl);

        List<QueryResultRow> copy = await QueryAsync(executor, db, dbname, "SHOW CREATE TABLE copy");
        StringAssert.Contains("`a`, `b` DESC", copy[0].Row["Create Table"].StrValue!,
            "the direction was lost when the rendered DDL was re-parsed");
    }

    /// <summary>
    /// <c>SHOW INDEXES</c> reports each key column's direction in a <c>Directions</c> list that is
    /// positionally aligned with <c>Columns</c>, and reports the index comment.
    ///
    /// <para>The direction is a separate field rather than part of <c>Columns</c> on purpose: a
    /// consumer splits <c>Columns</c> on the comma and treats each element as an identifier to quote,
    /// so folding <c>a DESC</c> into it would hand that consumer a name it cannot render.</para>
    /// </summary>
    [Test]
    public async Task ShowIndexes_ReportsDirectionsAndComment()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await DdlAsync(executor, db, dbname,
            "CREATE TABLE src (id STRING NOT NULL PRIMARY KEY, a INT64 NOT NULL, b INT64 NOT NULL)");
        await DdlAsync(executor, db, dbname, "CREATE INDEX mixed ON src (a ASC, b DESC)");
        await DdlAsync(executor, db, dbname, "COMMENT ON INDEX src.mixed IS 'why this exists'");

        List<QueryResultRow> rows = await QueryAsync(executor, db, dbname, "SHOW INDEXES FROM src");
        QueryResultRow mixed = rows.Find(r => r.Row["Key_name"].StrValue == "mixed")!;

        Assert.AreEqual("a,b", mixed.Row["Columns"].StrValue,
            "Columns must stay a plain identifier list so a consumer can quote each element");
        Assert.AreEqual("ASC,DESC", mixed.Row["Directions"].StrValue);
        Assert.AreEqual("why this exists", mixed.Row["Comment"].StrValue);
    }

    /// <summary>
    /// An index with no comment reports an empty string rather than a null, so a consumer reading the
    /// field does not have to null-check a column that is always present.
    /// </summary>
    [Test]
    public async Task ShowIndexes_IndexWithoutComment_ReportsEmptyString()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await DdlAsync(executor, db, dbname,
            "CREATE TABLE src (id STRING NOT NULL PRIMARY KEY, a INT64 NOT NULL)");
        await DdlAsync(executor, db, dbname, "CREATE INDEX a_idx ON src (a)");

        List<QueryResultRow> rows = await QueryAsync(executor, db, dbname, "SHOW INDEXES FROM src");
        QueryResultRow idx = rows.Find(r => r.Row["Key_name"].StrValue == "a_idx")!;

        Assert.AreEqual("", idx.Row["Comment"].StrValue);
        Assert.AreEqual("ASC", idx.Row["Directions"].StrValue);
    }
}
