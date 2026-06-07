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

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Verifies that the SQL emitted by SHOW CREATE TABLE is parseable and semantically correct
/// (gap 1 fix): the grammar now accepts PRIMARY KEY, KEY, and UNIQUE KEY inline within the
/// CREATE TABLE parentheses, which is the format the DDL generator produces.
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
}
