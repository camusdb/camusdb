
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using System;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Acceptance tests for RENAME TABLE, RENAME COLUMN, RENAME INDEX executor entry-points,
/// SQL grammar, and data-layer invariants.
/// </summary>
internal sealed class TestTableRename : SharedNodeBaseTest
{
    // ── helpers ──────────────────────────────────────────────────────────────

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupDatabase()
        => await CreateDatabase();

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)>
        SetupTableWithData(string tableName = "robots")
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupDatabase();

        CreateTableTicket createTicket = new(
            databaseName: dbname,
            tableName: tableName,
            columns: new ColumnInfo[]
            {
                new("id",   ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("year", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        );
        await executor.CreateTable(createTicket);

        KvTransaction tx = await database.Transactions.BeginAsync();
        for (int i = 0; i < 3; i++)
        {
            InsertTicket insert = new(
                txnState: tx,
                databaseName: dbname,
                tableName: tableName,
                values: new()
                {
                    new()
                    {
                        { "id",   new(ColumnType.Id,        ObjectIdGenerator.Generate().ToString()) },
                        { "name", new(ColumnType.String,    "robot " + i) },
                        { "year", new(ColumnType.Integer64, 2000L + i) },
                    }
                }
            );
            await executor.Insert(insert);
        }
        await database.Transactions.CommitAsync(tx);

        return (dbname, database, executor);
    }

    private static async Task<List<QueryResultRow>> QueryAll(
        string dbname, string tableName, DatabaseDescriptor database, CommandExecutor executor)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            QueryTicket queryTicket = new(
                txnState: tx,
                databaseName: dbname,
                tableName: tableName,
                index: null,
                projection: null,
                where: null,
                filters: null,
                orderBy: null,
                limit: null,
                offset: null,
                parameters: null
            );

            (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.Query(queryTicket);
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await database.Transactions.CommitAsync(tx);
            return rows;
        }
        catch
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
            throw;
        }
    }

    // ── RENAME TABLE ─────────────────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task RenameTable_Ticket_NewNameQueryable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableWithData("src");

        bool ok = await executor.RenameTable(new RenameTableTicket(dbname, "src", "dst"));
        Assert.IsTrue(ok);

        TableSchema schema = new CatalogsManager(logger).GetTableSchema(database, "dst");
        Assert.AreEqual("dst", schema.Name);
    }

    [Test]
    [NonParallelizable]
    public async Task RenameTable_Ticket_OldNameGone()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableWithData("src2");

        await executor.RenameTable(new RenameTableTicket(dbname, "src2", "dst2"));

        Assert.IsFalse(database.Schema.Tables.ContainsKey("src2"));
        Assert.IsTrue(database.Schema.Tables.ContainsKey("dst2"));
    }

    [Test]
    [NonParallelizable]
    public async Task RenameTable_Ticket_DataSurvives()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableWithData("bots");

        await executor.RenameTable(new RenameTableTicket(dbname, "bots", "machines"));

        List<QueryResultRow> rows = await QueryAll(dbname, "machines", database, executor);
        Assert.AreEqual(3, rows.Count);
        Assert.IsTrue(rows.All(r => r.Row.ContainsKey("name")));
    }

    [Test]
    [NonParallelizable]
    public async Task RenameTable_SQL_RoundTrip()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableWithData("gadgets");

        ExecuteSQLTicket ddl = new(
            txnState: await database.Transactions.BeginAsync(),
            database: dbname,
            sql: "ALTER TABLE gadgets RENAME TO widgets",
            parameters: null
        );
        await executor.ExecuteDDLSQL(ddl);

        Assert.IsTrue(database.Schema.Tables.ContainsKey("widgets"));
        Assert.IsFalse(database.Schema.Tables.ContainsKey("gadgets"));
    }

    [Test]
    [NonParallelizable]
    public async Task RenameTable_Ticket_NewNameConflict_Throws()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableWithData("t1");

        // Create a second table that would conflict with the rename target.
        CreateTableTicket t2 = new(
            databaseName: dbname,
            tableName: "t2",
            columns: new ColumnInfo[] { new("id", ColumnType.Id) },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        );
        await executor.CreateTable(t2);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await executor.RenameTable(new RenameTableTicket(dbname, "t1", "t2")));
        Assert.AreEqual(CamusDBErrorCodes.TableAlreadyExists, ex!.Code);
    }

    [Test]
    [NonParallelizable]
    public async Task RenameTable_Ticket_NonExistentTable_Throws()
    {
        (string dbname, _, CommandExecutor executor) = await SetupDatabase();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await executor.RenameTable(new RenameTableTicket(dbname, "ghost", "phantom")));
        Assert.AreEqual(CamusDBErrorCodes.TableDoesntExist, ex!.Code);
    }

    // ── RENAME COLUMN ────────────────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task RenameColumn_Ticket_NewNameInSchema()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableWithData("units");

        AlterTableTicket ticket = new(
            databaseName: dbname,
            tableName: "units",
            operation: AlterTableOperation.RenameColumn,
            column: new ColumnInfo("name", ColumnType.Null),
            newName: "label"
        );
        bool ok = await executor.AlterTable(ticket);
        Assert.IsTrue(ok);

        TableSchema schema = database.Schema.Tables["units"];
        Assert.IsTrue(schema.Columns!.Any(c => c.Name == "label"));
        Assert.IsFalse(schema.Columns!.Any(c => c.Name == "name"));
    }

    [Test]
    [NonParallelizable]
    public async Task RenameColumn_Ticket_VersionBumped()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableWithData("vers_tbl");

        long vBefore = database.Schema.Tables["vers_tbl"].Version;

        AlterTableTicket ticket = new(
            databaseName: dbname,
            tableName: "vers_tbl",
            operation: AlterTableOperation.RenameColumn,
            column: new ColumnInfo("name", ColumnType.Null),
            newName: "alias"
        );
        await executor.AlterTable(ticket);

        long vAfter = database.Schema.Tables["vers_tbl"].Version;
        Assert.Greater(vAfter, vBefore);
    }

    [Test]
    [NonParallelizable]
    public async Task RenameColumn_Ticket_DataSurvives()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableWithData("crew");

        AlterTableTicket ticket = new(
            databaseName: dbname,
            tableName: "crew",
            operation: AlterTableOperation.RenameColumn,
            column: new ColumnInfo("year", ColumnType.Null),
            newName: "founded"
        );
        await executor.AlterTable(ticket);

        // Rows are positionally encoded; rename is metadata-only — values still present.
        List<QueryResultRow> rows = await QueryAll(dbname, "crew", database, executor);
        Assert.AreEqual(3, rows.Count);
        Assert.IsTrue(rows.All(r => r.Row.ContainsKey("founded")));
        Assert.IsTrue(rows.All(r => !r.Row.ContainsKey("year")));
    }

    [Test]
    [NonParallelizable]
    public async Task RenameColumn_SQL_RoundTrip()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableWithData("fleet");

        ExecuteSQLTicket ddl = new(
            txnState: await database.Transactions.BeginAsync(),
            database: dbname,
            sql: "ALTER TABLE fleet RENAME COLUMN name TO title",
            parameters: null
        );
        await executor.ExecuteDDLSQL(ddl);

        TableSchema schema = database.Schema.Tables["fleet"];
        Assert.IsTrue(schema.Columns!.Any(c => c.Name == "title"));
        Assert.IsFalse(schema.Columns!.Any(c => c.Name == "name"));
    }

    [Test]
    [NonParallelizable]
    public async Task RenameColumn_Ticket_NonExistentColumn_Throws()
    {
        (string dbname, _, CommandExecutor executor) = await SetupTableWithData("scouts");

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.AlterTable(new AlterTableTicket(
                databaseName: dbname,
                tableName: "scouts",
                operation: AlterTableOperation.RenameColumn,
                column: new ColumnInfo("ghost", ColumnType.Null),
                newName: "phantom"
            )));
        Assert.AreEqual(CamusDBErrorCodes.UnknownColumn, ex!.Code);
    }

    [Test]
    [NonParallelizable]
    public async Task RenameColumn_Ticket_DuplicateTarget_Throws()
    {
        (string dbname, _, CommandExecutor executor) = await SetupTableWithData("dupes");

        // "name" and "year" both exist; renaming "name" → "year" must fail.
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.AlterTable(new AlterTableTicket(
                databaseName: dbname,
                tableName: "dupes",
                operation: AlterTableOperation.RenameColumn,
                column: new ColumnInfo("name", ColumnType.Null),
                newName: "year"
            )));
        Assert.AreEqual(CamusDBErrorCodes.DuplicateColumn, ex!.Code);
    }

    // ── RENAME INDEX ─────────────────────────────────────────────────────────

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)>
        SetupTableWithIndex(string tableName = "indexed")
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupDatabase();

        CreateTableTicket create = new(
            databaseName: dbname,
            tableName: tableName,
            columns: new ColumnInfo[]
            {
                new("id",   ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        );
        await executor.CreateTable(create);

        // Add a named secondary index.
        AlterIndexTicket addIdx = new(
            databaseName: dbname,
            tableName: tableName,
            indexName: "idx_name",
            columns: new ColumnIndexInfo[] { new("name", OrderType.Ascending) },
            operation: AlterIndexOperation.AddIndex
        );
        await executor.AlterIndex(addIdx);

        return (dbname, database, executor);
    }

    [Test]
    [NonParallelizable]
    public async Task RenameIndex_Ticket_NewNameInSchema()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableWithIndex("ixt1");

        AlterIndexTicket ticket = new(
            databaseName: dbname,
            tableName: "ixt1",
            indexName: "idx_name",
            columns: Array.Empty<ColumnIndexInfo>(),
            operation: AlterIndexOperation.RenameIndex,
            newName: "idx_label"
        );
        bool ok = await executor.AlterIndex(ticket);
        Assert.IsTrue(ok);

        TableSchema schema = database.Schema.Tables["ixt1"];
        Assert.IsTrue(schema.Indexes!.Any(ix => ix.Name == "idx_label"));
        Assert.IsFalse(schema.Indexes!.Any(ix => ix.Name == "idx_name"));
    }

    [Test]
    [NonParallelizable]
    public async Task RenameIndex_Ticket_VersionUnchanged()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableWithIndex("ixt2");

        long vBefore = database.Schema.Tables["ixt2"].Version;

        AlterIndexTicket ticket = new(
            databaseName: dbname,
            tableName: "ixt2",
            indexName: "idx_name",
            columns: Array.Empty<ColumnIndexInfo>(),
            operation: AlterIndexOperation.RenameIndex,
            newName: "idx_renamed"
        );
        await executor.AlterIndex(ticket);

        long vAfter = database.Schema.Tables["ixt2"].Version;
        Assert.AreEqual(vBefore, vAfter, "Index rename must NOT bump TableSchema.Version");
    }

    [Test]
    [NonParallelizable]
    public async Task RenameIndex_SQL_RoundTrip()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableWithIndex("ixt3");

        ExecuteSQLTicket ddl = new(
            txnState: await database.Transactions.BeginAsync(),
            database: dbname,
            sql: "ALTER TABLE ixt3 RENAME INDEX idx_name TO idx_alias",
            parameters: null
        );
        await executor.ExecuteDDLSQL(ddl);

        TableSchema schema = database.Schema.Tables["ixt3"];
        Assert.IsTrue(schema.Indexes!.Any(ix => ix.Name == "idx_alias"));
        Assert.IsFalse(schema.Indexes!.Any(ix => ix.Name == "idx_name"));
    }

    [Test]
    [NonParallelizable]
    public async Task RenameIndex_Ticket_NonExistentIndex_Throws()
    {
        (string dbname, _, CommandExecutor executor) = await SetupTableWithIndex("ixt4");

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.AlterIndex(new AlterIndexTicket(
                databaseName: dbname,
                tableName: "ixt4",
                indexName: "ghost_idx",
                columns: Array.Empty<ColumnIndexInfo>(),
                operation: AlterIndexOperation.RenameIndex,
                newName: "phantom_idx"
            )));
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
    }

    [Test]
    [NonParallelizable]
    public async Task RenameIndex_Ticket_DuplicateTarget_Throws()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableWithIndex("ixt5");

        // Add a second index.
        AlterIndexTicket addIdx2 = new(
            databaseName: dbname,
            tableName: "ixt5",
            indexName: "idx_other",
            columns: new ColumnIndexInfo[] { new("name", OrderType.Ascending) },
            operation: AlterIndexOperation.AddIndex
        );
        await executor.AlterIndex(addIdx2);

        // Rename idx_name → idx_other should fail.
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.AlterIndex(new AlterIndexTicket(
                databaseName: dbname,
                tableName: "ixt5",
                indexName: "idx_name",
                columns: Array.Empty<ColumnIndexInfo>(),
                operation: AlterIndexOperation.RenameIndex,
                newName: "idx_other"
            )));
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
    }

    // ── SQL parser sanity ────────────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task SQL_RenameTable_ParsesAndExecutes()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableWithData("parse_src");

        ExecuteSQLTicket ddl = new(
            txnState: await database.Transactions.BeginAsync(),
            database: dbname,
            sql: "ALTER TABLE parse_src RENAME TO parse_dst",
            parameters: null
        );
        ExecuteDDLSQLResult result = await executor.ExecuteDDLSQL(ddl);
        Assert.IsTrue(result.Success);

        Assert.IsTrue(database.Schema.Tables.ContainsKey("parse_dst"));
    }

    [Test]
    [NonParallelizable]
    public async Task SQL_RenameColumn_ParsesAndExecutes()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableWithData("parse_col");

        ExecuteSQLTicket ddl = new(
            txnState: await database.Transactions.BeginAsync(),
            database: dbname,
            sql: "ALTER TABLE parse_col RENAME COLUMN year TO founded",
            parameters: null
        );
        ExecuteDDLSQLResult result = await executor.ExecuteDDLSQL(ddl);
        Assert.IsTrue(result.Success);

        TableSchema schema = database.Schema.Tables["parse_col"];
        Assert.IsTrue(schema.Columns!.Any(c => c.Name == "founded"));
    }

    [Test]
    [NonParallelizable]
    public async Task SQL_RenameIndex_ParsesAndExecutes()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableWithIndex("parse_idx");

        ExecuteSQLTicket ddl = new(
            txnState: await database.Transactions.BeginAsync(),
            database: dbname,
            sql: "ALTER TABLE parse_idx RENAME INDEX idx_name TO idx_parsed",
            parameters: null
        );
        ExecuteDDLSQLResult result = await executor.ExecuteDDLSQL(ddl);
        Assert.IsTrue(result.Success);

        TableSchema schema = database.Schema.Tables["parse_idx"];
        Assert.IsTrue(schema.Indexes!.Any(ix => ix.Name == "idx_parsed"));
    }
}
