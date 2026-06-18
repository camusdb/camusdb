
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Verifies that all three rename operations are fully reachable through the
/// ExecuteDDLSQL path (the same entry-point that the /execute-sql-ddl HTTP controller
/// delegates to). Tests prove the wire path by asserting old name rejection and new
/// name accessibility after each operation.
/// </summary>
internal sealed class TestRenameSQL : SharedNodeBaseTest
{
    // ── helpers ──────────────────────────────────────────────────────────────

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)>
        SetupTableWithData(string tableName)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CreateTableTicket create = new(
            databaseName: dbname,
            tableName: tableName,
            columns: new ColumnInfo[]
            {
                new("id",    ColumnType.Id),
                new("label", ColumnType.String, notNull: true),
                new("score", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        );
        await executor.CreateTable(create);

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
                        { "id",    new(ColumnType.Id,        ObjectIdGenerator.Generate().ToString()) },
                        { "label", new(ColumnType.String,    "item " + i) },
                        { "score", new(ColumnType.Integer64, (long)i * 10) },
                    }
                }
            );
            await executor.Insert(insert);
        }
        await database.Transactions.CommitAsync(tx);
        return (dbname, database, executor);
    }

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)>
        SetupTableWithIndex(string tableName)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CreateTableTicket create = new(
            databaseName: dbname,
            tableName: tableName,
            columns: new ColumnInfo[]
            {
                new("id",    ColumnType.Id),
                new("label", ColumnType.String, notNull: true),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        );
        await executor.CreateTable(create);

        await executor.AlterIndex(new AlterIndexTicket(
            databaseName: dbname,
            tableName: tableName,
            indexName: "idx_label",
            columns: new ColumnIndexInfo[] { new("label", OrderType.Ascending) },
            operation: AlterIndexOperation.AddIndex
        ));

        return (dbname, database, executor);
    }

    private static async Task<List<QueryResultRow>> QueryAll(
        string dbname, string tableName, DatabaseDescriptor database, CommandExecutor executor)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            QueryTicket q = new(
                txnState: tx,
                databaseName: dbname,
                tableName: tableName,
                index: null, projection: null, where: null,
                filters: null, orderBy: null, limit: null, offset: null, parameters: null
            );
            (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.Query(q);
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

    private static ExecuteSQLTicket Sql(KvTransaction tx, string dbname, string sql) =>
        new(txnState: tx, database: dbname, sql: sql, parameters: null);

    // ── RENAME TABLE via ExecuteDDLSQL ───────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task ExecuteDDLSQL_RenameTable_NewNameAccessible()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupTableWithData("src_tbl");

        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteDDLSQLResult result = await executor.ExecuteDDLSQL(
            Sql(tx, dbname, "ALTER TABLE src_tbl RENAME TO dst_tbl"));

        Assert.IsTrue(result.Success);
        Assert.IsTrue(database.Schema.Tables.ContainsKey("dst_tbl"));
    }

    [Test]
    [NonParallelizable]
    public async Task ExecuteDDLSQL_RenameTable_OldNameRejected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupTableWithData("old_name");

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(Sql(tx, dbname, "ALTER TABLE old_name RENAME TO new_name"));

        Assert.IsFalse(database.Schema.Tables.ContainsKey("old_name"),
            "Old table name must be absent from schema after rename");

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await QueryAll(dbname, "old_name", database, executor));
        Assert.AreEqual(CamusDBErrorCodes.TableDoesntExist, ex!.Code);
    }

    [Test]
    [NonParallelizable]
    public async Task ExecuteDDLSQL_RenameTable_DataSurvivesUnderNewName()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupTableWithData("data_src");

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(Sql(tx, dbname, "ALTER TABLE data_src RENAME TO data_dst"));

        List<QueryResultRow> rows = await QueryAll(dbname, "data_dst", database, executor);
        Assert.AreEqual(3, rows.Count);
        Assert.IsTrue(rows.All(r => r.Row.ContainsKey("label")));
    }

    [Test]
    [NonParallelizable]
    public async Task ExecuteDDLSQL_RenameTable_MissingNewName_Throws()
    {
        // The parser sees no identifier after TO — a SqlSyntaxError before validation runs.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupTableWithData("nonempty_src");

        KvTransaction tx = await database.Transactions.BeginAsync();
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteDDLSQL(Sql(tx, dbname, "ALTER TABLE nonempty_src RENAME TO")));
        Assert.AreEqual(CamusDBErrorCodes.SqlSyntaxError, ex!.Code);
    }

    // ── RENAME COLUMN via ExecuteDDLSQL ──────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task ExecuteDDLSQL_RenameColumn_NewNameInSchema()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupTableWithData("col_tbl");

        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteDDLSQLResult result = await executor.ExecuteDDLSQL(
            Sql(tx, dbname, "ALTER TABLE col_tbl RENAME COLUMN score TO points"));

        Assert.IsTrue(result.Success);
        TableSchema schema = database.Schema.Tables["col_tbl"];
        Assert.IsTrue(schema.Columns!.Any(c => c.Name == "points"));
        Assert.IsFalse(schema.Columns!.Any(c => c.Name == "score"));
    }

    [Test]
    [NonParallelizable]
    public async Task ExecuteDDLSQL_RenameColumn_OldNameAbsentFromQueryResults()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupTableWithData("col_tbl2");

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(
            Sql(tx, dbname, "ALTER TABLE col_tbl2 RENAME COLUMN label TO title"));

        List<QueryResultRow> rows = await QueryAll(dbname, "col_tbl2", database, executor);
        Assert.AreEqual(3, rows.Count);
        Assert.IsTrue(rows.All(r => r.Row.ContainsKey("title")),
            "New column name 'title' must appear in every row");
        Assert.IsTrue(rows.All(r => !r.Row.ContainsKey("label")),
            "Old column name 'label' must not appear in any row");
    }

    [Test]
    [NonParallelizable]
    public async Task ExecuteDDLSQL_RenameColumn_SelfRename_Throws()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupTableWithData("col_tbl3");

        KvTransaction tx = await database.Transactions.BeginAsync();
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteDDLSQL(
                Sql(tx, dbname, "ALTER TABLE col_tbl3 RENAME COLUMN score TO score")));
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
    }

    [Test]
    [NonParallelizable]
    public async Task ExecuteDDLSQL_RenameColumn_NonExistent_Throws()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupTableWithData("col_tbl4");

        KvTransaction tx = await database.Transactions.BeginAsync();
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteDDLSQL(
                Sql(tx, dbname, "ALTER TABLE col_tbl4 RENAME COLUMN ghost TO phantom")));
        Assert.AreEqual(CamusDBErrorCodes.UnknownColumn, ex!.Code);
    }

    // ── RENAME INDEX via ExecuteDDLSQL ───────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task ExecuteDDLSQL_RenameIndex_NewNameInSchema()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupTableWithIndex("idx_tbl");

        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteDDLSQLResult result = await executor.ExecuteDDLSQL(
            Sql(tx, dbname, "ALTER TABLE idx_tbl RENAME INDEX idx_label TO idx_title"));

        Assert.IsTrue(result.Success);
        TableSchema schema = database.Schema.Tables["idx_tbl"];
        Assert.IsTrue(schema.Indexes!.Any(ix => ix.Name == "idx_title"));
        Assert.IsFalse(schema.Indexes!.Any(ix => ix.Name == "idx_label"));
    }

    [Test]
    [NonParallelizable]
    public async Task ExecuteDDLSQL_RenameIndex_OldNameRejected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupTableWithIndex("idx_tbl2");

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(
            Sql(tx, dbname, "ALTER TABLE idx_tbl2 RENAME INDEX idx_label TO idx_renamed"));

        TableSchema schema = database.Schema.Tables["idx_tbl2"];
        Assert.IsFalse(schema.Indexes!.Any(ix => ix.Name == "idx_label"),
            "Old index name must be absent from schema after rename");
    }

    [Test]
    [NonParallelizable]
    public async Task ExecuteDDLSQL_RenameIndex_SelfRename_Throws()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupTableWithIndex("idx_tbl3");

        KvTransaction tx = await database.Transactions.BeginAsync();
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteDDLSQL(
                Sql(tx, dbname, "ALTER TABLE idx_tbl3 RENAME INDEX idx_label TO idx_label")));
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
    }

    [Test]
    [NonParallelizable]
    public async Task ExecuteDDLSQL_RenameIndex_NonExistent_Throws()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupTableWithIndex("idx_tbl4");

        KvTransaction tx = await database.Transactions.BeginAsync();
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteDDLSQL(
                Sql(tx, dbname, "ALTER TABLE idx_tbl4 RENAME INDEX ghost_idx TO phantom_idx")));
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
    }

    // ── Validator guards through the SQL path ────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task ExecuteDDLSQL_RenameTable_SelfRename_Throws()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupTableWithData("self_src");

        KvTransaction tx = await database.Transactions.BeginAsync();
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteDDLSQL(
                Sql(tx, dbname, "ALTER TABLE self_src RENAME TO self_src")));
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
    }

    [Test]
    [NonParallelizable]
    public async Task ExecuteDDLSQL_RenameColumn_TooLongNewName_Throws()
    {
        // Validator rejects NewName > 255 chars; parser accepts long identifiers.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupTableWithData("tilde_tbl");

        string longName = new('a', 256);
        KvTransaction tx = await database.Transactions.BeginAsync();
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteDDLSQL(
                Sql(tx, dbname, $"ALTER TABLE tilde_tbl RENAME COLUMN score TO {longName}")));
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
    }
}
