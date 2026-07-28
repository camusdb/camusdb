/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using NUnit.Framework;

namespace CamusDB.Tests.CommandsExecutor;

internal sealed class TestSchemaElementStatesDml : SharedNodeBaseTest
{
    [Test]
    public async Task WriteOnlyColumn_IsWrittenButNotProjected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TableDescriptor table) =
            await SetupRobotsTable();
        SetColumnState(table, "enabled", SchemaElementState.WriteOnly);

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(tx, dbname, "robots", [
            new()
            {
                ["id"] = new(ColumnType.Id, "000000000000000000000001"),
                ["name"] = new(ColumnType.String, "r2"),
                ["enabled"] = new(ColumnType.Bool, true)
            }
        ]));
        await database.Transactions.CommitAsync(tx);

        KvTransaction queryTx = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(queryTx, dbname, "SELECT * FROM robots", null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(queryTx);

        Assert.AreEqual(1, rows.Count);
        Assert.True(rows[0].Row.ContainsKey("id"));
        Assert.True(rows[0].Row.ContainsKey("name"));
        Assert.False(rows[0].Row.ContainsKey("enabled"));

        Dictionary<string, ColumnValue> raw = await LoadSingleWritableRow(database, table);
        Assert.True(raw.ContainsKey("enabled"));
        Assert.AreEqual(true, raw["enabled"].BoolValue);
    }

    [Test]
    public async Task DeleteOnlyColumn_IsUnknownToInsertAndSelect()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TableDescriptor table) =
            await SetupRobotsTable();
        SetColumnState(table, "enabled", SchemaElementState.DeleteOnly);

        KvTransaction insertTx = await database.Transactions.BeginAsync();
        CamusDBException? insertError = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.Insert(new InsertTicket(insertTx, dbname, "robots", [
                new()
                {
                    ["id"] = new(ColumnType.Id, "000000000000000000000002"),
                    ["name"] = new(ColumnType.String, "r3"),
                    ["enabled"] = new(ColumnType.Bool, true)
                }
            ])));
        await database.Transactions.RollbackIfNotCompletedAsync(insertTx);

        Assert.NotNull(insertError);
        Assert.AreEqual(CamusDBErrorCodes.UnknownColumn, insertError!.Code);

        KvTransaction queryTx = await database.Transactions.BeginAsync();
        CamusDBException? selectError = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(queryTx, dbname, "SELECT enabled FROM robots", null)));
        await database.Transactions.RollbackIfNotCompletedAsync(queryTx);

        Assert.NotNull(selectError);
        Assert.AreEqual(CamusDBErrorCodes.UnknownColumn, selectError!.Code);
    }

    [Test]
    public async Task Update_PreservesWriteOnlyColumn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TableDescriptor table) =
            await SetupRobotsTable();
        SetColumnState(table, "enabled", SchemaElementState.WriteOnly);

        KvTransaction insertTx = await database.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(insertTx, dbname, "robots", [
            new()
            {
                ["id"] = new(ColumnType.Id, "000000000000000000000003"),
                ["name"] = new(ColumnType.String, "old"),
                ["enabled"] = new(ColumnType.Bool, true)
            }
        ]));
        await database.Transactions.CommitAsync(insertTx);

        KvTransaction updateTx = await database.Transactions.BeginAsync();
        ExecuteNonSQLResult updated = await executor.ExecuteNonSQLQuery(
            new ExecuteSQLTicket(updateTx, dbname, "UPDATE robots SET name = \"new\" WHERE id = \"000000000000000000000003\"", null));
        await database.Transactions.CommitAsync(updateTx);

        Assert.AreEqual(1, updated.ModifiedRows);

        Dictionary<string, ColumnValue> raw = await LoadSingleWritableRow(database, table);
        Assert.AreEqual("new", raw["name"].StrValue);
        Assert.AreEqual(true, raw["enabled"].BoolValue);
    }

    [Test]
    public async Task Commit_RejectsPinnedTransactionWhenSchemaVersionChanges()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, TableDescriptor table) = await SetupRobotsTable();

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(tx, dbname, "robots", [
            new()
            {
                ["id"] = new(ColumnType.Id, "000000000000000000000004"),
                ["name"] = new(ColumnType.String, "r4")
            }
        ]));

        table.Schema.Version++;

        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(async () =>
            await database.Transactions.CommitAsync(tx));
        await database.Transactions.RollbackIfNotCompletedAsync(tx);

        Assert.NotNull(error);
        Assert.That(error!.Message, Does.Contain("pinned schema resource"));
    }

    [Test]
    public async Task Commit_RejectsPinnedTransactionWhenTableIsDropped()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _) = await SetupRobotsTable();

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(tx, dbname, "robots", [
            new()
            {
                ["id"] = new(ColumnType.Id, "000000000000000000000005"),
                ["name"] = new(ColumnType.String, "r5")
            }
        ]));

        database.Schema.Tables.Remove("robots");

        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(async () =>
            await database.Transactions.CommitAsync(tx));
        await database.Transactions.RollbackIfNotCompletedAsync(tx);

        Assert.NotNull(error);
        Assert.That(error!.Message, Does.Contain("no longer present"));
    }

    private async Task<(string DbName, DatabaseDescriptor Database, CommandExecutor Executor, TableDescriptor Table)> SetupRobotsTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "robots",
            columns:
            [
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("enabled", ColumnType.Bool)
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        ));

        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(dbname, "robots"));
        return (dbname, database, executor, table);
    }

    private static void SetColumnState(TableDescriptor table, string columnName, SchemaElementState state)
    {
        List<TableColumnSchema> current = table.Schema.Columns ?? [];
        table.Schema.Version++;
        table.Schema.Columns = current.Select(column =>
            column.Name == columnName
                ? new TableColumnSchema(column.Id, column.Name, column.Type, column.NotNull, column.DefaultValue, state)
                : column
        ).ToList();
        table.Schema.SchemaHistory ??= [];
        table.Schema.SchemaHistory.Add(new()
        {
            Version = table.Schema.Version,
            Columns = table.Schema.Columns
        });
    }

    private static async Task<Dictionary<string, ColumnValue>> LoadSingleWritableRow(
        DatabaseDescriptor database,
        TableDescriptor table
    )
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await foreach ((CamusDB.Core.Util.ObjectIds.ObjectIdValue rowId, ReadOnlyMemory<byte> data) in table.Store.ScanRows(tx))
            {
                Dictionary<string, ColumnValue> row =
                    await RowEncoder.DecodeWritableAsync(table.Schema, tx.TransactionId, rowId, data);
                await database.Transactions.CommitAsync(tx);
                return row;
            }
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }

        throw new AssertionException("Expected one row");
    }
}
