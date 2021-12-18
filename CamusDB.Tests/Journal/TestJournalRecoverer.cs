
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.IO;
using NUnit.Framework;
using System.Threading.Tasks;
using System.Collections.Generic;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Tests.Utils;
using CamusDB.Core.Journal.Models;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.Journal.Models.Logs;
using CamusDB.Core.Journal.Controllers;
using Config = CamusDB.Core.CamusDBConfig;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Tests.Journal;

internal class TestJournalRecoverer
{
    private const string DatabaseName = "test";

    [SetUp]
    public void Setup()
    {
        SetupDb.Remove(DatabaseName);
    }

    private async Task<CommandExecutor> SetupDatabase()
    {
        CommandValidator validator = new();
        CatalogsManager catalogsManager = new();
        CommandExecutor executor = new(validator, catalogsManager);

        CreateDatabaseTicket databaseTicket = new(
            name: DatabaseName
        );

        await executor.CreateDatabase(databaseTicket);

        return executor;
    }

    private async Task<CommandExecutor> SetupBasicTable()
    {
        var executor = await SetupDatabase();

        CreateTableTicket tableTicket = new(
            database: DatabaseName,
            name: "robots",
            new ColumnInfo[]
            {
                new ColumnInfo("id", ColumnType.Id, primary: true),
                new ColumnInfo("name", ColumnType.String, notNull: true),
                new ColumnInfo("year", ColumnType.Integer),
                new ColumnInfo("enabled", ColumnType.Bool)
            }
        );

        await executor.CreateTable(tableTicket);

        return executor;
    }

    private InsertTicket GetInsertTicket(string id, JournalFailureTypes type)
    {
        InsertTicket ticket = new(
            database: DatabaseName,
            name: "robots",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Id, id) },
                { "name", new ColumnValue(ColumnType.String, "some name") },
                { "year", new ColumnValue(ColumnType.Integer, "1234") },
                { "enabled", new ColumnValue(ColumnType.Bool, "false") },
            },
            forceFailureType: type
        );

        return ticket;
    }

    private async Task CheckRecoveredRow(CommandExecutor executor)
    {
        QueryByIdTicket queryTicket = new(
            database: DatabaseName,
            name: "robots",
            id: 100
        );

        List<Dictionary<string, ColumnValue>> result = await executor.QueryById(queryTicket);

        Assert.AreEqual(1, result.Count);

        Dictionary<string, ColumnValue> row = result[0];

        Assert.AreEqual(row["id"].Type, ColumnType.Id);
        Assert.AreEqual(row["id"].Value, "100");

        Assert.AreEqual(row["name"].Type, ColumnType.String);
        Assert.AreEqual(row["name"].Value, "some name");

        Assert.AreEqual(row["year"].Type, ColumnType.Integer);
        Assert.AreEqual(row["year"].Value, "1234");
    }

    [Test]
    [NonParallelizable]
    public async Task TestJournalRecoverInsertFailPostInsert()
    {
        var executor = await SetupBasicTable();
        var database = await executor.OpenDatabase(DatabaseName);

        for (int i = 0; i < 5; i++)
            await executor.Insert(GetInsertTicket((200 + i).ToString(), JournalFailureTypes.None));

        var e = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.Insert(GetInsertTicket("100", JournalFailureTypes.PostInsert))
        );

        Assert.IsInstanceOf<CamusDBException>(e);

        await executor.CloseDatabase(new CloseDatabaseTicket(DatabaseName));

        database = await executor.OpenDatabase(DatabaseName);

        JournalVerifier journalVerifier = new();

        Dictionary<uint, JournalLogGroup> groups = await journalVerifier.Verify(
            Path.Combine(Config.DataDirectory, DatabaseName, "journal0")
        );

        Assert.AreEqual(1, groups.Count);

        JournalRecoverer recoverer = new();
        await recoverer.Recover(executor, database, groups);

        await CheckRecoveredRow(executor);
    }

    [Test]
    [NonParallelizable]
    public async Task TestJournalRecoverInsertFailPostInsertSlots()
    {
        var executor = await SetupBasicTable();
        var database = await executor.OpenDatabase(DatabaseName);

        for (int i = 0; i < 5; i++)
            await executor.Insert(GetInsertTicket((200 + i).ToString(), JournalFailureTypes.None));

        var e = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.Insert(GetInsertTicket("100", JournalFailureTypes.PostInsertSlots))
        );

        Assert.IsInstanceOf<CamusDBException>(e);

        await executor.CloseDatabase(new CloseDatabaseTicket(DatabaseName));

        database = await executor.OpenDatabase(DatabaseName);

        JournalVerifier journalVerifier = new();

        Dictionary<uint, JournalLogGroup> groups = await journalVerifier.Verify(
            Path.Combine(Config.DataDirectory, DatabaseName, "journal0")
        );

        Assert.AreEqual(1, groups.Count);

        JournalRecoverer recoverer = new();
        await recoverer.Recover(executor, database, groups);

        await CheckRecoveredRow(executor);
    }
}