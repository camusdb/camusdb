
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

internal class TestJournalVerifier
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

    private InsertTicket GetInsertTicket(JournalFailureTypes type)
    {
        InsertTicket ticket = new(
            database: DatabaseName,
            name: "robots",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Id, "1") },
                { "name", new ColumnValue(ColumnType.String, "some name") },
                { "year", new ColumnValue(ColumnType.Integer, "1234") },
                { "enabled", new ColumnValue(ColumnType.Bool, "false") },
            },
            forceFailureType: type
        );

        return ticket;
    }

    [Test]
    [NonParallelizable]
    public async Task TestJournalInsertTicketCheckpoint()
    {
        CommandExecutor executor = await SetupDatabase();

        DatabaseDescriptor database = await executor.OpenDatabase(DatabaseName);

        InsertTicket ticket = new(
            database: DatabaseName,
            name: "robots",
            values: new Dictionary<string, ColumnValue>()
            {
                { "id", new ColumnValue(ColumnType.Id, "1") },
                { "name", new ColumnValue(ColumnType.String, "some name") },
                { "year", new ColumnValue(ColumnType.Integer, "1234") },
                { "enabled", new ColumnValue(ColumnType.Bool, "false") },
            }
        );

        InsertLog schedule = new(0, ticket.TableName, ticket.Values);
        uint sequence = await database.Journal.Writer.Append(JournalFailureTypes.None, schedule);

        InsertCheckpointLog checkpointSchedule = new(sequence);
        uint checkpointSequence = await database.Journal.Writer.Append(JournalFailureTypes.None, checkpointSchedule);

        database.Journal.Writer.Close();

        JournalVerifier journalVerifier = new();

        Dictionary<uint, JournalLogGroup> groups = await journalVerifier.Verify(
            Path.Combine(Config.DataDirectory, DatabaseName, "journal")
        );

        Assert.AreEqual(groups.Count, 0);
    }

    [Test]
    [NonParallelizable]
    public async Task TestJournalInsertFailPreInsert()
    {
        var executor = await SetupBasicTable();

        var e = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.Insert(GetInsertTicket(JournalFailureTypes.PreInsert))
        );

        Assert.IsInstanceOf<CamusDBException>(e);

        await executor.CloseDatabase(new CloseDatabaseTicket(DatabaseName));     

        JournalVerifier journalVerifier = new();

        Dictionary<uint, JournalLogGroup> groups = await journalVerifier.Verify(
            Path.Combine(Config.DataDirectory, DatabaseName, "journal")
        );

        Assert.AreEqual(groups.Count, 0);
    }

    [Test]
    [NonParallelizable]
    public async Task TestJournalInsertFailPostInsert()
    {
        var executor = await SetupBasicTable();

        var e = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.Insert(GetInsertTicket(JournalFailureTypes.PostInsert))
        );

        Assert.IsInstanceOf<CamusDBException>(e);

        await executor.CloseDatabase(new CloseDatabaseTicket(DatabaseName));

        JournalVerifier journalVerifier = new();

        Dictionary<uint, JournalLogGroup> groups = await journalVerifier.Verify(
            Path.Combine(Config.DataDirectory, DatabaseName, "journal")
        );

        Assert.AreEqual(1, groups.Count);
        Assert.AreEqual(1, groups[1].Logs.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestJournalInsertFailPreInsertSlots()
    {
        var executor = await SetupBasicTable();        

        var e = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.Insert(GetInsertTicket(JournalFailureTypes.PreInsertSlots))
        );

        Assert.IsInstanceOf<CamusDBException>(e);

        await executor.CloseDatabase(new CloseDatabaseTicket(DatabaseName));

        JournalVerifier journalVerifier = new();

        Dictionary<uint, JournalLogGroup> groups = await journalVerifier.Verify(
            Path.Combine(Config.DataDirectory, DatabaseName, "journal")
        );

        Assert.AreEqual(1, groups.Count);
        Assert.AreEqual(1, groups[1].Logs.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestJournalInsertFailPostInsertSlots()
    {
        var executor = await SetupBasicTable();

        var e = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.Insert(GetInsertTicket(JournalFailureTypes.PostInsertSlots))
        );
        
        Assert.IsInstanceOf<CamusDBException>(e);

        await executor.CloseDatabase(new CloseDatabaseTicket(DatabaseName));

        JournalVerifier journalVerifier = new();

        Dictionary<uint, JournalLogGroup> groups = await journalVerifier.Verify(
            Path.Combine(Config.DataDirectory, DatabaseName, "journal")
        );

        Assert.AreEqual(1, groups.Count);
        Assert.AreEqual(2, groups[1].Logs.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestJournalInsertFailPreUpdateUniqueIndex()
    {
        var executor = await SetupBasicTable();

        var e = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.Insert(GetInsertTicket(JournalFailureTypes.PreUpdateUniqueIndex))
        );
        
        Assert.IsInstanceOf<CamusDBException>(e);

        await executor.CloseDatabase(new CloseDatabaseTicket(DatabaseName));

        JournalVerifier journalVerifier = new();

        Dictionary<uint, JournalLogGroup> groups = await journalVerifier.Verify(
            Path.Combine(Config.DataDirectory, DatabaseName, "journal")
        );

        Assert.AreEqual(1, groups.Count);
        Assert.AreEqual(2, groups[1].Logs.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestJournalInsertFailPostUpdateUniqueIndex()
    {
        var executor = await SetupBasicTable();

        var e = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.Insert(GetInsertTicket(JournalFailureTypes.PostUpdateUniqueIndex))
        );

        Assert.IsInstanceOf<CamusDBException>(e);

        await executor.CloseDatabase(new CloseDatabaseTicket(DatabaseName));

        JournalVerifier journalVerifier = new();

        Dictionary<uint, JournalLogGroup> groups = await journalVerifier.Verify(
            Path.Combine(Config.DataDirectory, DatabaseName, "journal")
        );

        Assert.AreEqual(1, groups.Count);
        Assert.AreEqual(3, groups[1].Logs.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestJournalInsertFailPreWritePage()
    {
        var executor = await SetupBasicTable();

        var e = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.Insert(GetInsertTicket(JournalFailureTypes.PreWritePage))
        );
        
        Assert.IsInstanceOf<CamusDBException>(e);

        await executor.CloseDatabase(new CloseDatabaseTicket(DatabaseName));

        JournalVerifier journalVerifier = new();

        Dictionary<uint, JournalLogGroup> groups = await journalVerifier.Verify(
            Path.Combine(Config.DataDirectory, DatabaseName, "journal")
        );

        Assert.AreEqual(1, groups.Count);
        Assert.AreEqual(3, groups[1].Logs.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestJournalInsertFailPostWritePage()
    {
        var executor = await SetupBasicTable();

        var e = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.Insert(GetInsertTicket(JournalFailureTypes.PostWritePage))
        );
        
        Assert.IsInstanceOf<CamusDBException>(e);

        await executor.CloseDatabase(new CloseDatabaseTicket(DatabaseName));

        JournalVerifier journalVerifier = new();

        Dictionary<uint, JournalLogGroup> groups = await journalVerifier.Verify(
            Path.Combine(Config.DataDirectory, DatabaseName, "journal")
        );

        Assert.AreEqual(1, groups.Count);
        Assert.AreEqual(4, groups[1].Logs.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestJournalInsertFailPreUpdateUniqueIndexCheckpoint()
    {
        var executor = await SetupBasicTable();

        var e = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.Insert(GetInsertTicket(JournalFailureTypes.PreUpdateUniqueCheckpoint))
        );

        Assert.IsInstanceOf<CamusDBException>(e);

        await executor.CloseDatabase(new CloseDatabaseTicket(DatabaseName));

        JournalVerifier journalVerifier = new();

        Dictionary<uint, JournalLogGroup> groups = await journalVerifier.Verify(
            Path.Combine(Config.DataDirectory, DatabaseName, "journal")
        );

        Assert.AreEqual(1, groups.Count);
        Assert.AreEqual(5, groups[1].Logs.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestJournalInsertFailPostUpdateUniqueIndexCheckpoint()
    {
        var executor = await SetupBasicTable();

        var e = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.Insert(GetInsertTicket(JournalFailureTypes.PostUpdateUniqueCheckpoint))
        );

        Assert.IsInstanceOf<CamusDBException>(e);

        await executor.CloseDatabase(new CloseDatabaseTicket(DatabaseName));

        JournalVerifier journalVerifier = new();

        Dictionary<uint, JournalLogGroup> groups = await journalVerifier.Verify(
            Path.Combine(Config.DataDirectory, DatabaseName, "journal")
        );

        Assert.AreEqual(1, groups.Count);
        Assert.AreEqual(6, groups[1].Logs.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestJournalInsertFailPreInsertCheckpoint()
    {
        var executor = await SetupBasicTable();

        var e = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.Insert(GetInsertTicket(JournalFailureTypes.PreInsertCheckpoint))
        );

        Assert.IsInstanceOf<CamusDBException>(e);

        await executor.CloseDatabase(new CloseDatabaseTicket(DatabaseName));

        JournalVerifier journalVerifier = new();

        Dictionary<uint, JournalLogGroup> groups = await journalVerifier.Verify(
            Path.Combine(Config.DataDirectory, DatabaseName, "journal")
        );

        Assert.AreEqual(1, groups.Count);
        Assert.AreEqual(7, groups[1].Logs.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TestJournalInsertFailPostInsertCheckpoint()
    {
        var executor = await SetupBasicTable();

        var e = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.Insert(GetInsertTicket(JournalFailureTypes.PostInsertCheckpoint))
        );

        Assert.IsInstanceOf<CamusDBException>(e);

        await executor.CloseDatabase(new CloseDatabaseTicket(DatabaseName));

        JournalVerifier journalVerifier = new();

        Dictionary<uint, JournalLogGroup> groups = await journalVerifier.Verify(
            Path.Combine(Config.DataDirectory, DatabaseName, "journal")
        );

        Assert.AreEqual(0, groups.Count);
        //Assert.AreEqual(8, groups[1].Logs.Count);
    }
}
