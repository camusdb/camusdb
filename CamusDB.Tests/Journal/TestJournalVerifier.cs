
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
using CamusDB.Core;

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
    public async Task TestJournalInsertFail()
    {
        var executor = await SetupBasicTable();

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
            forceFailureType: JournalFailureTypes.PreInsert
        );

        var e = Assert.ThrowsAsync<CamusDBException>(async () => await executor.Insert(ticket));
        Assert.IsInstanceOf<CamusDBException>(e);

        /*InsertLog schedule = new(0, ticket.TableName, ticket.Values);
        uint sequence = await database.Journal.Writer.Append(JournalFailureTypes.None, schedule);

        InsertCheckpointLog checkpointSchedule = new(sequence);
        uint checkpointSequence = await database.Journal.Writer.Append(JournalFailureTypes.None, checkpointSchedule);

        database.Journal.Writer.Close();

        JournalVerifier journalVerifier = new();

        Dictionary<uint, JournalLogGroup> groups = await journalVerifier.Verify(
            Path.Combine(Config.DataDirectory, DatabaseName, "journal")
        );

        Assert.AreEqual(groups.Count, 0);*/
    }
}
