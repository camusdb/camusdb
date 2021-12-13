
using System.IO;
using NUnit.Framework;
using System.Threading.Tasks;
using System.Collections.Generic;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Journal;
using CamusDB.Tests.Utils;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.Journal.Models;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.Journal.Models.Logs;
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

    private JournalReader GetJournalReader(DatabaseDescriptor database)
    {        
        return new(
            Path.Combine(Config.DataDirectory, database.Name, "journal")
        );
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

    [Test]
    [NonParallelizable]
    public async Task TestJournalInsertTicketMultiple()
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

        InsertLog schedule = new(ticket.TableName, ticket.Values);
        uint sequence = await database.JournalWriter.Append(JournalFailureTypes.None, schedule);

        InsertCheckpointLog checkpointSchedule = new(sequence);
        uint checkpointSequence = await database.JournalWriter.Append(JournalFailureTypes.None, checkpointSchedule);

        database.JournalWriter.Close();

        JournalReader journalReader = GetJournalReader(database);

        List<JournalLog> journalLogs = new();

        await foreach (JournalLog journalLog in journalReader.ReadNextLog())
            journalLogs.Add(journalLog);

        Assert.AreEqual(2, journalLogs.Count);

        Assert.AreEqual(JournalLogTypes.Insert, journalLogs[0].Type);
        Assert.AreEqual(sequence, journalLogs[0].Sequence);
        Assert.IsInstanceOf<InsertLog>(journalLogs[0].InsertLog);
        Assert.AreEqual(ticket.TableName, journalLogs[0].InsertLog!.TableName);
        Assert.AreEqual(ticket.Values.Count, journalLogs[0].InsertLog!.Values.Count);

        Assert.AreEqual(JournalLogTypes.InsertCheckpoint, journalLogs[1].Type);
        Assert.AreEqual(checkpointSequence, journalLogs[1].Sequence);
        Assert.IsInstanceOf<InsertCheckpointLog>(journalLogs[1].InsertCheckpointLog);
        Assert.AreEqual(sequence, journalLogs[1].InsertCheckpointLog!.Sequence);
    }
}
