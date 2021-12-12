
using System.IO;
using CamusDB.Core;
using NUnit.Framework;
using CamusDB.Core.Catalogs;
using System.Threading.Tasks;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor;
using Config = CamusDB.Core.CamusDBConfig;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Journal.Models.Writers;
using System.Collections.Generic;
using CamusDB.Core.Journal.Models.Logs;
using CamusDB.Core.Journal;
using CamusDB.Core.Journal.Models;
using CamusDB.Core.Util.Trees;

namespace CamusDB.Tests.Journal;

public class TestJournal
{
    private const string DatabaseName = "test";

    [SetUp]
    public void Setup()
    {
        string path = Config.DataDirectory + "/" + DatabaseName;
        if (Directory.Exists(path))
        {
            File.Delete(path + "/tablespace0");
            File.Delete(path + "/schema");
            File.Delete(path + "/system");
            File.Delete(path + "/journal");
            Directory.Delete(path);
        }
    }

    private JournalReader GetJournalReader(DatabaseDescriptor database)
    {
        string path = Config.DataDirectory + "/" + database.Name + "/journal";
        return new(path);
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
    public async Task TestJournalInsertTicket()
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
        uint sequence = await database.JournalWriter.Append(schedule);

        database.JournalWriter.Close();

        JournalReader journalReader = GetJournalReader(database);

        int total = 0;

        await foreach (JournalLog journalLog in journalReader.ReadNextLog())
        {
            Assert.AreEqual(JournalLogTypes.Insert, journalLog.Type);
            Assert.AreEqual(sequence, journalLog.Sequence);
            Assert.IsInstanceOf<InsertLog>(journalLog.InsertLog);
            Assert.AreEqual(journalLog.InsertLog.TableName, ticket.TableName);
            total++;
        }

        Assert.AreEqual(1, total);        
    }

    [Test]
    [NonParallelizable]
    public async Task TestJournalInsertSlots()
    {
        CommandExecutor executor = await SetupDatabase();

        DatabaseDescriptor database = await executor.OpenDatabase(DatabaseName);
        
        InsertSlotsLog schedule = new(100, new BTreeTuple(50, 25));
        uint sequence = await database.JournalWriter.Append(schedule);

        database.JournalWriter.Close();

        JournalReader journalReader = GetJournalReader(database);

        int total = 0;

        await foreach (JournalLog journalLog in journalReader.ReadNextLog())
        {
            Assert.AreEqual(JournalLogTypes.InsertSlots, journalLog.Type);
            Assert.AreEqual(sequence, journalLog.Sequence);
            //Assert.IsInstanceOf<InsertLog>(journalLog.InsertTicketLog);
            //Assert.AreEqual(journalLog.InsertTicketLog.TableName, ticket.TableName);
            total++;
        }

        Assert.AreEqual(1, total);
    }
}
