
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
using CamusDB.Tests.Utils;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Journal;
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

internal class TestJournal
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
        uint sequence = await database.JournalWriter.Append(JournalFailureTypes.None, schedule);

        database.JournalWriter.Close();

        JournalReader journalReader = GetJournalReader(database);

        int total = 0;

        await foreach (JournalLog journalLog in journalReader.ReadNextLog())
        {
            Assert.AreEqual(JournalLogTypes.Insert, journalLog.Type);
            Assert.AreEqual(sequence, journalLog.Sequence);
            Assert.IsInstanceOf<InsertLog>(journalLog.InsertLog);
            Assert.AreEqual(ticket.TableName, journalLog.InsertLog!.TableName);
            Assert.AreEqual(ticket.Values.Count, journalLog.InsertLog!.Values.Count);

            Assert.AreEqual(ticket.Values["id"].Type, journalLog.InsertLog!.Values["id"].Type);
            Assert.AreEqual(ticket.Values["id"].Value, journalLog.InsertLog!.Values["id"].Value);

            Assert.AreEqual(ticket.Values["name"].Type, journalLog.InsertLog!.Values["name"].Type);
            Assert.AreEqual(ticket.Values["name"].Value, journalLog.InsertLog!.Values["name"].Value);

            Assert.AreEqual(ticket.Values["year"].Type, journalLog.InsertLog!.Values["year"].Type);
            Assert.AreEqual(ticket.Values["year"].Value, journalLog.InsertLog!.Values["year"].Value);

            Assert.AreEqual(ticket.Values["enabled"].Type, journalLog.InsertLog!.Values["enabled"].Type);
            Assert.AreEqual(ticket.Values["enabled"].Value, journalLog.InsertLog!.Values["enabled"].Value);
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
        uint sequence = await database.JournalWriter.Append(JournalFailureTypes.None, schedule);

        database.JournalWriter.Close();

        JournalReader journalReader = GetJournalReader(database);

        int total = 0;

        await foreach (JournalLog journalLog in journalReader.ReadNextLog())
        {
            Assert.AreEqual(JournalLogTypes.InsertSlots, journalLog.Type);
            Assert.AreEqual(sequence, journalLog.Sequence);
            Assert.IsInstanceOf<InsertSlotsLog>(journalLog.InsertSlotsLog);
            Assert.AreEqual(100, journalLog.InsertSlotsLog!.Sequence);
            Assert.AreEqual(50, journalLog.InsertSlotsLog!.RowTuple.SlotOne);
            Assert.AreEqual(25, journalLog.InsertSlotsLog!.RowTuple.SlotTwo);
            total++;
        }

        Assert.AreEqual(1, total);
    }

    [Test]
    [NonParallelizable]
    public async Task TestJournalWritePage()
    {
        CommandExecutor executor = await SetupDatabase();

        DatabaseDescriptor database = await executor.OpenDatabase(DatabaseName);

        WritePageLog schedule = new(100, new byte[5] { 1, 2, 3, 4, 5 });
        uint sequence = await database.JournalWriter.Append(JournalFailureTypes.None, schedule);

        database.JournalWriter.Close();

        JournalReader journalReader = GetJournalReader(database);

        int total = 0;

        await foreach (JournalLog journalLog in journalReader.ReadNextLog())
        {
            Assert.AreEqual(JournalLogTypes.WritePage, journalLog.Type);
            Assert.AreEqual(sequence, journalLog.Sequence);
            Assert.IsInstanceOf<WritePageLog>(journalLog.WritePageLog);
            Assert.AreEqual(100, journalLog.WritePageLog!.Sequence);
            Assert.AreEqual(5, journalLog.WritePageLog.Data.Length);
            total++;
        }

        Assert.AreEqual(1, total);
    }

    [Test]
    [NonParallelizable]
    public async Task TestJournalInsertCheckpoint()
    {
        CommandExecutor executor = await SetupDatabase();

        DatabaseDescriptor database = await executor.OpenDatabase(DatabaseName);

        InsertCheckpointLog schedule = new(100);
        uint sequence = await database.JournalWriter.Append(JournalFailureTypes.None, schedule);

        database.JournalWriter.Close();

        JournalReader journalReader = GetJournalReader(database);

        int total = 0;

        await foreach (JournalLog journalLog in journalReader.ReadNextLog())
        {
            Assert.AreEqual(JournalLogTypes.InsertCheckpoint, journalLog.Type);
            Assert.AreEqual(sequence, journalLog.Sequence);
            Assert.IsInstanceOf<InsertCheckpointLog>(journalLog.InsertCheckpointLog);
            Assert.AreEqual(100, journalLog.InsertCheckpointLog!.Sequence);
            total++;
        }

        Assert.AreEqual(1, total);
    }

    [Test]
    [NonParallelizable]
    public async Task TestJournalUpdateUniqueCheckpoint()
    {
        CommandExecutor executor = await SetupDatabase();

        DatabaseDescriptor database = await executor.OpenDatabase(DatabaseName);

        UpdateUniqueCheckpointLog schedule = new(100, "unique");
        uint sequence = await database.JournalWriter.Append(JournalFailureTypes.None, schedule);

        database.JournalWriter.Close();

        JournalReader journalReader = GetJournalReader(database);

        int total = 0;

        await foreach (JournalLog journalLog in journalReader.ReadNextLog())
        {
            Assert.AreEqual(JournalLogTypes.UpdateUniqueIndexCheckpoint, journalLog.Type);
            Assert.AreEqual(sequence, journalLog.Sequence);
            Assert.IsInstanceOf<UpdateUniqueCheckpointLog>(journalLog.UpdateUniqueCheckpointLog);
            Assert.AreEqual(100, journalLog.UpdateUniqueCheckpointLog!.Sequence);
            Assert.AreEqual("unique", journalLog.UpdateUniqueCheckpointLog!.ColumnIndex);
            total++;
        }

        Assert.AreEqual(1, total);
    }

    [Test]
    [NonParallelizable]
    public async Task TestJournalUpdateUniqueIndex()
    {
        CommandExecutor executor = await SetupDatabase();

        DatabaseDescriptor database = await executor.OpenDatabase(DatabaseName);

        UpdateUniqueIndexLog schedule = new(100, "unique");
        uint sequence = await database.JournalWriter.Append(JournalFailureTypes.None, schedule);

        database.JournalWriter.Close();

        JournalReader journalReader = GetJournalReader(database);

        int total = 0;

        await foreach (JournalLog journalLog in journalReader.ReadNextLog())
        {
            Assert.AreEqual(JournalLogTypes.UpdateUniqueIndex, journalLog.Type);
            Assert.AreEqual(sequence, journalLog.Sequence);
            Assert.IsInstanceOf<UpdateUniqueIndexLog>(journalLog.UpdateUniqueIndexLog);
            Assert.AreEqual(100, journalLog.UpdateUniqueIndexLog!.Sequence);
            Assert.AreEqual("unique", journalLog.UpdateUniqueIndexLog!.ColumnIndex);
            total++;
        }

        Assert.AreEqual(1, total);
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
