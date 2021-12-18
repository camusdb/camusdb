
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

using CamusDB.Tests.Utils;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.Journal.Models;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.Journal.Models.Logs;
using Config = CamusDB.Core.CamusDBConfig;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Journal.Controllers;

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
            Path.Combine(Config.DataDirectory, database.Name, "journal0")
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

        InsertLog schedule = new(0, ticket.TableName, ticket.Values);
        uint sequence = await database.Journal.Writer.Append(JournalFailureTypes.None, schedule);

        database.Journal.Writer.Close();

        JournalReader journalReader = GetJournalReader(database);

        int total = 0;

        await foreach (JournalLog journalLog in journalReader.ReadNextLog())
        {
            Assert.AreEqual(JournalLogTypes.Insert, journalLog.Type);
            Assert.AreEqual(sequence, journalLog.Sequence);

            InsertLog? insertLog = journalLog.Log as InsertLog;

            Assert.IsInstanceOf<InsertLog>(insertLog);
            Assert.AreEqual(ticket.TableName, insertLog!.TableName);
            Assert.AreEqual(ticket.Values.Count, insertLog!.Values.Count);

            Assert.AreEqual(ticket.Values["id"].Type, insertLog.Values["id"].Type);
            Assert.AreEqual(ticket.Values["id"].Value, insertLog.Values["id"].Value);

            Assert.AreEqual(ticket.Values["name"].Type, insertLog.Values["name"].Type);
            Assert.AreEqual(ticket.Values["name"].Value, insertLog.Values["name"].Value);

            Assert.AreEqual(ticket.Values["year"].Type, insertLog.Values["year"].Type);
            Assert.AreEqual(ticket.Values["year"].Value, insertLog.Values["year"].Value);

            Assert.AreEqual(ticket.Values["enabled"].Type, insertLog.Values["enabled"].Type);
            Assert.AreEqual(ticket.Values["enabled"].Value, insertLog.Values["enabled"].Value);
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
        uint sequence = await database.Journal.Writer.Append(JournalFailureTypes.None, schedule);

        database.Journal.Writer.Close();

        JournalReader journalReader = GetJournalReader(database);

        int total = 0;

        await foreach (JournalLog journalLog in journalReader.ReadNextLog())
        {
            Assert.AreEqual(JournalLogTypes.InsertSlots, journalLog.Type);
            Assert.AreEqual(sequence, journalLog.Sequence);

            InsertSlotsLog? insertLog = journalLog.Log as InsertSlotsLog;

            Assert.IsInstanceOf<InsertSlotsLog>(insertLog);
            Assert.AreEqual(100, insertLog!.Sequence);
            Assert.AreEqual(50, insertLog.RowTuple.SlotOne);
            Assert.AreEqual(25, insertLog.RowTuple.SlotTwo);
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

        WritePageLog schedule = new(100, 0, new byte[5] { 1, 2, 3, 4, 5 });
        uint sequence = await database.Journal.Writer.Append(JournalFailureTypes.None, schedule);

        database.Journal.Writer.Close();

        JournalReader journalReader = GetJournalReader(database);

        int total = 0;

        await foreach (JournalLog journalLog in journalReader.ReadNextLog())
        {
            Assert.AreEqual(JournalLogTypes.WritePage, journalLog.Type);
            Assert.AreEqual(sequence, journalLog.Sequence);

            WritePageLog? writePageLog = journalLog.Log as WritePageLog;

            Assert.IsInstanceOf<WritePageLog>(writePageLog);
            Assert.AreEqual(100, writePageLog!.Sequence);
            Assert.AreEqual(5, writePageLog.Data.Length);
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
        uint sequence = await database.Journal.Writer.Append(JournalFailureTypes.None, schedule);

        database.Journal.Writer.Close();

        JournalReader journalReader = GetJournalReader(database);

        int total = 0;

        await foreach (JournalLog journalLog in journalReader.ReadNextLog())
        {
            Assert.AreEqual(JournalLogTypes.InsertCheckpoint, journalLog.Type);
            Assert.AreEqual(sequence, journalLog.Sequence);

            InsertCheckpointLog? insertCheckpointLog = journalLog.Log as InsertCheckpointLog;

            Assert.IsInstanceOf<InsertCheckpointLog>(insertCheckpointLog);
            Assert.AreEqual(100, insertCheckpointLog!.Sequence);
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
        uint sequence = await database.Journal.Writer.Append(JournalFailureTypes.None, schedule);

        database.Journal.Writer.Close();

        JournalReader journalReader = GetJournalReader(database);

        int total = 0;

        await foreach (JournalLog journalLog in journalReader.ReadNextLog())
        {
            Assert.AreEqual(JournalLogTypes.UpdateUniqueIndexCheckpoint, journalLog.Type);
            Assert.AreEqual(sequence, journalLog.Sequence);

            UpdateUniqueCheckpointLog? updateUniqueCheckpointLog = journalLog.Log as UpdateUniqueCheckpointLog;

            Assert.IsInstanceOf<UpdateUniqueCheckpointLog>(updateUniqueCheckpointLog);
            Assert.AreEqual(100, updateUniqueCheckpointLog!.Sequence);
            Assert.AreEqual("unique", updateUniqueCheckpointLog.ColumnIndex);
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
        uint sequence = await database.Journal.Writer.Append(JournalFailureTypes.None, schedule);

        database.Journal.Writer.Close();

        JournalReader journalReader = GetJournalReader(database);

        int total = 0;

        await foreach (JournalLog journalLog in journalReader.ReadNextLog())
        {
            Assert.AreEqual(JournalLogTypes.UpdateUniqueIndex, journalLog.Type);
            Assert.AreEqual(sequence, journalLog.Sequence);

            UpdateUniqueIndexLog? updateUniqueIndexLog = journalLog.Log as UpdateUniqueIndexLog;

            Assert.IsInstanceOf<UpdateUniqueIndexLog>(updateUniqueIndexLog);
            Assert.AreEqual(100, updateUniqueIndexLog!.Sequence);
            Assert.AreEqual("unique", updateUniqueIndexLog!.ColumnIndex);
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

        InsertLog schedule = new(0, ticket.TableName, ticket.Values);
        uint sequence = await database.Journal.Writer.Append(JournalFailureTypes.None, schedule);

        InsertCheckpointLog checkpointSchedule = new(sequence);
        uint checkpointSequence = await database.Journal.Writer.Append(JournalFailureTypes.None, checkpointSchedule);

        database.Journal.Writer.Close();

        JournalReader journalReader = GetJournalReader(database);

        List<JournalLog> journalLogs = new();

        await foreach (JournalLog journalLog in journalReader.ReadNextLog())
            journalLogs.Add(journalLog);

        Assert.AreEqual(2, journalLogs.Count);

        Assert.AreEqual(JournalLogTypes.Insert, journalLogs[0].Type);
        Assert.AreEqual(sequence, journalLogs[0].Sequence);

        InsertLog? insertLog = journalLogs[0].Log! as InsertLog;

        Assert.IsInstanceOf<InsertLog>(insertLog);
        Assert.AreEqual(ticket.TableName, insertLog!.TableName);
        Assert.AreEqual(ticket.Values.Count, insertLog.Values.Count);

        Assert.AreEqual(JournalLogTypes.InsertCheckpoint, journalLogs[1].Type);
        Assert.AreEqual(checkpointSequence, journalLogs[1].Sequence);

        InsertCheckpointLog? insertCheckpointLog = journalLogs[1].Log! as InsertCheckpointLog;

        Assert.IsInstanceOf<InsertCheckpointLog>(insertCheckpointLog);
        Assert.AreEqual(sequence, insertCheckpointLog!.Sequence);
    }
}
