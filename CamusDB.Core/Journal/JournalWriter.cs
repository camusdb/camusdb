
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Journal.Models;
using CamusDB.Core.Journal.Controllers;
using Config = CamusDB.Core.CamusDBConfig;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.Journal;

public sealed class JournalWriter
{
    private uint logSequenceNumber = 0;

    private FileStream? fileStream;

    private readonly DatabaseDescriptor database;

    public JournalWriter(DatabaseDescriptor database)
    {
        this.database = database;
    }

    public async Task Initialize()
    {
        Console.WriteLine("Data journal saved at {0}", Config.DataDirectory + "/" + database.Name + "/journal");

        // @todo improve recovery here

        fileStream = new(Config.DataDirectory + "/" + database.Name + "/journal", FileMode.Append, FileAccess.Write);
        await Task.Yield();
    }

    public uint GetNextSequence()
    {
        return Interlocked.Increment(ref logSequenceNumber);
    }

    public async Task<uint> Append(JournalInsert insertSchedule)
    {
        if (fileStream is null)
            throw new Exception("Journal has not been initialized");

        uint sequence = GetNextSequence();

        InsertTicket insertTicket = insertSchedule.InsertTicket;

        byte[] journal = InsertTicketPayload.Generate(sequence, insertTicket);

        await fileStream.WriteAsync(journal);
        await fileStream.FlushAsync();

        return sequence;
    }

    public async Task<uint> Append(JournalInsertSlots insertSchedule)
    {
        if (fileStream is null)
            throw new Exception("Journal has not been initialized");

        uint sequence = GetNextSequence();

        byte[] journal = InsertSlotsPayload.Generate(sequence, insertSchedule.Sequence, insertSchedule.RowTuple);
        await fileStream.WriteAsync(journal);
        await fileStream.FlushAsync();

        return sequence;
    }

    public async Task<uint> Append(JournalWritePage insertSchedule)
    {
        if (fileStream is null)
            throw new Exception("Journal has not been initialized");

        uint sequence = GetNextSequence();

        byte[] journal = WritePagePayload.Generate(sequence, insertSchedule.Sequence, insertSchedule.Data);
        await fileStream.WriteAsync(journal);
        await fileStream.FlushAsync();

        return sequence;
    }

    public async Task<uint> Append(JournalUpdateUniqueIndex indexSchedule)
    {
        if (fileStream is null)
            throw new Exception("Journal has not been initialized");

        uint sequence = GetNextSequence();

        byte[] journal = UpdateUniqueIndexPayload.Generate(sequence, indexSchedule.Sequence, indexSchedule.Index);
        await fileStream.WriteAsync(journal);
        await fileStream.FlushAsync();

        return sequence;
    }

    public async Task<uint> Append(JournalUpdateUniqueCheckpoint indexCheckpoint)
    {
        if (fileStream is null)
            throw new Exception("Journal has not been initialized");

        uint sequence = GetNextSequence();

        byte[] journal = UpdateUniqueIndexCheckpointPayload.Generate(sequence, indexCheckpoint.Sequence, indexCheckpoint.Index);
        await fileStream.WriteAsync(journal);
        await fileStream.FlushAsync();

        return sequence;
    }

    public async Task<uint> Append(JournalInsertCheckpoint insertCheckpoint)
    {
        if (fileStream is null)
            throw new Exception("Journal has not been initialized");

        uint sequence = GetNextSequence();

        byte[] journal = InsertTicketCheckpointPayload.Generate(sequence, insertCheckpoint.Sequence);
        await fileStream.WriteAsync(journal);
        await fileStream.FlushAsync();

        return sequence;
    }
}
