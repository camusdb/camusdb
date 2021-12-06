﻿
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
using CamusDB.Core.Journal.Models.Writers;
using CamusDB.Core.Journal.Controllers.Writers;

namespace CamusDB.Core.Journal;

public sealed class JournalWriter
{
    private uint logSequenceNumber = 0;

    private FileStream? journal;

    public DateTime LastFlush { get; private set; } = DateTime.Now;

    private readonly DatabaseDescriptor database;

    private readonly SemaphoreSlim semaphore = new(1, 1);    

    public JournalWriter(DatabaseDescriptor database)
    {
        this.database = database;
    }

    public async Task Initialize()
    {
        string path = Config.DataDirectory + "/" + database.Name + "/journal";

        Console.WriteLine("Data journal saved at {0}", path);

        // @todo improve recovery here        
        JournalReader journalReader = new(path);
        await journalReader.Verify();
        journalReader.Dispose();

        journal = new(path, FileMode.Append, FileAccess.Write);
    }

    private async Task TryWrite(byte[] buffer)
    {
        if (this.journal is null)
            throw new Exception("Journal has not been initialized");

        try
        {
            await semaphore.WaitAsync();

            await journal.WriteAsync(buffer);

            DateTime currentTime = DateTime.Now;

            if ((currentTime - LastFlush).TotalMilliseconds > Config.JournalFlushInterval)
            {
                await journal.FlushAsync();
                LastFlush = currentTime;
            }
        }
        finally
        {
            semaphore.Release();
        }
    }

    public uint GetNextSequence()
    {
        return Interlocked.Increment(ref logSequenceNumber);
    }

    public async Task<uint> Append(JournalInsert insertSchedule)
    {
        if (this.journal is null)
            throw new Exception("Journal has not been initialized");

        uint sequence = GetNextSequence();

        InsertTicket insertTicket = insertSchedule.InsertTicket;

        byte[] payload = InsertTicketWriter.Generate(sequence, insertTicket);

        await TryWrite(payload);

        return sequence;
    }

    public async Task<uint> Append(JournalInsertSlots insertSchedule)
    {
        if (this.journal is null)
            throw new Exception("Journal has not been initialized");

        uint sequence = GetNextSequence();

        byte[] payload = InsertSlotsWriter.Serialize(sequence, insertSchedule.Sequence, insertSchedule.RowTuple);

        await TryWrite(payload);

        return sequence;
    }

    public async Task<uint> Append(JournalWritePage insertSchedule)
    {
        if (this.journal is null)
            throw new Exception("Journal has not been initialized");

        uint sequence = GetNextSequence();

        byte[] payload = WritePageWriter.Generate(sequence, insertSchedule.Sequence, insertSchedule.Data);

        await TryWrite(payload);

        return sequence;
    }

    public async Task<uint> Append(JournalUpdateUniqueIndex indexSchedule)
    {
        if (this.journal is null)
            throw new Exception("Journal has not been initialized");

        uint sequence = GetNextSequence();

        byte[] payload = UpdateUniqueIndexWriter.Generate(sequence, indexSchedule.Sequence, indexSchedule.Index);

        await TryWrite(payload);

        return sequence;
    }

    public async Task<uint> Append(JournalUpdateUniqueCheckpoint indexCheckpoint)
    {
        if (this.journal is null)
            throw new Exception("Journal has not been initialized");

        uint sequence = GetNextSequence();

        byte[] payload = UpdateUniqueIndexCheckpointWriter.Generate(sequence, indexCheckpoint.Sequence, indexCheckpoint.Index);

        await TryWrite(payload);

        return sequence;
    }

    public async Task<uint> Append(JournalInsertCheckpoint insertCheckpoint)
    {
        if (this.journal is null)
            throw new Exception("Journal has not been initialized");

        uint sequence = GetNextSequence();

        byte[] payload = InsertTicketCheckpointWriter.Generate(sequence, insertCheckpoint.Sequence);

        await TryWrite(payload);

        return sequence;
    }
}
