
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Journal.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.Journal.Models.Logs;
using Config = CamusDB.Core.CamusDBConfig;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.Journal.Controllers.Controllers;

public sealed class JournalWriter
{
    private uint logSequenceNumber = 0;

    private FileStream? journal;

    public DateTime LastFlush { get; private set; } = DateTime.Now;

    private readonly string database;

    private readonly SemaphoreSlim semaphore = new(1, 1);

    public JournalWriter(string name)
    {
        this.database = name;
    }

    public async Task Initialize(CommandExecutor executor)
    {
        string path = Path.Combine(Config.DataDirectory, database, "journal");
       
        // @todo improve recovery here        
        JournalVerifier verifier = new();

        Dictionary<uint, JournalLogGroup> groups = await verifier.Verify(path);
        if (groups.Count > 0)
        {
            JournalRecoverer recoverer = new();
            await recoverer.Recover(executor, groups);
        }

        // Remove existing journal
        File.Delete(path);

        journal = new(path, FileMode.Append, FileAccess.Write);
    }

    private async Task TryWrite(byte[] buffer)
    {
        if (this.journal is null)
            throw new CamusDBException(
                CamusDBErrorCodes.JournalNotInitialized,
                "Journal has not been initialized"
            );

        try
        {
            await semaphore.WaitAsync();

            await journal.WriteAsync(buffer);

            DateTime currentTime = DateTime.Now;

            //if ((currentTime - LastFlush).TotalMilliseconds > Config.JournalFlushInterval)
            //{
            await journal.FlushAsync();
            //    LastFlush = currentTime;
            //}
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

    public void ForceFailure(JournalFailureTypes failureType)
    {
        throw new CamusDBException(
            CamusDBErrorCodes.JournalForcedFailure,
            "Journal forced failure: " + failureType
        );
    }

    public async Task<uint> Append(JournalFailureTypes failureType, InsertLog insertSchedule)
    {
        Console.WriteLine("JournalInsert ?");

        if (failureType == JournalFailureTypes.PreInsert)
            ForceFailure(failureType);
       
        uint sequence = GetNextSequence();

        byte[] payload = InsertLogSerializator.Serialize(sequence, insertSchedule);

        await TryWrite(payload);

        if (failureType == JournalFailureTypes.PostInsert)
            ForceFailure(failureType);

        return sequence;
    }

    public async Task<uint> Append(JournalFailureTypes failureType, InsertSlotsLog insertSchedule)
    {
        Console.WriteLine("JournalInsertSlots");        

        uint sequence = GetNextSequence();

        byte[] payload = InsertSlotsLogSerializator.Serialize(sequence, insertSchedule);

        await TryWrite(payload);

        return sequence;
    }

    public async Task<uint> Append(JournalFailureTypes failureType, WritePageLog insertSchedule)
    {
        Console.WriteLine("JournalWritePage");        

        uint sequence = GetNextSequence();

        byte[] payload = WritePageLogSerializator.Serialize(sequence, insertSchedule);

        await TryWrite(payload);

        return sequence;
    }

    public async Task<uint> Append(JournalFailureTypes failureType, UpdateUniqueIndexLog indexSchedule)
    {
        Console.WriteLine("JournalUpdateUniqueIndex");        

        uint sequence = GetNextSequence();

        byte[] payload = UpdateUniqueIndexLogSerializator.Serialize(sequence, indexSchedule);

        await TryWrite(payload);

        return sequence;
    }

    public async Task<uint> Append(JournalFailureTypes failureType, UpdateUniqueCheckpointLog indexCheckpoint)
    {
        Console.WriteLine("JournalUpdateUniqueCheckpoint");        

        uint sequence = GetNextSequence();

        byte[] payload = UpdateUniqueCheckpointLogSerializator.Serialize(sequence, indexCheckpoint);

        await TryWrite(payload);

        return sequence;
    }

    public async Task<uint> Append(JournalFailureTypes failureType, InsertCheckpointLog insertCheckpoint)
    {
        Console.WriteLine("JournalInsertCheckpoint");        

        uint sequence = GetNextSequence();

        byte[] payload = InsertCheckpointLogSerializator.Serialize(sequence, insertCheckpoint);

        await TryWrite(payload);

        return sequence;
    }

    public async Task<uint> Append(JournalFailureTypes failureType, FlushedPagesLog flushedPages)
    {
        Console.WriteLine("FlushedPages");

        uint sequence = GetNextSequence();

        byte[] payload = FlushedPagesLogSerializator.Serialize(sequence, flushedPages);

        await TryWrite(payload);

        return sequence;
    }

    public void Close()
    {
        if (journal != null)
        {
            journal.Flush();
            journal.Dispose();
        }
    }
}
