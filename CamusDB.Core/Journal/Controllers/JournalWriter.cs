
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

namespace CamusDB.Core.Journal.Controllers;

public sealed class JournalWriter
{
    private uint logSequenceNumber = 0;

    private uint lastSequence = 0;

    private uint lastFlushedSequence = 0;

    private FileStream? journal;

    private readonly string database;

    private readonly SemaphoreSlim semaphore = new(1, 1);

    public JournalWriter(string name)
    {
        this.database = name;
    }

    private static List<FileInfo> GetJournals(string name)
    {
        DirectoryInfo directory = new(
            Path.Combine(Config.DataDirectory, name)
        );

        List<FileInfo> journals = new();

        FileInfo[] files = directory.GetFiles();

        for (int i = 0; i < files.Length; i++)
        {
            FileInfo file = files[i];

            if (file.Name.Length >= 7 && file.Name[..7] == "journal")
                journals.Add(file);
        }

        return journals;
    }

    private async Task RecoverJournals(CommandExecutor executor, DatabaseDescriptor database, List<FileInfo> journals)
    {
        JournalVerifier verifier = new();
        JournalRecoverer recoverer = new();

        foreach (FileInfo file in journals)
        {
            Console.WriteLine("{0}", file.FullName);

            Dictionary<uint, JournalLogGroup> groups = await verifier.Verify(file.FullName);
            if (groups.Count > 0)
                await recoverer.Recover(executor, database, groups);
        }
    }

    private async Task<int> GetNextJournal(List<FileInfo> journals)
    {
        foreach (FileInfo file in journals)
        {
            int number = int.Parse(file.Name.Replace("journal", ""));
            Console.WriteLine(number);
        }

        var r = new System.Random();
        return r.Next(1000, 9999);
    }

    public async Task Initialize(CommandExecutor executor, DatabaseDescriptor database)
    {
        List<FileInfo> journals = GetJournals(database.Name);

        int next = await GetNextJournal(journals);

        journal = new(
            Path.Combine(Config.DataDirectory, this.database, "journal" + next.ToString()),
            FileMode.Append,
            FileAccess.Write
        );

        await RecoverJournals(executor, database, journals);

        Console.WriteLine("{0}", journal.Name);
    }

    private async Task TryWrite(uint lastSequence, byte[] buffer)
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

            this.lastSequence = lastSequence;
        }
        finally
        {
            semaphore.Release();
        }
    }

    public async Task Flush()
    {
        if (lastFlushedSequence == lastSequence)
            return;

        if (this.journal is null)
            throw new CamusDBException(
                CamusDBErrorCodes.JournalNotInitialized,
                "Journal has not been initialized"
            );

        try
        {
            await semaphore.WaitAsync();

            await journal.FlushAsync();

            lastFlushedSequence = lastSequence;
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
        //Console.WriteLine("JournalInsert ?");

        if (failureType == JournalFailureTypes.PreInsert)
            ForceFailure(failureType);

        uint sequence = GetNextSequence();

        byte[] payload = InsertLogSerializator.Serialize(sequence, insertSchedule);

        await TryWrite(sequence, payload);

        if (failureType == JournalFailureTypes.PostInsert)
            ForceFailure(failureType);

        return sequence;
    }

    public async Task<uint> Append(JournalFailureTypes failureType, InsertSlotsLog insertSchedule)
    {
        //Console.WriteLine("JournalInsertSlots");

        if (failureType == JournalFailureTypes.PreInsertSlots)
            ForceFailure(failureType);

        uint sequence = GetNextSequence();

        byte[] payload = InsertSlotsLogSerializator.Serialize(sequence, insertSchedule);

        await TryWrite(sequence, payload);

        if (failureType == JournalFailureTypes.PostInsertSlots)
            ForceFailure(failureType);

        return sequence;
    }

    public async Task<uint> Append(JournalFailureTypes failureType, WritePageLog insertSchedule)
    {
        //Console.WriteLine("JournalWritePage");

        if (failureType == JournalFailureTypes.PreWritePage)
            ForceFailure(failureType);

        uint sequence = GetNextSequence();

        byte[] payload = WritePageLogSerializator.Serialize(sequence, insertSchedule);

        await TryWrite(sequence, payload);

        if (failureType == JournalFailureTypes.PostWritePage)
            ForceFailure(failureType);

        return sequence;
    }

    public async Task<uint> Append(JournalFailureTypes failureType, UpdateUniqueIndexLog indexSchedule)
    {
        //Console.WriteLine("JournalUpdateUniqueIndex");

        if (failureType == JournalFailureTypes.PreUpdateUniqueIndex)
            ForceFailure(failureType);

        uint sequence = GetNextSequence();

        byte[] payload = UpdateUniqueIndexLogSerializator.Serialize(sequence, indexSchedule);

        await TryWrite(sequence, payload);

        if (failureType == JournalFailureTypes.PostUpdateUniqueIndex)
            ForceFailure(failureType);

        return sequence;
    }

    public async Task<uint> Append(JournalFailureTypes failureType, UpdateUniqueCheckpointLog indexCheckpoint)
    {
        //Console.WriteLine("JournalUpdateUniqueCheckpoint");

        if (failureType == JournalFailureTypes.PreUpdateUniqueCheckpoint)
            ForceFailure(failureType);

        uint sequence = GetNextSequence();

        byte[] payload = UpdateUniqueCheckpointLogSerializator.Serialize(sequence, indexCheckpoint);

        await TryWrite(sequence, payload);

        if (failureType == JournalFailureTypes.PostUpdateUniqueCheckpoint)
            ForceFailure(failureType);

        return sequence;
    }

    public async Task<uint> Append(JournalFailureTypes failureType, InsertCheckpointLog insertCheckpoint)
    {
        //Console.WriteLine("JournalInsertCheckpoint");

        if (failureType == JournalFailureTypes.PreInsertCheckpoint)
            ForceFailure(failureType);

        uint sequence = GetNextSequence();

        byte[] payload = InsertCheckpointLogSerializator.Serialize(sequence, insertCheckpoint);

        await TryWrite(sequence, payload);

        if (failureType == JournalFailureTypes.PostInsertCheckpoint)
            ForceFailure(failureType);

        return sequence;
    }

    public async Task<uint> Append(JournalFailureTypes failureType, FlushedPagesLog flushedPages)
    {
        //Console.WriteLine("FlushedPages");

        uint sequence = GetNextSequence();

        byte[] payload = FlushedPagesLogSerializator.Serialize(sequence, flushedPages);

        await TryWrite(sequence, payload);

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
