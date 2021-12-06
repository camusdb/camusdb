
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using CamusDB.Core.Serializer;
using CamusDB.Core.Journal.Models;
using CamusDB.Core.Journal.Controllers;
using Config = CamusDB.Core.CamusDBConfig;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Journal.Controllers.Readers;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.Journal;

public sealed class JournalReader : IDisposable
{
    private readonly FileStream journal;

    public JournalReader(string path)
    {
        this.journal = new FileStream(path, FileMode.OpenOrCreate, FileAccess.Read);
    }

    public async Task Verify()
    {
        journal.Seek(0, SeekOrigin.Begin);

        byte[] header = new byte[8];

        int readBytes = await journal.ReadAsync(header.AsMemory(0, 8));
        if (readBytes != 8)
        {
            Console.WriteLine("Journal is empty");
            return;
        }

        journal.Seek(0, SeekOrigin.Begin);

        await ReadNextLog();
    }

    private async Task ReadNextLog()
    {
        byte[] header = new byte[8];

        int readBytes = await journal.ReadAsync(header.AsMemory(0, 8));

        if (readBytes == 0)        
            return;

        if (readBytes > 0)
        {
            Console.WriteLine("Journal is incomplete or corrupt");
            return;
        }

        int pointer = 0;

        uint sequence = Serializator.ReadUInt32(header, ref pointer);
        short type = Serializator.ReadInt16(header, ref pointer);

        switch (type)
        {
            case JournalLogTypes.InsertSlots:
                InsertTicketReader.Deserialize(sequence, journal);
                break;
        }

        Console.WriteLine(sequence);
        Console.WriteLine(type);
    }

    public void Dispose()
    {
        if (journal != null)        
            journal.Dispose();
    }
}
