
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
using CamusDB.Core.Journal.Models.Logs;
using Config = CamusDB.Core.CamusDBConfig;
using CamusDB.Core.CommandsExecutor.Models;
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

        await foreach (JournalLog journalLog in ReadNextLog())
        {
            Console.WriteLine(journalLog.Type);
        }
    }

    public async IAsyncEnumerable<JournalLog> ReadNextLog()
    {
        //journal.Seek(0, SeekOrigin.Begin);

        byte[] header = new byte[6];

        int readBytes = await journal.ReadAsync(header, 0, 6);

        if (readBytes == 0)        
            yield break;        

        if (readBytes < 6)
        {
            Console.WriteLine("Journal is incomplete or corrupt");
            yield break;
        }

        int pointer = 0;

        uint sequence = Serializator.ReadUInt32(header, ref pointer);
        short type = Serializator.ReadInt16(header, ref pointer);

        /*Console.WriteLine(pointer);
        Console.WriteLine(sequence);
        Console.WriteLine(type);
        Console.WriteLine(journal.Position);*/

        switch (type)
        {
            case (short)JournalLogTypes.Insert:
                yield return new JournalLog(
                    sequence,
                    JournalLogTypes.Insert,
                    await InsertLogSerializator.Deserialize(journal)
                );
                break;

            case (short)JournalLogTypes.InsertSlots:
                yield return new JournalLog(
                    sequence,
                    JournalLogTypes.InsertSlots,
                    await InsertSlotsLogSerializator.Deserialize(journal)
                );
                break;

            default:
                throw new Exception("Unsupported type" + type);
                break;
        }        
    }

    public void Dispose()
    {
        if (journal != null)
        {
            journal.Close();
            journal.Dispose();
        }
    }
}
