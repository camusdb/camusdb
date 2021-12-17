
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Serializer;
using CamusDB.Core.Journal.Models;
using CamusDB.Core.Serializer.Models;
using CamusDB.Core.Journal.Models.Logs;

namespace CamusDB.Core.Journal.Controllers;

public sealed class JournalReader : IDisposable
{
    private readonly FileStream journal;

    public JournalReader(string path)
    {
        this.journal = new FileStream(path, FileMode.OpenOrCreate, FileAccess.Read);
    }

    public async IAsyncEnumerable<JournalLog> ReadNextLog()
    {
        journal.Seek(0, SeekOrigin.Begin);

        byte[] header = new byte[
            SerializatorTypeSizes.TypeInteger32 +
            SerializatorTypeSizes.TypeInteger32
        ];

        int readBytes = await journal.ReadAsync(header.AsMemory(0, header.Length));
        if (readBytes != header.Length)
        {
            Console.WriteLine("Journal is empty");
            yield break;
        }

        journal.Seek(0, SeekOrigin.Begin);

        while (true)
        {
            header = new byte[
                SerializatorTypeSizes.TypeInteger32 +
                SerializatorTypeSizes.TypeInteger16
            ];

            readBytes = await journal.ReadAsync(header, 0, header.Length);

            if (readBytes == 0)
                yield break;

            if (readBytes < header.Length)
            {
                Console.WriteLine("Journal is incomplete or corrupt");
                yield break;
            }

            int pointer = 0;

            uint sequence = Serializator.ReadUInt32(header, ref pointer);
            JournalLogTypes type = (JournalLogTypes)Serializator.ReadInt16(header, ref pointer);

            switch (type)
            {
                case JournalLogTypes.Insert:
                    yield return new JournalLog(
                        sequence,
                        JournalLogTypes.Insert,
                        await InsertLogSerializator.Deserialize(journal)
                    );
                    break;

                case JournalLogTypes.InsertSlots:
                    yield return new JournalLog(
                        sequence,
                        JournalLogTypes.InsertSlots,
                        await InsertSlotsLogSerializator.Deserialize(journal)
                    );
                    break;

                case JournalLogTypes.WritePage:
                    yield return new JournalLog(
                        sequence,
                        JournalLogTypes.WritePage,
                        await WritePageLogSerializator.Deserialize(journal)
                    );
                    break;

                case JournalLogTypes.InsertCheckpoint:
                    yield return new JournalLog(
                        sequence,
                        JournalLogTypes.InsertCheckpoint,
                        await InsertCheckpointLogSerializator.Deserialize(journal)
                    );
                    break;

                case JournalLogTypes.UpdateUniqueIndexCheckpoint:
                    yield return new JournalLog(
                        sequence,
                        JournalLogTypes.UpdateUniqueIndexCheckpoint,
                        await UpdateUniqueCheckpointLogSerializator.Deserialize(journal)
                    );
                    break;

                case JournalLogTypes.UpdateUniqueIndex:
                    yield return new JournalLog(
                        sequence,
                        JournalLogTypes.UpdateUniqueIndex,
                        await UpdateUniqueIndexLogSerializator.Deserialize(journal)
                    );
                    break;

                case JournalLogTypes.FlushedPages:
                    yield return new JournalLog(
                        sequence,
                        JournalLogTypes.FlushedPages,
                        await FlushedPagesLogSerializator.Deserialize(journal)
                    );
                    break;

                default:
                    throw new Exception("Unsupported type: " + type);
            }
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
