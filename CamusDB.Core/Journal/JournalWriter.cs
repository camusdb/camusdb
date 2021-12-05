
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Journal.Models;
using CamusDB.Core.Serializer;
using Config = CamusDB.Core.CamusDBConfig;

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
        Console.WriteLine(Config.DataDirectory + "/" + database.Name + "/journal");
        fileStream = new(Config.DataDirectory + "/" + database.Name + "/journal", FileMode.Append, FileAccess.Write);
        await Task.Yield();
    }

    public uint GetNextSequence()
    {
        return Interlocked.Increment(ref logSequenceNumber);
    }

    private int GetLogLength(InsertTicket insertTicket)
    {
        int length = insertTicket.TableName.Length;

        foreach (KeyValuePair<string, ColumnValue> columnValue in insertTicket.Values)
        {
            length += columnValue.Key.Length;

            switch (columnValue.Value.Type)
            {
                case ColumnType.Id:
                case ColumnType.Integer:
                    length += 5;
                    break;

                case ColumnType.String:
                    length += 1 + columnValue.Value.Value.Length;
                    break;

                case ColumnType.Bool:
                    length += 1;
                    break;
            }
        }

        return length;
    }    

    public async Task Append(JournalInsertSchedule insertSchedule)
    {
        if (fileStream is null)
            throw new Exception("Journal has not been initialized");

        uint sequence = GetNextSequence();

        InsertTicket insertTicket = insertSchedule.InsertTicket;

        int length = GetLogLength(insertTicket);
        Console.WriteLine(length);

        byte[] journal = new byte[4 + 2 + 4 + 2 + length]; // LSN (4 bytes) + journal type (2 bytes) + number fields (2 bytes) +
                                                           // length(4 bytes) + payload

        //var b = Encoding.UTF8.GetBytes("hello");

        int pointer = 0;
        Serializator.WriteUInt32(journal, sequence, ref pointer);
        Serializator.WriteInt16(journal, JournalScheduleTypes.Insert, ref pointer);
        Serializator.WriteInt32(journal, length, ref pointer);
        Serializator.WriteInt16(journal, insertTicket.Values.Count, ref pointer);

        foreach (KeyValuePair<string, ColumnValue> columnValue in insertTicket.Values)
        {
            Serializator.WriteString(journal, columnValue.Key, ref pointer);

            switch (columnValue.Value.Type)
            {
                case ColumnType.Id:
                case ColumnType.Integer:
                    length += 5;
                    break;

                case ColumnType.String:
                    length += 1 + columnValue.Value.Value.Length;
                    Serializator.WriteString(journal, columnValue.Value.Value, ref pointer);
                    break;

                case ColumnType.Bool:
                    length += 1;
                    break;
            }
        }

        await fileStream.WriteAsync(journal);
        await fileStream.FlushAsync();
    }
}
