
using System.Diagnostics;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.BufferPool;
using CamusDB.Core.Serializer;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.BufferPool.Models;
using CamusDB.Core.Serializer.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Controllers;

public sealed class RowInserter
{
    private readonly IndexSaver indexSaver = new();

    private byte[] GetRowBuffer(TableDescriptor table, InsertTicket ticket, int rowId, ref int primaryKeyValue)
    {
        int length = 10; // 1 type + 4 schemaVersion + 1 type + 4 rowId

        foreach (ColumnValue columnValue in ticket.Values)
        {
            switch (columnValue.Type)
            {
                case ColumnType.Id:
                    length += 5;
                    break;

                case ColumnType.Integer:
                    length += 5;
                    break;

                case ColumnType.String:
                    length += 1 + 4 + columnValue.Value.Length;
                    break;

                case ColumnType.Bool:
                    length++;
                    break;
            }
            //length += Get
        }

        byte[] rowBuffer = new byte[length];

        int pointer = 0;

        Serializator.WriteType(rowBuffer, SerializatorTypes.TypeInteger32, ref pointer);
        Serializator.WriteInt32(rowBuffer, table.Schema!.Version, ref pointer); // schema version

        Serializator.WriteType(rowBuffer, SerializatorTypes.TypeInteger32, ref pointer);
        Serializator.WriteInt32(rowBuffer, rowId, ref pointer); // row Id        

        foreach (ColumnValue columnValue in ticket.Values)
        {
            int value = 0;

            switch (columnValue.Type)
            {
                case ColumnType.Id:
                    value = int.Parse(columnValue.Value);
                    Serializator.WriteType(rowBuffer, SerializatorTypes.TypeInteger32, ref pointer);
                    Serializator.WriteInt32(rowBuffer, value, ref pointer);
                    primaryKeyValue = value;
                    break;

                case ColumnType.Integer:
                    value = int.Parse(columnValue.Value);
                    Serializator.WriteType(rowBuffer, SerializatorTypes.TypeInteger32, ref pointer);
                    Serializator.WriteInt32(rowBuffer, value, ref pointer);
                    break;

                case ColumnType.String:
                    Serializator.WriteType(rowBuffer, SerializatorTypes.TypeString32, ref pointer);
                    Serializator.WriteInt32(rowBuffer, columnValue.Value.Length, ref pointer);
                    Serializator.WriteString(rowBuffer, columnValue.Value, ref pointer);
                    break;

                case ColumnType.Bool:
                    Serializator.WriteBool(rowBuffer, columnValue.Value == "true", ref pointer);
                    break;
            }
        }

        return rowBuffer;
    }

    public async Task Insert(DatabaseDescriptor database, TableDescriptor table, InsertTicket ticket)
    {
        var timer = new Stopwatch();
        timer.Start();

        // check primary key violations

        int rowId = await database.TableSpace!.GetNextRowId();

        int primaryKeyValue = 0;

        byte[] rowBuffer = GetRowBuffer(table, ticket, rowId, ref primaryKeyValue);

        int? pageOffset = table.Indexes["pk"].Get(primaryKeyValue);

        if (pageOffset is not null)
            throw new CamusDBException(CamusDBErrorCodes.DuplicatePrimaryKey, "PK violation trying to insert key " + primaryKeyValue);

        // Insert data to a free page and update indexes

        int dataPage = await database.TableSpace!.WriteDataToFreePage(rowBuffer);

        await indexSaver.Save(database.TableSpace, table.Rows, rowId, dataPage);

        foreach (KeyValuePair<string, BTree> index in table.Indexes)
            await indexSaver.Save(database.TableSpace, index.Value, primaryKeyValue, dataPage);

        timer.Stop();

        TimeSpan timeTaken = timer.Elapsed;        

        /*foreach (KeyValuePair<string, BTree> index in table.Indexes)
        {
            foreach (BTreeEntry entry in index.Value.EntriesTraverse())            
                Console.WriteLine("Index Key={0} PageOffset={1}", entry.Key, entry.Value);
        }*/

        Console.WriteLine("Row {0} inserted at {1}, Time taken: {2}", rowId, dataPage, timeTaken.ToString(@"m\:ss\.fff"));
    }
}
