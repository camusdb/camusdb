
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.Serializer;
using CamusDB.Core.BufferPool;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Serializer.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Controllers;

public sealed class RowInserter
{
    private readonly IndexSaver indexSaver = new();

    private int? GetPrimaryKeyValue(TableDescriptor table, InsertTicket ticket)
    {
        List<TableColumnSchema> columns = table.Schema!.Columns!;

        for (int i = 0; i < columns.Count; i++)
        {
            TableColumnSchema column = columns[i];

            if (column.Primary)
                return int.Parse(ticket.Values[column.Name].Value);
        }

        return null;
    }

    private int CalculateBufferLength(TableDescriptor table, InsertTicket ticket)
    {
        int length = 10; // 1 type + 4 schemaVersion + 1 type + 4 rowId

        for (int i = 0; i < table.Schema!.Columns!.Count; i++)
        {
            TableColumnSchema column = table.Schema!.Columns[i];

            if (!ticket.Values.TryGetValue(column.Name, out ColumnValue? columnValue))
            {
                length += 1; // null (1 byte)
                continue;
            }

            switch (columnValue.Type) // @todo check if value is compatible with column
            {
                case ColumnType.Id:
                    length += 5; // type 1 byte + 4 byte int
                    break;

                case ColumnType.Integer:
                    length += 5; // type 1 byte + 4 byte int
                    break;

                case ColumnType.String:
                    length += 5 + columnValue.Value.Length; // type 1 byte + 4 byte length + strLength
                    break;

                case ColumnType.Bool:
                    length++; // bool (1 byte)
                    break;

                default:
                    throw new CamusDBException(
                        CamusDBErrorCodes.UnknownType,
                        "Unknown type " + columnValue.Type
                    );
            }
        }

        return length;
    }

    private byte[] GetRowBuffer(TableDescriptor table, InsertTicket ticket, int rowId)
    {
        int length = CalculateBufferLength(table, ticket);

        byte[] rowBuffer = new byte[length];

        int pointer = 0;

        Serializator.WriteType(rowBuffer, SerializatorTypes.TypeInteger32, ref pointer);
        Serializator.WriteInt32(rowBuffer, table.Schema!.Version, ref pointer); // schema version

        Serializator.WriteType(rowBuffer, SerializatorTypes.TypeInteger32, ref pointer);
        Serializator.WriteInt32(rowBuffer, rowId, ref pointer); // row Id

        List<TableColumnSchema> columns = table.Schema!.Columns!;

        for (int i = 0; i < columns.Count; i++)
        {
            TableColumnSchema column = columns[i];

            if (!ticket.Values.TryGetValue(column.Name, out ColumnValue? columnValue))
            {
                Serializator.WriteType(rowBuffer, SerializatorTypes.TypeNull, ref pointer);
                continue;
            }

            int value;

            switch (columnValue.Type)
            {
                case ColumnType.Id: // @todo use int.TryParse
                    Serializator.WriteType(rowBuffer, SerializatorTypes.TypeInteger32, ref pointer);
                    Serializator.WriteInt32(rowBuffer, int.Parse(columnValue.Value), ref pointer);
                    break;

                case ColumnType.Integer: // @todo use int.TryParse
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

                default:
                    throw new CamusDBException(
                        CamusDBErrorCodes.UnknownType,
                        "Unknown type " + columnValue.Type
                    );
            }
        }

        return rowBuffer;
    }

    private int CheckPrimaryKeyViolations(TableDescriptor table, BTree pkIndex, InsertTicket ticket)
    {
        int? primaryKeyValue = GetPrimaryKeyValue(table, ticket);

        if (primaryKeyValue is null)
            throw new CamusDBException(
                CamusDBErrorCodes.DuplicatePrimaryKey,
                "Cannot retrieve primary key for table " + table.Name
            );

        int? pageOffset = pkIndex.Get(primaryKeyValue.Value);

        if (pageOffset is not null)
            throw new CamusDBException(
                CamusDBErrorCodes.DuplicatePrimaryKey,
                "Duplicate entry for key " + table.Name + " " + primaryKeyValue
            );

        return primaryKeyValue.Value;
    }

    public async Task Insert(DatabaseDescriptor database, TableDescriptor table, InsertTicket ticket)
    {
        int rowId = 0, dataPage = 0;

        var timer = new Stopwatch();
        timer.Start();

        BufferPoolHandler tablespace = database.TableSpace!;

        BTree pkIndex = table.Indexes["pk"];

        try
        {
            await pkIndex.WriteLock.WaitAsync();

            int primaryKeyValue = CheckPrimaryKeyViolations(table, pkIndex, ticket);

            // allocate pages and rowid 
            rowId = await tablespace.GetNextRowId();
            dataPage = await tablespace.GetNextFreeOffset();

            await indexSaver.NoLockingSave(tablespace, pkIndex, primaryKeyValue, dataPage);
        }
        finally
        {
            pkIndex.WriteLock.Release();
        }

        // Insert data to a free page and update indexes

        byte[] rowBuffer = GetRowBuffer(table, ticket, rowId);
        await tablespace.WriteDataToPage(dataPage, rowBuffer);

        await indexSaver.Save(tablespace, table.Rows, rowId, dataPage);

        // @todo update other indexes here

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
