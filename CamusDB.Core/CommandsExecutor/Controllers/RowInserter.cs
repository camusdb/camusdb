
using System.Diagnostics;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.Serializer;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Serializer.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.BufferPool;

namespace CamusDB.Core.CommandsExecutor.Controllers;

public sealed class RowInserter
{
    private readonly IndexSaver indexSaver = new();

    private int GetPrimaryKey(TableDescriptor table, InsertTicket ticket)
    {
        return int.Parse(ticket.Values[0].Value);
    }

    private byte[] GetRowBuffer(TableDescriptor table, InsertTicket ticket, int rowId)
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
            int value;

            switch (columnValue.Type)
            {
                case ColumnType.Id:
                    value = int.Parse(columnValue.Value);
                    Serializator.WriteType(rowBuffer, SerializatorTypes.TypeInteger32, ref pointer);
                    Serializator.WriteInt32(rowBuffer, value, ref pointer);
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

    private int CheckPrimaryKeyViolations(TableDescriptor table, BTree pkIndex, InsertTicket ticket)
    {
        int primaryKeyValue = GetPrimaryKey(table, ticket);

        int? pageOffset = pkIndex.Get(primaryKeyValue);

        if (pageOffset is not null)
            throw new CamusDBException(CamusDBErrorCodes.DuplicatePrimaryKey, "PK violation trying to insert key " + primaryKeyValue);

        return primaryKeyValue;
    }

    public async Task Insert(DatabaseDescriptor database, TableDescriptor table, InsertTicket ticket)
    {
        int rowId = 0, dataPage = 0, primaryKeyValue = 0;

        var timer = new Stopwatch();
        timer.Start();

        BufferPoolHandler tablespace = database.TableSpace!;

        BTree pkIndex = table.Indexes["pk"];

        try
        {
            await pkIndex.WriteLock.WaitAsync();

            primaryKeyValue = CheckPrimaryKeyViolations(table, pkIndex, ticket);

            // create buffer for new record
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
