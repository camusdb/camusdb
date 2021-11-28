
using System;
using CamusDB.Library.Catalogs;
using CamusDB.Library.Util.Trees;
using CamusDB.Library.BufferPool;
using CamusDB.Library.Serializer;
using System.Collections.Generic;
using System.IO.MemoryMappedFiles;
using CamusDB.Library.Catalogs.Models;
using CamusDB.Library.BufferPool.Models;
using CamusDB.Library.Serializer.Models;
using CamusDB.Library.CommandsExecutor.Models;
using CamusDB.Library.CommandsExecutor.Controllers;

namespace CamusDB.Library.CommandsExecutor.Controllers;

public sealed class RowInserter
{
    private IndexSaver indexSaver = new();

    public async Task<bool> Insert(DatabaseDescriptor database, TableDescriptor table, InsertTicket ticket)
    {
        int rowId = await database.TableSpace!.GetNextRowId();

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
            switch (columnValue.Type)
            {
                case ColumnType.Id:
                    Serializator.WriteType(rowBuffer, SerializatorTypes.TypeInteger32, ref pointer);
                    Serializator.WriteInt32(rowBuffer, int.Parse(columnValue.Value), ref pointer);
                    break;

                case ColumnType.Integer:
                    Serializator.WriteType(rowBuffer, SerializatorTypes.TypeInteger32, ref pointer);
                    Serializator.WriteInt32(rowBuffer, int.Parse(columnValue.Value), ref pointer);
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

        int dataPage = await database.TableSpace!.WriteDataToFreePage(rowBuffer);

        await indexSaver.Save(database.TableSpace, table.Rows, rowId, dataPage);

        /*await table.Rows.Put(rowId, dataPage);

        await table.Rows.Put(100, 203);
        await table.Rows.Put(101, 204);
        await table.Rows.Put(102, 205);
        await table.Rows.Put(103, 206);
        await table.Rows.Put(104, 207);
        await table.Rows.Put(105, 208);*/

        foreach (Entry entry in table.Rows.EntriesTraverse())
        {
            Console.WriteLine("Index RowId={0} PageOffset={1}", entry.Key, entry.Value);
        }
        
        return true;
    }    
}
