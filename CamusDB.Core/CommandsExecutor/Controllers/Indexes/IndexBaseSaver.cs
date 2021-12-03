
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.BufferPool;
using CamusDB.Core.Serializer;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Indexes;

public abstract class IndexBaseSaver
{
    protected static int GetKeySize(ColumnValue columnValue)
    {
        return columnValue.Type switch
        {
            ColumnType.Id or ColumnType.Integer => 6,
            ColumnType.String => 2 + 4 + columnValue.Value.Length,
            _ => throw new Exception("Can't use this type as index"),
        };
    }

    protected static int GetKeySizes(BTreeNode<ColumnValue> node)
    {
        int length = 0;

        for (int i = 0; i < node.KeyCount; i++)
        {
            BTreeEntry<ColumnValue> entry = node.children[i];

            if (entry is null)
                length += 10; // type (2 byte) + 4 byte + 4 byte
            else
                length += 8 + GetKeySize(entry.Key);
        }

        return length;
    }

    protected static int GetKeySizes(BTreeMultiNode<ColumnValue> node)
    {
        int length = 0;

        for (int i = 0; i < node.KeyCount; i++)
        {
            BTreeMultiEntry<ColumnValue> entry = node.children[i];

            if (entry is null)
                length += 10; // type (2 byte) + 4 byte + 4 byte
            else
                length += 8 + GetKeySize(entry.Key);
        }

        return length;
    }

    protected static void SerializeKey(byte[] nodeBuffer, ColumnValue columnValue, ref int pointer)
    {
        switch (columnValue.Type)
        {
            case ColumnType.Id:
                Serializator.WriteInt16(nodeBuffer, (int)ColumnType.Id, ref pointer);
                Serializator.WriteInt32(nodeBuffer, int.Parse(columnValue.Value), ref pointer);
                break;

            case ColumnType.Integer:
                Serializator.WriteInt16(nodeBuffer, (int)ColumnType.Integer, ref pointer);
                Serializator.WriteInt32(nodeBuffer, int.Parse(columnValue.Value), ref pointer);
                break;

            case ColumnType.String:
                Serializator.WriteInt16(nodeBuffer, (int)ColumnType.String, ref pointer);
                Serializator.WriteInt32(nodeBuffer, columnValue.Value.Length, ref pointer);
                Serializator.WriteString(nodeBuffer, columnValue.Value, ref pointer);
                break;

            default:
                throw new Exception("Can't use this type as index");
        }
    }
}

