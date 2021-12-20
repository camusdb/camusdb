
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Serializer;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Serializer.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Controllers.Indexes;

internal abstract class IndexBaseSaver
{
    protected static int GetKeySize(ColumnValue columnValue)
    {
        return columnValue.Type switch
        {
            ColumnType.Id => SerializatorTypeSizes.TypeInteger16 + SerializatorTypeSizes.TypeInteger32 * 3,
            ColumnType.Integer => SerializatorTypeSizes.TypeInteger16 + SerializatorTypeSizes.TypeInteger32,
            ColumnType.String => SerializatorTypeSizes.TypeInteger16 + SerializatorTypeSizes.TypeInteger32 + columnValue.Value.Length,
            _ => throw new Exception("Can't use this type as index"),
        };
    }

    protected static int GetKeySizes(BTreeNode<ColumnValue, BTreeTuple?> node)
    {
        int length = 0;

        for (int i = 0; i < node.KeyCount; i++)
        {
            BTreeEntry<ColumnValue, BTreeTuple?> entry = node.children[i];

            if (entry is null)
                length += 14; // type(2 byte) + tuple(4 byte + 4 byte) + nextPage(4 byte)
            else
                length += 12 + GetKeySize(entry.Key);
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
                length += 12 + GetKeySize(entry.Key);
        }

        return length;
    }

    protected static void SerializeKey(byte[] nodeBuffer, ColumnValue columnValue, ref int pointer)
    {
        switch (columnValue.Type)
        {
            case ColumnType.Id:
                Serializator.WriteInt16(nodeBuffer, (int)ColumnType.Id, ref pointer);
                Serializator.WriteObjectId(nodeBuffer, ObjectId.ToValue(columnValue.Value), ref pointer);
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

    protected static void SerializeTuple(byte[] nodeBuffer, BTreeTuple? rowTuple, ref int pointer)
    {
        if (rowTuple is not null)
        {
            Serializator.WriteInt32(nodeBuffer, rowTuple.SlotOne, ref pointer);
            Serializator.WriteInt32(nodeBuffer, rowTuple.SlotTwo, ref pointer);
        }
        else
        {
            Serializator.WriteInt32(nodeBuffer, 0, ref pointer);
            Serializator.WriteInt32(nodeBuffer, 0, ref pointer);
        }
    }
}

