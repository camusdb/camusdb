
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
using System.Text;

namespace CamusDB.Core.CommandsExecutor.Controllers.Indexes;

internal abstract class IndexBaseSaver
{
    private static int GetStringLengthInBytes(string str)
    {
        byte[] bytes = Encoding.Unicode.GetBytes(str);
        return bytes.Length;
    }

    protected static int GetKeySize(ColumnValue columnValue)
    {
        return columnValue.Type switch
        {
            ColumnType.Id => SerializatorTypeSizes.TypeInteger16 + SerializatorTypeSizes.TypeObjectId,
            ColumnType.Integer64 => SerializatorTypeSizes.TypeInteger16 + SerializatorTypeSizes.TypeInteger64,
            ColumnType.String => SerializatorTypeSizes.TypeInteger16 + SerializatorTypeSizes.TypeInteger32 + GetStringLengthInBytes(columnValue.Value),
            _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Can't use this type as index"),
        };
    }

    protected static int GetKeySizes(BTreeNode<ColumnValue, BTreeTuple?> node)
    {
        int length = 0;

        for (int i = 0; i < node.KeyCount; i++)
        {
            BTreeEntry<ColumnValue, BTreeTuple?> entry = node.children[i];

            if (entry is null)
                length += 2 + 12 * 4; // type(2 byte) + tuple(12 byte + 12 byte) + nextPage(12 byte)
            else
                length += 12 * 4 + GetKeySize(entry.Key);
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
                length += 2 + 36 + 12; // type (2 byte) + 12 byte + 12 byte
            else
                length += 36 + GetKeySize(entry.Key);
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

            case ColumnType.Integer64:
                Serializator.WriteInt16(nodeBuffer, (int)ColumnType.Integer64, ref pointer);
                Serializator.WriteInt64(nodeBuffer, long.Parse(columnValue.Value), ref pointer);
                break;

            case ColumnType.String:
                Serializator.WriteInt16(nodeBuffer, (int)ColumnType.String, ref pointer);
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
            Serializator.WriteObjectId(nodeBuffer, rowTuple.SlotOne, ref pointer);
            Serializator.WriteObjectId(nodeBuffer, rowTuple.SlotTwo, ref pointer);
        }
        else
        {
            Serializator.WriteObjectId(nodeBuffer, new(), ref pointer);
            Serializator.WriteObjectId(nodeBuffer, new(), ref pointer);
        }
    }
}

