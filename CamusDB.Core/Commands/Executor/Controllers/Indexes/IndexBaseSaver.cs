
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
        return Encoding.Unicode.GetByteCount(str);
    }

    protected static int GetKeySize(ColumnValue columnValue)
    {
        return columnValue.Type switch
        {
            ColumnType.Id => SerializatorTypeSizes.TypeInteger16 + SerializatorTypeSizes.TypeObjectId,
            ColumnType.Integer64 => SerializatorTypeSizes.TypeInteger16 + SerializatorTypeSizes.TypeInteger64,
            ColumnType.String => SerializatorTypeSizes.TypeInteger16 + SerializatorTypeSizes.TypeInteger32 + GetStringLengthInBytes(columnValue.StrValue!),
            _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Can't use this type as index: " + columnValue.Type),
        };
    }

    protected static int GetKeySize(CompositeColumnValue columnValue)
    {
        int size = SerializatorTypeSizes.TypeInteger8;

        for (int i = 0; i < columnValue.Values.Length; i++)
            size += GetKeySize(columnValue.Values[i]);

        return size;
    }

    protected static int GetKeySizes(BTreeNode<ColumnValue, BTreeTuple> node)
    {
        int length = 0;

        for (int i = 0; i < node.KeyCount; i++)
        {
            BTreeEntry<ColumnValue, BTreeTuple> entry = node.children[i];

            if (entry is null)
                length += 2 + 12 * 4; // type(2 byte) + tuple(12 byte + 12 byte) + nextPage(12 byte)
            else
                length += 12 * 4 + GetKeySize(entry.Key);
        }

        return length;
    }

    protected static int GetEntrySizes(BTreeNode<CompositeColumnValue, BTreeTuple> node)
    {
        int length = 0;

        for (int i = 0; i < node.KeyCount; i++)
        {
            BTreeEntry<CompositeColumnValue, BTreeTuple> entry = node.children[i];

            if (entry is null)
                length += (
                    SerializatorTypeSizes.TypeInteger8 +     // null key (1 byte) +
                    SerializatorTypeSizes.TypeHLCTimestamp + // HLCTimestamp(12 bytes) +
                    SerializatorTypeSizes.TypeTuple +        // tupleSize(2 byte) +
                    SerializatorTypeSizes.TypeObjectId       // nextPageSize(2 byte) +
                );
            else
                length += (
                    GetKeySize(entry.Key) +                  // dynamic +                    
                    SerializatorTypeSizes.TypeHLCTimestamp + // HLCTimestamp(12 bytes) +
                    SerializatorTypeSizes.TypeTuple +        // tupleSize(2 byte) +
                    SerializatorTypeSizes.TypeObjectId       // nextPageSize(2 byte) +
                );
        }

        return length;        
    }

    protected static void SerializeKey(byte[] nodeBuffer, CompositeColumnValue columnValue, ref int pointer)
    {
        Serializator.WriteInt8(nodeBuffer, columnValue.Values.Length, ref pointer);

        for (int i = 0; i < columnValue.Values.Length; i++)
            SerializeKey(nodeBuffer, columnValue.Values[i], ref pointer);
    }

    protected static void SerializeKey(byte[] nodeBuffer, ColumnValue columnValue, ref int pointer)
    {
        switch (columnValue.Type)
        {
            case ColumnType.Id:
                Serializator.WriteInt16(nodeBuffer, (int)ColumnType.Id, ref pointer);
                Serializator.WriteObjectId(nodeBuffer, ObjectId.ToValue(columnValue.StrValue!), ref pointer);
                break;

            case ColumnType.Integer64:
                Serializator.WriteInt16(nodeBuffer, (int)ColumnType.Integer64, ref pointer);
                Serializator.WriteInt64(nodeBuffer, columnValue.LongValue, ref pointer);
                break;

            case ColumnType.String:
                Serializator.WriteInt16(nodeBuffer, (int)ColumnType.String, ref pointer);
                Serializator.WriteString(nodeBuffer, columnValue.StrValue!, ref pointer);
                break;

            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Can't use this type as index: " + columnValue.Type);
        }
    }   
}
