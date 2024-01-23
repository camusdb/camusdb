
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.BufferPool;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Serializer;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Util.Time;
using CamusDB.Core.Util.Trees;

namespace CamusDB.Core.CommandsExecutor.Controllers.Indexes;

public sealed class IndexUniqueNodeReader : IBTreeNodeReader<CompositeColumnValue, BTreeTuple>
{
    private readonly BufferPoolManager bufferpool;

    public IndexUniqueNodeReader(BufferPoolManager bufferpool)
    {
        this.bufferpool = bufferpool;
    }

    private static HLCTimestamp UnserializeTimestamp(byte[] nodeBuffer, ref int pointer)
    {
        return Serializator.ReadHLCTimestamp(nodeBuffer, ref pointer);
    }

    private static ColumnValue UnserializeKey(byte[] nodeBuffer, ref int pointer)
    {
        ColumnType type = (ColumnType)Serializator.ReadInt16(nodeBuffer, ref pointer);

        switch (type)
        {
            case ColumnType.Id:
                {
                    ObjectIdValue value = Serializator.ReadObjectId(nodeBuffer, ref pointer);
                    return new ColumnValue(ColumnType.Id, value.ToString());
                }

            case ColumnType.Integer64:
                {
                    long value = Serializator.ReadInt64(nodeBuffer, ref pointer);
                    return new ColumnValue(ColumnType.Integer64, value);
                }

            case ColumnType.String:
                {
                    string value = Serializator.ReadString(nodeBuffer, ref pointer);
                    return new ColumnValue(ColumnType.String, value);
                }

            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Can't use this type as index: " + type);
        }
    }

    private static CompositeColumnValue UnserializeCompositeKey(byte[] nodeBuffer, ref int pointer)
    {
        int length = Serializator.ReadInt8(nodeBuffer, ref pointer);

        ColumnValue[] values = new ColumnValue[length];

        for (int i = 0; i < length; i++)
            values[i] = UnserializeKey(nodeBuffer, ref pointer);

        return new CompositeColumnValue(values);
    }

    private static BTreeTuple? UnserializeTuple(byte[] nodeBuffer, ref int pointer)
    {
        ObjectIdValue slotOne = Serializator.ReadObjectId(nodeBuffer, ref pointer);
        ObjectIdValue slotTwo = Serializator.ReadObjectId(nodeBuffer, ref pointer);

        if (slotOne.IsNull() && slotTwo.IsNull())
            return null;

        return new BTreeTuple(slotOne, slotTwo);
    }

    public async Task<BTreeNode<CompositeColumnValue, BTreeTuple>?> GetNode(ObjectIdValue offset, int maxNodeCapacity)
    {
        byte[] data = await bufferpool.GetDataFromPage(offset);
        if (data.Length == 0)
            return null;

        int pointer = 0;

        BTreeNode<CompositeColumnValue, BTreeTuple> node = new(-1, maxNodeCapacity)
        {
            KeyCount = Serializator.ReadInt32(data, ref pointer),
            PageOffset = Serializator.ReadObjectId(data, ref pointer)
        };

        //Console.WriteLine("Node Read KeyCount={0} PageOffset={1}", node.KeyCount, node.PageOffset);

        for (int i = 0; i < node.KeyCount; i++)
        {
            CompositeColumnValue key = UnserializeCompositeKey(data, ref pointer);

            HLCTimestamp timestamp = UnserializeTimestamp(data, ref pointer);
            BTreeTuple? tuple = UnserializeTuple(data, ref pointer);

            BTreeEntry<CompositeColumnValue, BTreeTuple> entry = new(key, this, null, maxNodeCapacity)
            {
                NextPageOffset = Serializator.ReadObjectId(data, ref pointer)
            };

            if (!timestamp.IsNull())
                entry.SetValue(timestamp, BTreeCommitState.Committed, tuple);

            node.children[i] = entry;
        }

        return node;
    }
}
