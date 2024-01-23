
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.BufferPool;
using CamusDB.Core.Serializer;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Util.Trees;

namespace CamusDB.Core.CommandsExecutor.Controllers.Indexes;

internal sealed class IndexUniqueOffsetReader : IndexBaseReader
{
    private readonly IndexReader indexReader;

    public IndexUniqueOffsetReader(IndexReader indexReader)
    {
        this.indexReader = indexReader;
    }

    public async Task<BTree<ObjectIdValue, ObjectIdValue>> ReadOffsets(BufferPoolManager bufferpool, ObjectIdValue offset)
    {
        //Console.WriteLine("***");

        IndexUniqueOffsetNodeReader reader = new(bufferpool);

        byte[] data = await bufferpool.GetDataFromPage(offset);
        if (data.Length == 0)
            return new(offset, BTreeUtils.GetNodeCapacity<ObjectIdValue, ObjectIdValue>(), reader);

        int pointer = 0;

        int version = Serializator.ReadInt32(data, ref pointer);
        if (version != BTreeConfig.LayoutVersion)
            throw new CamusDBException(CamusDBErrorCodes.InvalidIndexLayout, "Unsupported b+tree version found");

        int maxCapacity = Serializator.ReadInt32(data, ref pointer);

        BTree<ObjectIdValue, ObjectIdValue> index = new(offset, maxCapacity, reader)
        {
            height = Serializator.ReadInt32(data, ref pointer),
            size = Serializator.ReadInt32(data, ref pointer)
        };

        ObjectIdValue rootPageOffset = Serializator.ReadObjectId(data, ref pointer);

        //Console.WriteLine("NumberNodes={0} PageOffset={1} RootOffset={2}", index.n, index.PageOffset, rootPageOffset);

        if (!rootPageOffset.IsNull())
        {
            BTreeNode<ObjectIdValue, ObjectIdValue>? node = await reader.GetNode(rootPageOffset, index.maxNodeCapacity);
            if (node is not null)
            {
                index.root = node;
                index.loaded++;
            }
        }

        return index;
    }
}

