
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.BufferPool;
using CamusDB.Core.Serializer;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Controllers.Indexes;

internal sealed class IndexUniqueReader : IndexBaseReader
{
    private readonly IndexReader indexReader;

    public IndexUniqueReader(IndexReader indexReader)
    {
        this.indexReader = indexReader;
    }

    public async Task<BPTree<CompositeColumnValue, ColumnValue, BTreeTuple>> Read(BufferPoolManager bufferpool, ObjectIdValue offset)
    {
        IndexUniqueNodeReader reader = new(bufferpool);

        byte[] data = await bufferpool.GetDataFromPage(offset).ConfigureAwait(false);
        if (data.Length == 0)
            return new(offset, BTreeUtils.GetNodeCapacity<CompositeColumnValue, BTreeTuple>(), reader);

        int pointer = 0;

        int version = Serializator.ReadInt32(data, ref pointer);
        if (version != BTreeConfig.LayoutVersion)
            throw new CamusDBException(CamusDBErrorCodes.InvalidIndexLayout, "Unsupported b+tree version found");

        int maxCapacity = Serializator.ReadInt32(data, ref pointer);

        BPTree<CompositeColumnValue, ColumnValue, BTreeTuple> index = new(offset, maxCapacity, reader)
        {
            height = Serializator.ReadInt32(data, ref pointer),
            size = Serializator.ReadInt32(data, ref pointer)
        };

        ObjectIdValue rootPageOffset = Serializator.ReadObjectId(data, ref pointer);

        if (!rootPageOffset.IsNull())
        {
            BTreeNode<CompositeColumnValue, BTreeTuple>? node = await reader.GetNode(rootPageOffset, maxCapacity).ConfigureAwait(false);
            if (node is not null)
                index.root = node;
        }

        return index;
    }
}
