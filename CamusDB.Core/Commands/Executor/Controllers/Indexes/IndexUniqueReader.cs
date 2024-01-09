
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

    public async Task<BTree<ColumnValue, BTreeTuple>> ReadUnique(BufferPoolManager bufferpool, ObjectIdValue offset)
    {
        //Console.WriteLine("***");

        IndexUniqueNodeReader reader = new(bufferpool);

        BTree<ColumnValue, BTreeTuple> index = new(offset, reader);

        byte[] data = await bufferpool.GetDataFromPage(offset);
        if (data.Length == 0)
            return index;

        int pointer = 0;

        index.height = Serializator.ReadInt32(data, ref pointer);
        index.size = Serializator.ReadInt32(data, ref pointer);

        ObjectIdValue rootPageOffset = Serializator.ReadObjectId(data, ref pointer);

        if (!rootPageOffset.IsNull())
        {
            BTreeNode<ColumnValue, BTreeTuple>? node = await reader.GetNode(rootPageOffset);
            if (node is not null)
                index.root = node;
        }

        return index;
    }
}
