
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

        BTree<ObjectIdValue, ObjectIdValue> index = new(offset, reader);

        byte[] data = await bufferpool.GetDataFromPage(offset);
        if (data.Length == 0)
            return index;

        int pointer = 0;

        index.height = Serializator.ReadInt32(data, ref pointer);
        index.size = Serializator.ReadInt32(data, ref pointer);

        ObjectIdValue rootPageOffset = Serializator.ReadObjectId(data, ref pointer);

        //Console.WriteLine("NumberNodes={0} PageOffset={1} RootOffset={2}", index.n, index.PageOffset, rootPageOffset);

        if (!rootPageOffset.IsNull())
        {
            BTreeNode<ObjectIdValue, ObjectIdValue>? node = await reader.GetNode(rootPageOffset);
            if (node is not null)
            {
                index.root = node;
                index.loaded++;
            }
        }

        /*foreach (Entry entry in index.EntriesTraverse())
        {
            Console.WriteLine("Index RowId={0} PageOffset={1}", entry.Key, entry.Value);
        }*/

        //Console.WriteLine("***");

        //Console.WriteLine("Loaded index of size {0} {1}", index.size, index.loaded);

        return index;
    }
}

