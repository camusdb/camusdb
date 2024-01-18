
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.BufferPool;
using CamusDB.Core.Serializer;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Util.Trees.Experimental;

namespace CamusDB.Core.CommandsExecutor.Controllers.Indexes;

internal sealed class IndexUniqueOffsetReader : IndexBaseReader
{
    private readonly IndexReader indexReader;

    public IndexUniqueOffsetReader(IndexReader indexReader)
    {
        this.indexReader = indexReader;
    }

    public async Task<BPlusTree<ObjectIdValue, ObjectIdValue>> ReadOffsets(BufferPoolManager bufferpool, ObjectIdValue offset)
    {
        //Console.WriteLine("***");

        IndexUniqueOffsetNodeReader reader = new(bufferpool);

        BPlusTree<ObjectIdValue, ObjectIdValue> index = new(offset, reader);

        byte[] data = await bufferpool.GetDataFromPage(offset).ConfigureAwait(false);
        if (data.Length == 0)
            return index;

        int pointer = 0;

        //index.height = Serializator.ReadInt32(data, ref pointer);
        //index.size = Serializator.ReadInt32(data, ref pointer);

        ObjectIdValue rootPageOffset = Serializator.ReadObjectId(data, ref pointer);

        //Console.WriteLine("NumberNodes={0} PageOffset={1} RootOffset={2}", index.n, index.PageOffset, rootPageOffset);

        if (!rootPageOffset.IsNull())
        {
            BPlusTreeNode<ObjectIdValue, ObjectIdValue>? node = await reader.GetNode(rootPageOffset).ConfigureAwait(false);
            if (node is not null)
            {
                index.root = new(default!, reader, node);
                //index.loaded++;
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

