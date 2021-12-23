
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.BufferPool;
using CamusDB.Core.Serializer;
using CamusDB.Core.Util.Trees;

namespace CamusDB.Core.CommandsExecutor.Controllers.Indexes;

internal sealed class IndexUniqueOffsetReader : IndexBaseReader
{
    private readonly IndexReader indexReader;

    public IndexUniqueOffsetReader(IndexReader indexReader)
    {
        this.indexReader = indexReader;
    }

    public async Task<BTree<int, int?>> ReadOffsets(BufferPoolHandler bufferpool, int offset)
    {
        //Console.WriteLine("***");

        IndexUniqueOffsetNodeReader reader = new(bufferpool);

        BTree<int, int?> index = new(offset, reader);

        byte[] data = await bufferpool.GetDataFromPage(offset);
        if (data.Length == 0)
            return index;

        int pointer = 0;

        index.height = Serializator.ReadInt32(data, ref pointer);
        index.size = Serializator.ReadInt32(data, ref pointer);

        int rootPageOffset = Serializator.ReadInt32(data, ref pointer);

        //Console.WriteLine("NumberNodes={0} PageOffset={1} RootOffset={2}", index.n, index.PageOffset, rootPageOffset);

        if (rootPageOffset > -1)
        {
            BTreeNode<int, int?>? node = await reader.GetNode(rootPageOffset);
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

        Console.WriteLine("Loaded index of size {0} {1}", index.size, index.loaded);

        return index;
    }
}

