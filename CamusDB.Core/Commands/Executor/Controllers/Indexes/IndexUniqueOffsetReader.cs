
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

    public async Task<BTree<int, int?>> ReadOffsets(BufferPoolHandler tablespace, int offset)
    {
        //Console.WriteLine("***");

        BTree<int, int?> index = new(offset);

        byte[] data = await tablespace.GetDataFromPage(offset);
        if (data.Length == 0)
            return index;

        int pointer = 0;

        index.height = Serializator.ReadInt32(data, ref pointer);
        index.size = Serializator.ReadInt32(data, ref pointer);

        int rootPageOffset = Serializator.ReadInt32(data, ref pointer);

        //Console.WriteLine("NumberNodes={0} PageOffset={1} RootOffset={2}", index.n, index.PageOffset, rootPageOffset);

        if (rootPageOffset > -1)
        {
            BTreeNode<int, int?>? node = await GetUniqueOffsetNode(tablespace, rootPageOffset);
            if (node is not null)
                index.root = node;
        }

        /*foreach (Entry entry in index.EntriesTraverse())
        {
            Console.WriteLine("Index RowId={0} PageOffset={1}", entry.Key, entry.Value);
        }*/

        //Console.WriteLine("***");

        return index;
    }

    private async Task<BTreeNode<int, int?>?> GetUniqueOffsetNode(BufferPoolHandler tablespace, int offset)
    {
        byte[] data = await tablespace.GetDataFromPage(offset);
        if (data.Length == 0)
            return null;

        BTreeNode<int, int?> node = new(-1);

        node.Dirty = false; // read nodes from disk must be not persisted

        int pointer = 0;
        node.KeyCount = Serializator.ReadInt32(data, ref pointer);
        node.PageOffset = Serializator.ReadInt32(data, ref pointer);

        //Console.WriteLine("KeyCount={0} PageOffset={1}", node.KeyCount, node.PageOffset);

        for (int i = 0; i < node.KeyCount; i++)
        {
            BTreeEntry<int, int?> entry = new(0, null, null);

            entry.Key = Serializator.ReadInt32(data, ref pointer);
            entry.Value = Serializator.ReadInt32(data, ref pointer);

            int nextPageOffset = Serializator.ReadInt32(data, ref pointer);
            //Console.WriteLine("Children={0} Key={1} Value={2} NextOffset={3}", i, entry.Key, entry.Value, nextPageOffset);

            if (nextPageOffset > -1)
                entry.Next = await GetUniqueOffsetNode(tablespace, nextPageOffset);

            node.children[i] = entry;
        }

        return node;
    }
}

