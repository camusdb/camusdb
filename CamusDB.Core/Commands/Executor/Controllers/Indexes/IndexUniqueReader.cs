
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

namespace CamusDB.Core.CommandsExecutor.Controllers.Indexes;

internal sealed class IndexUniqueReader : IndexBaseReader
{
    private readonly IndexReader indexReader;

    public IndexUniqueReader(IndexReader indexReader)
    {
        this.indexReader = indexReader;
    }

    public async Task<BTree<ColumnValue, BTreeTuple?>> ReadUnique(BufferPoolHandler tablespace, int offset)
    {
        //Console.WriteLine("***");

        BTree<ColumnValue, BTreeTuple?> index = new(offset);

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
            BTreeNode<ColumnValue, BTreeTuple?>? node = await GetUniqueNode(tablespace, rootPageOffset);
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

    private async Task<BTreeNode<ColumnValue, BTreeTuple?>?> GetUniqueNode(BufferPoolHandler tablespace, int offset)
    {
        byte[] data = await tablespace.GetDataFromPage(offset);
        if (data.Length == 0)
            return null;

        BTreeNode<ColumnValue, BTreeTuple?> node = new(-1);

        node.Dirty = false; // read nodes from disk must be not persisted

        int pointer = 0;
        node.KeyCount = Serializator.ReadInt32(data, ref pointer);
        node.PageOffset = Serializator.ReadInt32(data, ref pointer);

        //Console.WriteLine("KeyCount={0} PageOffset={1}", node.KeyCount, node.PageOffset);

        for (int i = 0; i < node.KeyCount; i++)
        {
            ColumnValue key = UnserializeKey(data, ref pointer);
            BTreeTuple? tuple = UnserializeTuple(data, ref pointer);

            BTreeEntry<ColumnValue, BTreeTuple?> entry = new(key, tuple, null);

            //entry.Key = Serializator.ReadInt32(data, ref pointer);
            //entry.Value =

            int nextPageOffset = Serializator.ReadInt32(data, ref pointer);
            //Console.WriteLine("Children={0} Key={1} Value={2} NextOffset={3}", i, entry.Key, entry.Value, nextPageOffset);

            if (nextPageOffset > -1)
                entry.Next = await GetUniqueNode(tablespace, nextPageOffset);

            node.children[i] = entry;
        }

        return node;
    }
}
