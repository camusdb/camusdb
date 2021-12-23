
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

internal sealed class IndexMultiReader : IndexBaseReader
{
    private readonly IndexReader indexReader;

    public IndexMultiReader(IndexReader indexReader)
    {
        this.indexReader = indexReader;
    }

    public async Task<BTreeMulti<ColumnValue>> ReadMulti(BufferPoolHandler tablespace, int offset)
    {
        //Console.WriteLine("***");

        BTreeMulti<ColumnValue> index = new(offset);

        byte[] data = await tablespace.GetDataFromPage(offset);
        if (data.Length == 0)
            return index;

        int pointer = 0;

        index.height = Serializator.ReadInt32(data, ref pointer);
        index.size = Serializator.ReadInt32(data, ref pointer);
        index.denseSize = Serializator.ReadInt32(data, ref pointer);

        int rootPageOffset = Serializator.ReadInt32(data, ref pointer);

        //Console.WriteLine("NumberNodes={0} PageOffset={1} RootOffset={2}", index.n, index.PageOffset, rootPageOffset);

        if (rootPageOffset > -1)
        {
            BTreeMultiNode<ColumnValue>? node = await GetMultiNode(tablespace, rootPageOffset);
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

    private async Task<BTreeMultiNode<ColumnValue>?> GetMultiNode(BufferPoolHandler tablespace, int offset)
    {
        byte[] data = await tablespace.GetDataFromPage(offset);
        if (data.Length == 0)
            return null;

        BTreeMultiNode<ColumnValue> node = new(-1);

        int pointer = 0;
        node.KeyCount = Serializator.ReadInt32(data, ref pointer);
        node.PageOffset = Serializator.ReadInt32(data, ref pointer);

        //Console.WriteLine("KeyCount={0} PageOffset={1}", node.KeyCount, node.PageOffset);

        for (int i = 0; i < node.KeyCount; i++)
        {
            //int type = Serializator.ReadType(data, ref pointer);
            //if (type == SerializatorTypes.TypeInteger32)

            //Serializator.ReadInt32(data, ref pointer);

            BTreeMultiEntry<ColumnValue> entry = new(UnserializeKey(data, ref pointer), null);

            //entry.Key =

            int subTreeOffset = Serializator.ReadInt32(data, ref pointer);
            if (subTreeOffset > 0)
                entry.Value = await indexReader.ReadOffsets(tablespace, subTreeOffset);

            int nextPageOffset = Serializator.ReadInt32(data, ref pointer);
            if (nextPageOffset > -1)
                entry.Next = await GetMultiNode(tablespace, nextPageOffset);

            //Console.WriteLine("Children={0} Key={1} Value={2} NextOffset={3}", i, entry.Key, entry.Value, nextPageOffset);

            node.children[i] = entry;
        }

        return node;
    }
}

