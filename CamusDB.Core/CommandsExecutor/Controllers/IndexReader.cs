
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.BufferPool;
using CamusDB.Core.Serializer;
using CamusDB.Core.Util.Trees;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class IndexReader
{
    public async Task<BTree> ReadUnique(BufferPoolHandler tablespace, int offset)
    {
        //Console.WriteLine("***");

        BTree index = new(offset);

        byte[] data = await tablespace.GetDataFromPage(offset);
        if (data.Length == 0)
            return index;
        
        int pointer = 0;

        index.height = Serializator.ReadInt32(data, ref pointer);
        index.n = Serializator.ReadInt32(data, ref pointer);

        int rootPageOffset = Serializator.ReadInt32(data, ref pointer);

        //Console.WriteLine("NumberNodes={0} PageOffset={1} RootOffset={2}", index.n, index.PageOffset, rootPageOffset);

        if (rootPageOffset > -1)
        {
            BTreeNode? node = await GetUniqueNode(tablespace, rootPageOffset);
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

    public async Task<BTreeMulti> ReadMulti(BufferPoolHandler tablespace, int offset)
    {
        //Console.WriteLine("***");

        BTreeMulti index = new(offset);

        byte[] data = await tablespace.GetDataFromPage(offset);
        if (data.Length == 0)
            return index;

        int pointer = 0;

        index.height = Serializator.ReadInt32(data, ref pointer);
        index.n = Serializator.ReadInt32(data, ref pointer);

        int rootPageOffset = Serializator.ReadInt32(data, ref pointer);

        //Console.WriteLine("NumberNodes={0} PageOffset={1} RootOffset={2}", index.n, index.PageOffset, rootPageOffset);

        if (rootPageOffset > -1)
        {
            BTreeMultiNode? node = await GetMultiNode(tablespace, rootPageOffset);
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

    private async Task<BTreeNode?> GetUniqueNode(BufferPoolHandler tablespace, int offset)
    {
        byte[] data = await tablespace.GetDataFromPage(offset);
        if (data.Length == 0)
            return null;

        BTreeNode node = new(-1);

        node.Dirty = false; // read nodes from disk must be not persisted

        int pointer = 0;
        node.KeyCount = Serializator.ReadInt32(data, ref pointer);
        node.PageOffset = Serializator.ReadInt32(data, ref pointer);

        //Console.WriteLine("KeyCount={0} PageOffset={1}", node.KeyCount, node.PageOffset);

        for (int i = 0; i < node.KeyCount; i++)
        {
            BTreeEntry entry = new(0, null, null);

            entry.Key = Serializator.ReadInt32(data, ref pointer);
            entry.Value = Serializator.ReadInt32(data, ref pointer);

            int nextPageOffset = Serializator.ReadInt32(data, ref pointer);
            //Console.WriteLine("Children={0} Key={1} Value={2} NextOffset={3}", i, entry.Key, entry.Value, nextPageOffset);

            if (nextPageOffset > -1)
                entry.Next = await GetUniqueNode(tablespace, nextPageOffset);
                    
            node.children[i] = entry;
        }

        return node;
    }

    private async Task<BTreeMultiNode?> GetMultiNode(BufferPoolHandler tablespace, int offset)
    {
        byte[] data = await tablespace.GetDataFromPage(offset);
        if (data.Length == 0)
            return null;

        BTreeMultiNode node = new(-1);

        node.Dirty = false; // read nodes from disk must be not persisted

        int pointer = 0;
        node.KeyCount = Serializator.ReadInt32(data, ref pointer);
        node.PageOffset = Serializator.ReadInt32(data, ref pointer);

        //Console.WriteLine("KeyCount={0} PageOffset={1}", node.KeyCount, node.PageOffset);

        for (int i = 0; i < node.KeyCount; i++)
        {
            BTreeMultiEntry entry = new(0, null);

            entry.Key = Serializator.ReadInt32(data, ref pointer);

            int subTreeOffset = Serializator.ReadInt32(data, ref pointer);
            if (subTreeOffset > 0)
                entry.Value = await ReadUnique(tablespace, subTreeOffset);

            int nextPageOffset = Serializator.ReadInt32(data, ref pointer);            
            if (nextPageOffset > -1)
                entry.Next = await GetMultiNode(tablespace, nextPageOffset);

            //Console.WriteLine("Children={0} Key={1} Value={2} NextOffset={3}", i, entry.Key, entry.Value, nextPageOffset);

            node.children[i] = entry;
        }

        return node;
    }
}
