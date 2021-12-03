
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.BufferPool;
using CamusDB.Core.Serializer;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.Serializer.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class IndexReader
{
    public async Task<BTree<int>> ReadOffsets(BufferPoolHandler tablespace, int offset)
    {
        //Console.WriteLine("***");

        BTree<int> index = new(offset);

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
            BTreeNode<int>? node = await GetUniqueOffsetNode(tablespace, rootPageOffset);
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

    public async Task<BTree<ColumnValue>> ReadUnique(BufferPoolHandler tablespace, int offset)
    {
        //Console.WriteLine("***");

        BTree<ColumnValue> index = new(offset);

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
            BTreeNode<ColumnValue>? node = await GetUniqueNode(tablespace, rootPageOffset);
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

    public async Task<BTreeMulti<ColumnValue>> ReadMulti(BufferPoolHandler tablespace, int offset)
    {
        //Console.WriteLine("***");

        BTreeMulti<ColumnValue> index = new(offset);

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

    private async Task<BTreeNode<ColumnValue>?> GetUniqueNode(BufferPoolHandler tablespace, int offset)
    {
        byte[] data = await tablespace.GetDataFromPage(offset);
        if (data.Length == 0)
            return null;

        BTreeNode<ColumnValue> node = new(-1);

        node.Dirty = false; // read nodes from disk must be not persisted

        int pointer = 0;
        node.KeyCount = Serializator.ReadInt32(data, ref pointer);
        node.PageOffset = Serializator.ReadInt32(data, ref pointer);

        //Console.WriteLine("KeyCount={0} PageOffset={1}", node.KeyCount, node.PageOffset);

        for (int i = 0; i < node.KeyCount; i++)
        {
            ColumnValue key = UnserializeKey(data, ref pointer);

            BTreeEntry<ColumnValue> entry = new(key, null, null);

            //entry.Key = Serializator.ReadInt32(data, ref pointer);
            entry.Value = Serializator.ReadInt32(data, ref pointer);

            int nextPageOffset = Serializator.ReadInt32(data, ref pointer);
            //Console.WriteLine("Children={0} Key={1} Value={2} NextOffset={3}", i, entry.Key, entry.Value, nextPageOffset);

            if (nextPageOffset > -1)
                entry.Next = await GetUniqueNode(tablespace, nextPageOffset);

            node.children[i] = entry;
        }

        return node;
    }

    private async Task<BTreeNode<int>?> GetUniqueOffsetNode(BufferPoolHandler tablespace, int offset)
    {
        byte[] data = await tablespace.GetDataFromPage(offset);
        if (data.Length == 0)
            return null;

        BTreeNode<int> node = new(-1);

        node.Dirty = false; // read nodes from disk must be not persisted

        int pointer = 0;
        node.KeyCount = Serializator.ReadInt32(data, ref pointer);
        node.PageOffset = Serializator.ReadInt32(data, ref pointer);

        //Console.WriteLine("KeyCount={0} PageOffset={1}", node.KeyCount, node.PageOffset);

        for (int i = 0; i < node.KeyCount; i++)
        {
            BTreeEntry<int> entry = new(0, null, null);

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

    private static ColumnValue UnserializeKey(byte[] nodeBuffer, ref int pointer)
    {
        int type = Serializator.ReadInt16(nodeBuffer, ref pointer);

        switch (type)
        {
            case (int)ColumnType.Id:
                {
                    int value = Serializator.ReadInt32(nodeBuffer, ref pointer);
                    return new ColumnValue(ColumnType.Id, value.ToString());
                }

            case (int)ColumnType.Integer:
                {
                    int value = Serializator.ReadInt32(nodeBuffer, ref pointer);
                    return new ColumnValue(ColumnType.Integer, value.ToString());
                }
            
            /*case ColumnType.String:
                Serializator.WriteInt16(nodeBuffer, (int)ColumnType.String, ref pointer);
                Serializator.WriteInt32(nodeBuffer, columnValue.Value.Length, ref pointer);
                Serializator.WriteString(nodeBuffer, columnValue.Value, ref pointer);
                break;*/

            default:
                throw new Exception("Can't use this type as index");
        }
    }

    private async Task<BTreeMultiNode<ColumnValue>?> GetMultiNode(BufferPoolHandler tablespace, int offset)
    {
        byte[] data = await tablespace.GetDataFromPage(offset);
        if (data.Length == 0)
            return null;

        BTreeMultiNode<ColumnValue> node = new(-1);

        node.Dirty = false; // read nodes from disk must be not persisted

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
                entry.Value = await ReadOffsets(tablespace, subTreeOffset);

            int nextPageOffset = Serializator.ReadInt32(data, ref pointer);
            if (nextPageOffset > -1)
                entry.Next = await GetMultiNode(tablespace, nextPageOffset);

            //Console.WriteLine("Children={0} Key={1} Value={2} NextOffset={3}", i, entry.Key, entry.Value, nextPageOffset);

            node.children[i] = entry;
        }

        return node;
    }
}
