
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
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Controllers.Indexes;

internal sealed class IndexMultiSaver : IndexBaseSaver
{
    private readonly IndexSaver indexSaver;

    public IndexMultiSaver(IndexSaver indexSaver)
    {
        this.indexSaver = indexSaver;
    }

    public async Task Save(BufferPoolHandler tablespace, BTreeMulti<ColumnValue> index, ColumnValue key, BTreeTuple value)
    {
        try
        {
            await index.WriteLock.WaitAsync();

            await SaveInternal(tablespace, index, key, value);
        }
        finally
        {
            index.WriteLock.Release();
        }
    }

    public async Task NoLockingSave(BufferPoolHandler tablespace, BTreeMulti<ColumnValue> index, ColumnValue key, BTreeTuple value)
    {
        await SaveInternal(tablespace, index, key, value);
    }

    public async Task Remove(BufferPoolHandler tablespace, BTreeMulti<ColumnValue> index, ColumnValue key)
    {
        try
        {
            await index.WriteLock.WaitAsync();

            await RemoveInternal(tablespace, index, key);
        }
        finally
        {
            index.WriteLock.Release();
        }
    }

    private async Task SaveInternal(BufferPoolHandler tablespace, BTreeMulti<ColumnValue> index, ColumnValue key, BTreeTuple value)
    {
        Dictionary<int, BTreeMultiDelta<ColumnValue>> deltas = await index.Put(key, value);

        await PersistSave(tablespace, index, value, deltas);
    }

    private static async Task RemoveInternal(BufferPoolHandler tablespace, BTreeMulti<ColumnValue> index, ColumnValue key)
    {
        index.Remove(key);

        await PersistRemove(tablespace, index);
    }

    private async Task PersistSave(
        BufferPoolHandler tablespace,
        BTreeMulti<ColumnValue> index,
        BTreeTuple value,
        Dictionary<int, BTreeMultiDelta<ColumnValue>> deltas
    )
    {
        foreach (KeyValuePair<int, BTreeMultiDelta<ColumnValue>> delta in deltas)
        {
            if (delta.Value.Node.PageOffset == -1)
                delta.Value.Node.PageOffset = await tablespace.GetNextFreeOffset();
        }

        byte[] treeBuffer = new byte[SerializatorTypeSizes.TypeInteger32 * 4]; // height(4 byte) + size(4 byte) + denseSize(4 byte) + root(4 byte)

        int pointer = 0;
        Serializator.WriteInt32(treeBuffer, index.height, ref pointer);
        Serializator.WriteInt32(treeBuffer, index.size, ref pointer);
        Serializator.WriteInt32(treeBuffer, index.denseSize, ref pointer);
        Serializator.WriteInt32(treeBuffer, index.root!.PageOffset, ref pointer);

        await tablespace.WriteDataToPage(index.PageOffset, 0, treeBuffer);

        //Console.WriteLine("Will save index at {0}", index.PageOffset);

        //@todo update nodes concurrently

        foreach (KeyValuePair<int, BTreeMultiDelta<ColumnValue>> delta in deltas)
        {
            BTreeMultiNode<ColumnValue> node = delta.Value.Node;

            // @todo number entries must not be harcoded
            byte[] nodeBuffer = new byte[
                SerializatorTypeSizes.TypeInteger32 + // key count
                SerializatorTypeSizes.TypeInteger32 + // page offset
                GetKeySizes(node) // 12 int (4 byte) * nodeKeyCount
            ];

            pointer = 0;
            Serializator.WriteInt32(nodeBuffer, node.KeyCount, ref pointer);
            Serializator.WriteInt32(nodeBuffer, node.PageOffset, ref pointer);

            for (int i = 0; i < node.KeyCount; i++)
            {
                BTreeMultiEntry<ColumnValue> entry = node.children[i];

                if (entry is null)
                {
                    Serializator.WriteInt32(nodeBuffer, 0, ref pointer);
                    Serializator.WriteInt32(nodeBuffer, 0, ref pointer);
                    Serializator.WriteInt32(nodeBuffer, 0, ref pointer);
                    continue;
                }

                BTree<int, int?>? subTree = entry.Value;

                if (subTree is null)
                {
                    SerializeKey(nodeBuffer, entry.Key, ref pointer);
                    Serializator.WriteInt32(nodeBuffer, -1, ref pointer);
                    Serializator.WriteInt32(nodeBuffer, entry.Next is not null ? entry.Next.PageOffset : -1, ref pointer);
                    continue;
                }

                //Console.WriteLine("Read Tree={0} PageOffset={1}", subTree.Id, subTree.PageOffset);

                if (subTree.PageOffset == -1)
                    subTree.PageOffset = await tablespace.GetNextFreeOffset();

                if (delta.Value.InnerDeltas is null)
                    throw new Exception("Inner deltas cannot be null on " + delta.Value.Node.Id);

                Console.WriteLine("Saved deltas for {0} {1}", delta.Value.Node.Id, subTree.Id);

                SaveUniqueOffsetIndexTicket saveUniqueOffsetIndex = new(
                    tablespace: tablespace,
                    index: subTree,
                    key: value.SlotOne,
                    value.SlotTwo,
                    deltas: delta.Value.InnerDeltas
                );

                await indexSaver.Save(saveUniqueOffsetIndex);

                //Console.WriteLine("Write Tree={0} PageOffset={1}", subTree.Id, subTree.PageOffset);

                //Serializator.WriteInt32(nodeBuffer, entry.Key, ref pointer);
                SerializeKey(nodeBuffer, entry.Key, ref pointer);
                Serializator.WriteInt32(nodeBuffer, subTree.PageOffset, ref pointer);
                Serializator.WriteInt32(nodeBuffer, entry.Next is not null ? entry.Next.PageOffset : -1, ref pointer);
            }

            await tablespace.WriteDataToPage(node.PageOffset, 0, nodeBuffer);

            //Console.WriteLine("Node {0} at {1} Length={2}", node.Id, node.PageOffset, nodeBuffer.Length);            
        }
    }

    private static async Task PersistRemove(BufferPoolHandler tablespace, BTreeMulti<ColumnValue> index)
    {
        foreach (BTreeMultiNode<ColumnValue> node in index.NodesTraverse())
        {
            if (node.PageOffset == -1)
                node.PageOffset = await tablespace.GetNextFreeOffset();
        }

        byte[] treeBuffer = new byte[SerializatorTypeSizes.TypeInteger32 * 3]; // height(4 byte) + size(4 byte) + root(4 byte)

        int pointer = 0;
        Serializator.WriteInt32(treeBuffer, index.height, ref pointer);
        Serializator.WriteInt32(treeBuffer, index.size, ref pointer);
        Serializator.WriteInt32(treeBuffer, index.root!.PageOffset, ref pointer);

        await tablespace.WriteDataToPage(index.PageOffset, 0, treeBuffer);

        //Console.WriteLine("Will save index at {0}", index.PageOffset);

        //@todo update nodes concurrently 

        foreach (BTreeMultiNode<ColumnValue> node in index.NodesTraverse())
        {
            byte[] nodeBuffer = new byte[
                SerializatorTypeSizes.TypeInteger32 + // key count
                SerializatorTypeSizes.TypeInteger32 + // page offset
                GetKeySizes(node) // 12 int (4 byte) * nodeKeyCount
            ];

            pointer = 0;
            Serializator.WriteInt32(nodeBuffer, node.KeyCount, ref pointer);
            Serializator.WriteInt32(nodeBuffer, node.PageOffset, ref pointer);

            for (int i = 0; i < node.KeyCount; i++)
            {
                BTreeMultiEntry<ColumnValue> entry = node.children[i];

                if (entry is null)
                {
                    Serializator.WriteInt32(nodeBuffer, 0, ref pointer);
                    Serializator.WriteInt32(nodeBuffer, 0, ref pointer);
                    Serializator.WriteInt32(nodeBuffer, 0, ref pointer);
                    continue;
                }

                BTree<int, int?>? subTree = entry.Value;

                if (subTree is null)
                {
                    SerializeKey(nodeBuffer, entry.Key, ref pointer);
                    Serializator.WriteInt32(nodeBuffer, -1, ref pointer);
                    Serializator.WriteInt32(nodeBuffer, entry.Next is not null ? entry.Next.PageOffset : -1, ref pointer);
                    continue;
                }

                // @todo destroy internal tree

                //Console.WriteLine("Read Tree={0} PageOffset={1}", subTree.Id, subTree.PageOffset);                

                //await indexSaver.Save(tablespace, subTree, value.SlotOne, value.SlotTwo, false);
                //await indexSaver.Remove(tablespace, subTree, entry.Key);

                //Console.WriteLine("Write Tree={0} PageOffset={1}", subTree.Id, subTree.PageOffset);

                //Serializator.WriteInt32(nodeBuffer, entry.Key, ref pointer);
                SerializeKey(nodeBuffer, entry.Key, ref pointer);
                Serializator.WriteInt32(nodeBuffer, subTree.PageOffset, ref pointer);
                Serializator.WriteInt32(nodeBuffer, entry.Next is not null ? entry.Next.PageOffset : -1, ref pointer);
            }

            await tablespace.WriteDataToPage(node.PageOffset, 0, nodeBuffer);

            //Console.WriteLine("Node {0} at {1} Length={2}", node.Id, node.PageOffset, nodeBuffer.Length);            
        }
    }
}
