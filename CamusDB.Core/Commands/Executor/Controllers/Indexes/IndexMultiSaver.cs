
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
using CamusDB.Core.CommandsExecutor.Models.StateMachines;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Controllers.Indexes;

internal sealed class IndexMultiSaver : IndexBaseSaver
{
    private readonly IndexSaver indexSaver;

    public IndexMultiSaver(IndexSaver indexSaver)
    {
        this.indexSaver = indexSaver;
    }

    public async Task Save(SaveMultiKeyIndexTicket ticket)
    {
        await SaveInternal(ticket.Tablespace, ticket.Index, ticket.MultiKeyValue, ticket.RowTuple, ticket.Locks, ticket.ModifiedPages);
    }

    public async Task Remove(BufferPoolHandler tablespace, BTreeMulti<ColumnValue> index, ColumnValue key)
    {
        await RemoveInternal(tablespace, index, key);
    }

    private async Task SaveInternal(BufferPoolHandler tablespace, BTreeMulti<ColumnValue> index, ColumnValue key, BTreeTuple value, List<IDisposable> locks, List<InsertModifiedPage> modifiedPages)
    {
        Dictionary<int, BTreeMultiDelta<ColumnValue>> deltas = await index.Put(key, value);

        await PersistSave(tablespace, index, value, locks, modifiedPages, deltas);
    }

    private static async Task RemoveInternal(BufferPoolHandler tablespace, BTreeMulti<ColumnValue> index, ColumnValue key)
    {
        (bool found, HashSet<BTreeMultiNode<ColumnValue>>? deltas) = index.Remove(key);

        if (found)
            await PersistRemove(tablespace, index, deltas);
    }

    private async Task PersistSave(
        BufferPoolHandler tablespace,
        BTreeMulti<ColumnValue> index,
        BTreeTuple value,
        List<IDisposable> locks,
        List<InsertModifiedPage> modifiedPages,
        Dictionary<int, BTreeMultiDelta<ColumnValue>> deltas
    )
    {
        foreach (KeyValuePair<int, BTreeMultiDelta<ColumnValue>> delta in deltas)
        {
            if (delta.Value.Node.PageOffset.IsNull())
                delta.Value.Node.PageOffset = tablespace.GetNextFreeOffset();
        }

        // height(4 byte) + size(4 byte) + denseSize(4 byte) + root(4 byte)
        byte[] treeBuffer = new byte[SerializatorTypeSizes.TypeInteger32 * 3 + SerializatorTypeSizes.TypeObjectId];

        int pointer = 0;
        Serializator.WriteInt32(treeBuffer, index.height, ref pointer);
        Serializator.WriteInt32(treeBuffer, index.size, ref pointer);
        Serializator.WriteInt32(treeBuffer, index.denseSize, ref pointer);
        Serializator.WriteObjectId(treeBuffer, index.root!.PageOffset, ref pointer);

        //await tablespace.WriteDataToPage(index.PageOffset, 0, treeBuffer);
        modifiedPages.Add(new InsertModifiedPage(index.PageOffset, 0, treeBuffer));

        //Console.WriteLine("Will save index at {0}", index.PageOffset);

        //@todo update nodes concurrently

        foreach (KeyValuePair<int, BTreeMultiDelta<ColumnValue>> delta in deltas)
        {
            BTreeMultiNode<ColumnValue> node = delta.Value.Node;

            // @todo number entries must not be harcoded
            byte[] nodeBuffer = new byte[
                SerializatorTypeSizes.TypeInteger32 + // key count
                SerializatorTypeSizes.TypeObjectId + // page offset
                GetKeySizes(node) // 12 int (4 byte) * nodeKeyCount
            ];

            pointer = 0;
            Serializator.WriteInt32(nodeBuffer, node.KeyCount, ref pointer);
            Serializator.WriteObjectId(nodeBuffer, node.PageOffset, ref pointer);

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

                BTree<ObjectIdValue, ObjectIdValue>? subTree = entry.Value;

                if (subTree is null)
                {
                    SerializeKey(nodeBuffer, entry.Key, ref pointer);
                    Serializator.WriteInt32(nodeBuffer, -1, ref pointer);
                    Serializator.WriteObjectId(nodeBuffer, entry.Next is not null ? entry.Next.PageOffset : new(), ref pointer);
                    continue;
                }

                if (delta.Value.InnerDeltas is not null)
                {
                    //Console.WriteLine("Read Tree={0} PageOffset={1}", subTree.Id, subTree.PageOffset);

                    if (subTree.PageOffset.IsNull())
                        subTree.PageOffset = tablespace.GetNextFreeOffset();

                    if (delta.Value.InnerDeltas is null)
                        throw new Exception("Inner deltas cannot be null on " + delta.Value.Node.Id + " " + i + " " + subTree.Size());

                    //Console.WriteLine("Saved deltas for {0} {1}", delta.Value.Node.Id, subTree.Id);

                    SaveUniqueOffsetIndexTicket saveUniqueOffsetIndex = new(
                        tablespace: tablespace,
                        index: subTree,
                        key: value.SlotOne,
                        value.SlotTwo,
                        locks: locks,
                        modifiedPages: modifiedPages,
                        deltas: delta.Value.InnerDeltas
                    );

                    await indexSaver.Save(saveUniqueOffsetIndex);
                }

                //Console.WriteLine("Write Tree={0} PageOffset={1}", subTree.Id, subTree.PageOffset);

                //Serializator.WriteInt32(nodeBuffer, entry.Key, ref pointer);
                SerializeKey(nodeBuffer, entry.Key, ref pointer);
                Serializator.WriteObjectId(nodeBuffer, subTree.PageOffset, ref pointer);
                Serializator.WriteObjectId(nodeBuffer, entry.Next is not null ? entry.Next.PageOffset : new(), ref pointer);
            }

            //await tablespace.WriteDataToPage(node.PageOffset, 0, nodeBuffer);
            modifiedPages.Add(new InsertModifiedPage(node.PageOffset, 0, nodeBuffer));

            //Console.WriteLine("Node {0} at {1} Length={2}", node.Id, node.PageOffset, nodeBuffer.Length);
            //
            Console.WriteLine("{0}/{1}", pointer, nodeBuffer.Length);
        }
    }

    private static async Task PersistRemove(BufferPoolHandler tablespace, BTreeMulti<ColumnValue> index, HashSet<BTreeMultiNode<ColumnValue>> deltas)
    {
        foreach (BTreeMultiNode<ColumnValue> node in index.NodesTraverse())
        {
            if (node.PageOffset.IsNull())
                node.PageOffset = tablespace.GetNextFreeOffset();
        }

        byte[] treeBuffer = new byte[SerializatorTypeSizes.TypeInteger32 * 3]; // height(4 byte) + size(4 byte) + root(4 byte)

        int pointer = 0;
        Serializator.WriteInt32(treeBuffer, index.height, ref pointer);
        Serializator.WriteInt32(treeBuffer, index.size, ref pointer);
        Serializator.WriteObjectId(treeBuffer, index.root!.PageOffset, ref pointer);

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
            Serializator.WriteObjectId(nodeBuffer, node.PageOffset, ref pointer);

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

                BTree<ObjectIdValue, ObjectIdValue>? subTree = entry.Value;

                if (subTree is null)
                {
                    SerializeKey(nodeBuffer, entry.Key, ref pointer);
                    Serializator.WriteInt32(nodeBuffer, -1, ref pointer);
                    Serializator.WriteObjectId(nodeBuffer, entry.Next is not null ? entry.Next.PageOffset : new(), ref pointer);
                    continue;
                }

                // @todo destroy internal tree

                //Console.WriteLine("Read Tree={0} PageOffset={1}", subTree.Id, subTree.PageOffset);                

                //await indexSaver.Save(tablespace, subTree, value.SlotOne, value.SlotTwo, false);
                //await indexSaver.Remove(tablespace, subTree, entry.Key);

                //Console.WriteLine("Write Tree={0} PageOffset={1}", subTree.Id, subTree.PageOffset);

                //Serializator.WriteInt32(nodeBuffer, entry.Key, ref pointer);
                SerializeKey(nodeBuffer, entry.Key, ref pointer);
                Serializator.WriteObjectId(nodeBuffer, subTree.PageOffset, ref pointer);
                Serializator.WriteObjectId(nodeBuffer, entry.Next is not null ? entry.Next.PageOffset : new(), ref pointer);
            }

            await tablespace.WriteDataToPage(node.PageOffset, 0, nodeBuffer);

            //Console.WriteLine("Node {0} at {1} Length={2}", node.Id, node.PageOffset, nodeBuffer.Length);            
        }
    }
}
