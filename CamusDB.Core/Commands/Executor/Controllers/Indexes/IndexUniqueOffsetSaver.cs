
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
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Models.StateMachines;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Controllers.Indexes;

internal sealed class IndexUniqueOffsetSaver : IndexBaseSaver
{
    private readonly IndexSaver indexSaver;

    public IndexUniqueOffsetSaver(IndexSaver indexSaver)
    {
        this.indexSaver = indexSaver;
    }

    public async Task Save(SaveUniqueOffsetIndexTicket ticket)
    {
        ticket.Locks.Add(await ticket.Index.ReaderWriterLock.WriterLockAsync());

        await SaveInternal(ticket.Tablespace, ticket.Index, ticket.Key, ticket.Value, ticket.ModifiedPages, ticket.Deltas);
    }

    public async Task Remove(RemoveUniqueOffsetIndexTicket ticket)
    {
        ticket.Locks.Add(await ticket.Index.ReaderWriterLock.WriterLockAsync());

        await RemoveInternal(ticket.Tablespace, ticket.Index, ticket.Key, ticket.ModifiedPages, ticket.Deltas);
    }

    private static async Task SaveInternal(
        BufferPoolHandler tablespace,
        BTree<ObjectIdValue, ObjectIdValue> index,
        ObjectIdValue key,
        ObjectIdValue value,
        List<InsertModifiedPage> modifiedPages,
        HashSet<BTreeNode<ObjectIdValue, ObjectIdValue>>? deltas
    )
    {
        if (deltas is null)
        {
            deltas = await index.Put(key, value);

            Persist(tablespace, index, modifiedPages, deltas);
        }
    }

    private static async Task RemoveInternal(
        BufferPoolHandler tablespace,
        BTree<ObjectIdValue, ObjectIdValue> index,
        ObjectIdValue key,        
        List<InsertModifiedPage> modifiedPages,
        HashSet<BTreeNode<ObjectIdValue, ObjectIdValue>>? deltas
    )
    {
        if (deltas is null)
        {
            (bool found, HashSet<BTreeNode<ObjectIdValue, ObjectIdValue>> newDeltas) = await index.Remove(key);

            if (found)
                Persist(tablespace, index, modifiedPages, newDeltas);
        }
    }

    private static void Persist(
        BufferPoolHandler tablespace,
        BTree<ObjectIdValue, ObjectIdValue> index,
        List<InsertModifiedPage> modifiedPages,
        HashSet<BTreeNode<ObjectIdValue, ObjectIdValue>> deltas
    )
    {
        foreach (BTreeNode<ObjectIdValue, ObjectIdValue> node in deltas)
        {
            if (node.PageOffset.IsNull())
                node.PageOffset = tablespace.GetNextFreeOffset();
        }

        byte[] treeBuffer = new byte[
            SerializatorTypeSizes.TypeInteger32 + // height(4 byte) +
            SerializatorTypeSizes.TypeInteger32 + // size(4 byte)
            SerializatorTypeSizes.TypeObjectId    // root(4 byte)
        ];

        int pointer = 0;
        Serializator.WriteInt32(treeBuffer, index.height, ref pointer);
        Serializator.WriteInt32(treeBuffer, index.size, ref pointer);
        Serializator.WriteObjectId(treeBuffer, index.root!.PageOffset, ref pointer);

        //await tablespace.WriteDataToPage(index.PageOffset, 0, treeBuffer);
        modifiedPages.Add(new InsertModifiedPage(index.PageOffset, 0, treeBuffer));

        //@todo update nodes concurrently        

        foreach (BTreeNode<ObjectIdValue, ObjectIdValue> node in deltas)
        {
            byte[] nodeBuffer = new byte[
                SerializatorTypeSizes.TypeInteger32 + // key count
                SerializatorTypeSizes.TypeObjectId + // page offset
                36 * node.KeyCount // 36 bytes * node (key + value + next)
            ];

            pointer = 0;
            Serializator.WriteInt32(nodeBuffer, node.KeyCount, ref pointer);
            Serializator.WriteObjectId(nodeBuffer, node.PageOffset, ref pointer);

            for (int i = 0; i < node.KeyCount; i++)
            {
                BTreeEntry<ObjectIdValue, ObjectIdValue> entry = node.children[i];

                if (entry is not null)
                {
                    Serializator.WriteObjectId(nodeBuffer, entry.Key, ref pointer);
                    Serializator.WriteObjectId(nodeBuffer, entry.Value, ref pointer);
                    Serializator.WriteObjectId(nodeBuffer, entry.Next is not null ? entry.Next.PageOffset : new(), ref pointer);
                    //Console.WriteLine(pointer);
                }
                else
                {
                    Serializator.WriteObjectId(nodeBuffer, new(), ref pointer);
                    Serializator.WriteObjectId(nodeBuffer, new(), ref pointer);
                    Serializator.WriteObjectId(nodeBuffer, new(), ref pointer);
                }
            }

            //await tablespace.WriteDataToPage(node.PageOffset, 0, nodeBuffer);
            modifiedPages.Add(new InsertModifiedPage(node.PageOffset, 0, nodeBuffer));

            //Console.WriteLine("Node {0} at {1} Length={2}", node.Id, node.PageOffset, nodeBuffer.Length);            
        }
    }
}
