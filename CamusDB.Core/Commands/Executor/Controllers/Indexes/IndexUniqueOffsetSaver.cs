
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
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.BufferPool.Models;
using CamusDB.Core.Util.Time;

namespace CamusDB.Core.CommandsExecutor.Controllers.Indexes;

internal sealed class IndexUniqueOffsetSaver : IndexBaseSaver
{
    private readonly IndexSaver indexSaver;

    public IndexUniqueOffsetSaver(IndexSaver indexSaver)
    {
        this.indexSaver = indexSaver;
    }

    public async Task<BTreeMutationDeltas<ObjectIdValue, ObjectIdValue>> Save(SaveUniqueOffsetIndexTicket ticket)
    {        
        using IDisposable writerLock = await ticket.Index.WriterLockAsync();

        BTreeMutationDeltas<ObjectIdValue, ObjectIdValue> mutations = await ticket.Index.Put(ticket.TxnId, BTreeCommitState.Uncommitted, ticket.Key, ticket.Value);

        await Persist(ticket.Tablespace, ticket.Index, ticket.ModifiedPages, mutations);

        return mutations;
    }

    public async Task Remove(RemoveUniqueOffsetIndexTicket ticket)
    {
        await RemoveInternal(ticket.Tablespace, ticket.Index, ticket.Key, ticket.ModifiedPages, ticket.Deltas);
    }   

    private static async Task RemoveInternal(
        BufferPoolManager tablespace,
        BTree<ObjectIdValue, ObjectIdValue> index,
        ObjectIdValue key,
        List<BufferPageOperation> modifiedPages,
        BTreeMutationDeltas<ObjectIdValue, ObjectIdValue>? deltas
    )
    {
        if (deltas is null)
        {
            (bool found, BTreeMutationDeltas<ObjectIdValue, ObjectIdValue> newDeltas) = await index.Remove(key);

            //if (found)
            //    Persist(tablespace, index, modifiedPages, newDeltas);
        }
    }

    private async Task Persist(
        BufferPoolManager tablespace,
        BTree<ObjectIdValue, ObjectIdValue> index,
        List<BufferPageOperation> modifiedPages,
        BTreeMutationDeltas<ObjectIdValue, ObjectIdValue> deltas
    )
    {
        foreach (BTreeNode<ObjectIdValue, ObjectIdValue> node in deltas.Nodes)
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
        //modifiedPages.Add(new BufferPageOperation(BufferPageOperationType.InsertOrUpdate, index.PageOffset, 0, treeBuffer));

        tablespace.WriteDataToPageBatch(modifiedPages, index.PageOffset, 0, treeBuffer);

        ObjectIdValue nullAddressValue = new();
        HLCTimestamp nullTimestamp = HLCTimestamp.Zero;

        //@todo update nodes concurrently        

        foreach (BTreeNode<ObjectIdValue, ObjectIdValue> node in deltas.Nodes)
        {
            using IDisposable readerLock = await node.ReaderLockAsync();

            byte[] nodeBuffer = new byte[
                SerializatorTypeSizes.TypeInteger32 + // key count
                SerializatorTypeSizes.TypeObjectId +  // page offset
                (36 + 12) * node.KeyCount             // 36 bytes * node (key + value + next)
            ];

            pointer = 0;
            Serializator.WriteInt32(nodeBuffer, node.KeyCount, ref pointer);
            Serializator.WriteObjectId(nodeBuffer, node.PageOffset, ref pointer);

            for (int i = 0; i < node.KeyCount; i++)
            {
                BTreeEntry<ObjectIdValue, ObjectIdValue> entry = node.children[i];

                if (entry is not null)
                {
                    (HLCTimestamp timestamp, ObjectIdValue value) = entry.GetMaxCommitedValue();

                    //Console.WriteLine("Saved K={0} T={1} V={2}", entry.Key, timestamp, value);

                    Serializator.WriteObjectId(nodeBuffer, entry.Key, ref pointer);
                    Serializator.WriteHLCTimestamp(nodeBuffer, timestamp, ref pointer); // @todo LastValue
                    Serializator.WriteObjectId(nodeBuffer, value, ref pointer); // @todo LastValue

                    BTreeNode<ObjectIdValue, ObjectIdValue>? next = (await entry.Next);
                    Serializator.WriteObjectId(nodeBuffer, next is not null ? next.PageOffset : nullAddressValue, ref pointer);
                }
                else
                {
                    Serializator.WriteObjectId(nodeBuffer, nullAddressValue, ref pointer);
                    Serializator.WriteHLCTimestamp(nodeBuffer, nullTimestamp, ref pointer);
                    Serializator.WriteObjectId(nodeBuffer, nullAddressValue, ref pointer);
                    Serializator.WriteObjectId(nodeBuffer, nullAddressValue, ref pointer);
                }
            }

            //await tablespace.WriteDataToPage(node.PageOffset, 0, nodeBuffer);
            //modifiedPages.Add(new BufferPageOperation(BufferPageOperationType.InsertOrUpdate, node.PageOffset, 0, nodeBuffer));

            tablespace.WriteDataToPageBatch(modifiedPages, node.PageOffset, 0, nodeBuffer);

            Console.WriteLine("Modified Node {0} at {1} Height={2} KeyCount={3} Length={4}", node.Id, node.PageOffset, index.height, node.KeyCount, nodeBuffer.Length);
        }
    }
}
