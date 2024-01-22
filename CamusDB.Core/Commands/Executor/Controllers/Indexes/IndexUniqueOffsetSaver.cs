
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

    public async Task Save(SaveOffsetIndexTicket ticket)
    {
        await ticket.Index.Put(
            ticket.TxnId,
            ticket.CommitState,
            ticket.Key,
            ticket.Value,
            async (nodes) => await PersistNodes(ticket.Tablespace, ticket.Index, ticket.ModifiedPages, nodes)
        ).ConfigureAwait(false);
    }

    public async Task Remove(RemoveUniqueOffsetIndexTicket ticket)
    {
        await RemoveInternal(ticket.Tablespace, ticket.Index, ticket.Key, ticket.ModifiedPages, ticket.Deltas).ConfigureAwait(false);
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
            (bool found, BTreeMutationDeltas<ObjectIdValue, ObjectIdValue> newDeltas) = await index.Remove(key).ConfigureAwait(false);

            //if (found)
            //    Persist(tablespace, index, modifiedPages, newDeltas);
        }
    }    

    private async Task PersistNodes(
        BufferPoolManager tablespace,
        BTree<ObjectIdValue, ObjectIdValue> index,
        List<BufferPageOperation> modifiedPages,
        HashSet<BTreeNode<ObjectIdValue, ObjectIdValue>> nodes
    )
    {
        foreach (BTreeNode<ObjectIdValue, ObjectIdValue> node in nodes)
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

        //Console.WriteLine("Persisting Index {0} at {1} Height={2} Size={3}", index.Id, index.PageOffset, index.height, index.size);

        //await tablespace.WriteDataToPage(index.PageOffset, 0, treeBuffer);
        //modifiedPages.Add(new BufferPageOperation(BufferPageOperationType.InsertOrUpdate, index.PageOffset, 0, treeBuffer));

        tablespace.WriteDataToPageBatch(modifiedPages, index.PageOffset, 0, treeBuffer);

        ObjectIdValue nullAddressValue = new();
        HLCTimestamp nullTimestamp = HLCTimestamp.Zero;
        //@todo update nodes concurrently        

        //Console.WriteLine("Persisting {0} nodes", nodes.Count);

        foreach (BTreeNode<ObjectIdValue, ObjectIdValue> node in nodes)
        {
            //Console.WriteLine("> Persisting Node {0} at {1} KeyCount={2}", node.Id, node.PageOffset, node.KeyCount);            

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
                    //Console.WriteLine("-> Persisting Entry {0} from {1} Key={2}", i, node.Id, entry.Key);

                    (HLCTimestamp timestamp, ObjectIdValue value) = entry.GetMaxCommittedValue();

                    //Console.WriteLine("-> Saved K={0} T={1} V={2}", entry.Key, timestamp, value);

                    Serializator.WriteObjectId(nodeBuffer, entry.Key, ref pointer);
                    Serializator.WriteHLCTimestamp(nodeBuffer, timestamp, ref pointer); // @todo LastValue
                    Serializator.WriteObjectId(nodeBuffer, value, ref pointer); // @todo LastValue

                    BTreeNode<ObjectIdValue, ObjectIdValue>? next = (await entry.Next.ConfigureAwait(false));
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

            //Console.WriteLine("Modified Node {0}/{1} at {2} Pointer={3} BufferLength={4}", node.Id, node.KeyCount, node.PageOffset, pointer, nodeBuffer.Length);
        }
    }
}
