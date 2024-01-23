
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
using CamusDB.Core.BufferPool.Models;
using CamusDB.Core.Util.Time;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Controllers.Indexes;

internal sealed class IndexUniqueSaver : IndexBaseSaver
{
    private readonly IndexSaver indexSaver;

    public IndexUniqueSaver(IndexSaver indexSaver)
    {
        this.indexSaver = indexSaver;
    }

    public async Task Save(SaveIndexTicket ticket)
    {
        await ticket.Index.Put(
            ticket.TxnId,
            ticket.CommitState,
            ticket.Key,
            ticket.Value,
            async (nodes) => await PersistNodes(ticket.Tablespace, ticket.Index, ticket.ModifiedPages, nodes).ConfigureAwait(false)
        ).ConfigureAwait(false);
    }

    public async Task Remove(RemoveUniqueIndexTicket ticket)
    {
        await RemoveInternal(ticket).ConfigureAwait(false);
    }

    private static async Task RemoveInternal(RemoveUniqueIndexTicket ticket)
    {
        (bool found, BTreeMutationDeltas<CompositeColumnValue, BTreeTuple> deltas) = await ticket.Index.Remove(ticket.Key).ConfigureAwait(false);

        //if (found)
        //    Persist(ticket.Tablespace, ticket.Index, ticket.ModifiedPages, deltas);
    }    

    private static async Task PersistNodes(
        BufferPoolManager tablespace,
        BTree<CompositeColumnValue, BTreeTuple> index,
        List<BufferPageOperation> modifiedPages,
        HashSet<BTreeNode<CompositeColumnValue, BTreeTuple>> nodes
    )
    {
        if (nodes.Count == 0)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Deltas cannot be null or empty"
            );

        foreach (BTreeNode<CompositeColumnValue, BTreeTuple> node in nodes)
        {
            if (node.PageOffset.IsNull())
                node.PageOffset = tablespace.GetNextFreeOffset();
        }

        byte[] treeBuffer = new byte[
            SerializatorTypeSizes.TypeInteger32 + // version (4 byte) +
            SerializatorTypeSizes.TypeInteger32 + // capacity (4 byte) +
            SerializatorTypeSizes.TypeInteger32 + // height (4 byte) +
            SerializatorTypeSizes.TypeInteger32 + // size (4 byte)
            SerializatorTypeSizes.TypeObjectId    // root (4 byte)
        ];

        int pointer = 0;
        Serializator.WriteInt32(treeBuffer, BTreeConfig.LayoutVersion, ref pointer);
        Serializator.WriteInt32(treeBuffer, index.maxNodeCapacity, ref pointer);
        Serializator.WriteInt32(treeBuffer, index.height, ref pointer);
        Serializator.WriteInt32(treeBuffer, index.size, ref pointer);
        Serializator.WriteObjectId(treeBuffer, index.root!.PageOffset, ref pointer);

        // Write to buffer page
        //await tablespace.WriteDataToPage(index.PageOffset, sequence, treeBuffer);
        //modifiedPages.Add(new BufferPageOperation(BufferPageOperationType.InsertOrUpdate, index.PageOffset, sequence, treeBuffer));

        tablespace.WriteDataToPageBatch(modifiedPages, index.PageOffset, 0, treeBuffer);

        //@todo update nodes concurrently
        ObjectIdValue nullValue = new();
        BTreeTuple nullTuple = new(nullValue, nullValue);
        HLCTimestamp timestampZero = HLCTimestamp.Zero;

        foreach (BTreeNode<CompositeColumnValue, BTreeTuple> node in nodes)
        {
            byte[] nodeBuffer = new byte[
                SerializatorTypeSizes.TypeInteger32 + // keyCount(4 byte) + 
                SerializatorTypeSizes.TypeObjectId +  // pageOffset(4 byte)
                GetEntrySizes(node)
            ];

            pointer = 0;
            Serializator.WriteInt32(nodeBuffer, node.KeyCount, ref pointer);
            Serializator.WriteObjectId(nodeBuffer, node.PageOffset, ref pointer);

            for (int i = 0; i < node.KeyCount; i++)
            {
                BTreeEntry<CompositeColumnValue, BTreeTuple> entry = node.children[i];

                if (entry is not null)
                {
                    (HLCTimestamp timestamp, BTreeTuple? tuple) = entry.GetMaxCommittedValue();
                    BTreeNode<CompositeColumnValue, BTreeTuple>? next = (await entry.Next.ConfigureAwait(false));

                    //Console.WriteLine("Saved K={0} T={1} V={2}", entry.Key, timestamp, tuple);

                    SerializeKey(nodeBuffer, entry.Key, ref pointer);
                    Serializator.WriteHLCTimestamp(nodeBuffer, timestamp, ref pointer);
                    Serializator.WriteTuple(nodeBuffer, tuple ?? nullTuple, ref pointer); // @todo LastValue                    
                    Serializator.WriteObjectId(nodeBuffer, next is not null ? next.PageOffset : nullValue, ref pointer);
                }
                else
                {
                    Serializator.WriteInt8(nodeBuffer, 0, ref pointer);
                    Serializator.WriteHLCTimestamp(nodeBuffer, timestampZero, ref pointer);
                    Serializator.WriteTuple(nodeBuffer, nullTuple, ref pointer);
                    Serializator.WriteObjectId(nodeBuffer, nullValue, ref pointer);
                }
            }

            //await tablespace.WriteDataToPage(node.PageOffset, sequence, nodeBuffer);
            //modifiedPages.Add(new BufferPageOperation(BufferPageOperationType.InsertOrUpdate, node.PageOffset, sequence, nodeBuffer));

            tablespace.WriteDataToPageBatch(modifiedPages, node.PageOffset, 0, nodeBuffer);

            //Console.WriteLine("Modified Node {0}/{1} at {2} Pointer={3} BufferLength={4}", node.Id, node.KeyCount, node.PageOffset, pointer, nodeBuffer.Length);               
        }
    }
}
