﻿
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
using CamusDB.Core.Util.Trees.Experimental;

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

    public async Task Persist(
        BufferPoolManager tablespace,
        BPlusTree<ObjectIdValue, ObjectIdValue> index,
        List<BufferPageOperation> modifiedPages,
        BPlusTreeMutationDeltas<ObjectIdValue, ObjectIdValue> deltas
    )
    {
        // @todo this lock will produce contention
        using (await index.WriterLockAsync().ConfigureAwait(false))
        {
            foreach (BPlusTreeNode<ObjectIdValue, ObjectIdValue> node in deltas.Nodes)
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

            BPlusTreeNode<ObjectIdValue, ObjectIdValue>? rootNode = (await index.root.Next.ConfigureAwait(false));
            if (rootNode is null)
                return;

            //Serializator.WriteInt32(treeBuffer, index.height, ref pointer);
            //Serializator.WriteInt32(treeBuffer, index.size, ref pointer);
            Serializator.WriteObjectId(treeBuffer, rootNode.PageOffset, ref pointer);

            //await tablespace.WriteDataToPage(index.PageOffset, 0, treeBuffer);
            //modifiedPages.Add(new BufferPageOperation(BufferPageOperationType.InsertOrUpdate, index.PageOffset, 0, treeBuffer));

            tablespace.WriteDataToPageBatch(modifiedPages, index.PageOffset, 0, treeBuffer);

            ObjectIdValue nullAddressValue = new();
            HLCTimestamp nullTimestamp = HLCTimestamp.Zero;
            //@todo update nodes concurrently        

            foreach (BPlusTreeNode<ObjectIdValue, ObjectIdValue> node in deltas.Nodes)
            {
                //using IDisposable readerLock = await node.ReaderLockAsync();

                byte[] nodeBuffer = new byte[
                    SerializatorTypeSizes.TypeInteger32 + // key count
                    SerializatorTypeSizes.TypeObjectId +  // page offset
                    (36 + 12) * node.Entries.Count        // 36 bytes * node (key + value + next)
                ];

                pointer = 0;
                Serializator.WriteInt32(nodeBuffer, node.Entries.Count, ref pointer);
                Serializator.WriteObjectId(nodeBuffer, node.PageOffset, ref pointer);

                for (int i = 0; i < node.Entries.Count; i++)
                {
                    BPlusTreeEntry<ObjectIdValue, ObjectIdValue> entry = node.Entries[i];

                    if (entry is not null)
                    {
                        //Console.WriteLine("Saved K={0} T={1} V={2}", entry.Key, timestamp, value);
                        Serializator.WriteObjectId(nodeBuffer, entry.Key, ref pointer);

                        BPlusTreeNode<ObjectIdValue, ObjectIdValue>? next = await entry.Next.ConfigureAwait(false);

                        if (next is not null)
                        {
                            Serializator.WriteHLCTimestamp(nodeBuffer, nullTimestamp, ref pointer);
                            Serializator.WriteObjectId(nodeBuffer, nullAddressValue, ref pointer);
                            Serializator.WriteObjectId(nodeBuffer, next.PageOffset, ref pointer);
                        }
                        else
                        {
                            (HLCTimestamp timestamp, ObjectIdValue value) = entry.GetMaxCommittedValue();
                            Serializator.WriteHLCTimestamp(nodeBuffer, timestamp, ref pointer); // @todo LastValue
                            Serializator.WriteObjectId(nodeBuffer, value, ref pointer); // @todo LastValue
                            Serializator.WriteObjectId(nodeBuffer, nullAddressValue, ref pointer);
                        }
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

    public async Task PersistNodes(
        BufferPoolManager tablespace,
        BPlusTree<ObjectIdValue, ObjectIdValue> index,
        List<BufferPageOperation> modifiedPages,
        HashSet<BPlusTreeNode<ObjectIdValue, ObjectIdValue>> nodes
    )
    {
        
        foreach (BPlusTreeNode<ObjectIdValue, ObjectIdValue> node in nodes)
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

        BPlusTreeNode<ObjectIdValue, ObjectIdValue>? rootNode = (await index.root.Next.ConfigureAwait(false));
        if (rootNode is null)
            return;

        //Serializator.WriteInt32(treeBuffer, index.height, ref pointer);
        //Serializator.WriteInt32(treeBuffer, index.size, ref pointer);
        Serializator.WriteObjectId(treeBuffer, rootNode.PageOffset, ref pointer);

        //await tablespace.WriteDataToPage(index.PageOffset, 0, treeBuffer);
        //modifiedPages.Add(new BufferPageOperation(BufferPageOperationType.InsertOrUpdate, index.PageOffset, 0, treeBuffer));

        tablespace.WriteDataToPageBatch(modifiedPages, index.PageOffset, 0, treeBuffer);

        ObjectIdValue nullAddressValue = new();
        HLCTimestamp nullTimestamp = HLCTimestamp.Zero;
        //@todo update nodes concurrently

        foreach (BPlusTreeNode<ObjectIdValue, ObjectIdValue> node in nodes)
        {
            //using IDisposable readerLock = await node.ReaderLockAsync();

            byte[] nodeBuffer = new byte[
                SerializatorTypeSizes.TypeInteger32 + // key count
                SerializatorTypeSizes.TypeObjectId +  // page offset
                (36 + 12) * node.Entries.Count        // 36 bytes * node (key + value + next)
            ];

            pointer = 0;
            Serializator.WriteInt32(nodeBuffer, node.Entries.Count, ref pointer);
            Serializator.WriteObjectId(nodeBuffer, node.PageOffset, ref pointer);

            for (int i = 0; i < node.Entries.Count; i++)
            {
                BPlusTreeEntry<ObjectIdValue, ObjectIdValue> entry = node.Entries[i];

                if (entry is not null)
                {
                    //Console.WriteLine("Saved K={0} T={1} V={2}", entry.Key, timestamp, value);
                    Serializator.WriteObjectId(nodeBuffer, entry.Key, ref pointer);

                    BPlusTreeNode<ObjectIdValue, ObjectIdValue>? next = await entry.Next.ConfigureAwait(false);

                    if (next is not null)
                    {
                        Serializator.WriteHLCTimestamp(nodeBuffer, nullTimestamp, ref pointer);
                        Serializator.WriteObjectId(nodeBuffer, nullAddressValue, ref pointer);
                        Serializator.WriteObjectId(nodeBuffer, next.PageOffset, ref pointer);
                    }
                    else
                    {
                        (HLCTimestamp timestamp, ObjectIdValue value) = entry.GetMaxCommittedValue();
                        Serializator.WriteHLCTimestamp(nodeBuffer, timestamp, ref pointer); // @todo LastValue
                        Serializator.WriteObjectId(nodeBuffer, value, ref pointer); // @todo LastValue
                        Serializator.WriteObjectId(nodeBuffer, nullAddressValue, ref pointer);
                    }
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
