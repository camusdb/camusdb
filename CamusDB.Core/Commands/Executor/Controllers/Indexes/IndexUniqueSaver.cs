
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

namespace CamusDB.Core.CommandsExecutor.Controllers.Indexes;

internal sealed class IndexUniqueSaver : IndexBaseSaver
{
    private readonly IndexSaver indexSaver;

    public IndexUniqueSaver(IndexSaver indexSaver)
    {
        this.indexSaver = indexSaver;
    }

    public async Task Save(SaveUniqueIndexTicket ticket)
    {        
        await SaveInternal(ticket);
    }

    public async Task NoLockingSave(SaveUniqueIndexTicket ticket)
    {
        await SaveInternal(ticket);
    }

    public async Task Remove(RemoveUniqueIndexTicket ticket)
    {        
        await RemoveInternal(ticket);
    }

    private static async Task SaveInternal(SaveUniqueIndexTicket ticket)
    {
        HashSet<BTreeNode<ColumnValue, BTreeTuple?>> deltas = await ticket.Index.Put(ticket.TxnId, ticket.Key, ticket.Value);

        Persist(ticket.Tablespace, ticket.Index, ticket.ModifiedPages, deltas);
    }

    private static async Task RemoveInternal(RemoveUniqueIndexTicket ticket)
    {
        (bool found, HashSet<BTreeNode<ColumnValue, BTreeTuple?>> deltas) = await ticket.Index.Remove(ticket.Key);

        if (found)
            Persist(ticket.Tablespace, ticket.Index, ticket.ModifiedPages, deltas);
    }

    private static void Persist(
        BufferPoolHandler tablespace,        
        BTree<ColumnValue, BTreeTuple?> index,
        List<BufferPageOperation> modifiedPages,
        HashSet<BTreeNode<ColumnValue, BTreeTuple?>> deltas
    )
    {
        if (deltas.Count == 0)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Deltas cannot be null or empty"
            );

        foreach (BTreeNode<ColumnValue, BTreeTuple?> node in deltas)
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

        // Write to buffer page
        //await tablespace.WriteDataToPage(index.PageOffset, sequence, treeBuffer);
        //modifiedPages.Add(new BufferPageOperation(BufferPageOperationType.InsertOrUpdate, index.PageOffset, sequence, treeBuffer));

        tablespace.WriteDataToPageBatch(modifiedPages, index.PageOffset, 0, treeBuffer);

        //@todo update nodes concurrently

        foreach (BTreeNode<ColumnValue, BTreeTuple?> node in deltas)
        {
            byte[] nodeBuffer = new byte[
                SerializatorTypeSizes.TypeInteger32 + // keyCount(4 byte) + 
                SerializatorTypeSizes.TypeObjectId +  // pageOffset(4 byte)
                GetKeySizes(node)
            ];

            pointer = 0;
            Serializator.WriteInt32(nodeBuffer, node.KeyCount, ref pointer);
            Serializator.WriteObjectId(nodeBuffer, node.PageOffset, ref pointer);

            for (int i = 0; i < node.KeyCount; i++)
            {
                BTreeEntry<ColumnValue, BTreeTuple?> entry = node.children[i];

                if (entry is not null)
                {
                    SerializeKey(nodeBuffer, entry.Key, ref pointer);
                    //SerializeTuple(nodeBuffer, entry.LastValue, ref pointer); @todo LastValue
                    Serializator.WriteObjectId(nodeBuffer, entry.Next is not null ? entry.Next.PageOffset : new(), ref pointer);
                }
                else
                {
                    Serializator.WriteInt8(nodeBuffer, 0, ref pointer);
                    Serializator.WriteObjectId(nodeBuffer, new(), ref pointer);
                    Serializator.WriteObjectId(nodeBuffer, new(), ref pointer);
                    Serializator.WriteObjectId(nodeBuffer, new(), ref pointer);
                }
            }

            //await tablespace.WriteDataToPage(node.PageOffset, sequence, nodeBuffer);
            //modifiedPages.Add(new BufferPageOperation(BufferPageOperationType.InsertOrUpdate, node.PageOffset, sequence, nodeBuffer));

            tablespace.WriteDataToPageBatch(modifiedPages, node.PageOffset, 0, nodeBuffer);

            //Console.WriteLine("Node {0}/{1} at {2} Length={3}", node.Id, node.KeyCount, node.PageOffset, nodeBuffer.Length);            
        }
    }
}
