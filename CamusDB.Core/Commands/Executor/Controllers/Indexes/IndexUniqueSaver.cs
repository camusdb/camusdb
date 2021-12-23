
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.BufferPool;
using CamusDB.Core.Serializer;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.Journal.Models;
using CamusDB.Core.Serializer.Models;
using CamusDB.Core.Journal.Models.Logs;
using CamusDB.Core.Journal.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

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
        try
        {
            await ticket.Index.WriteLock.WaitAsync();

            await SaveInternal(ticket);
        }
        finally
        {
            ticket.Index.WriteLock.Release();
        }
    }

    public async Task NoLockingSave(SaveUniqueIndexTicket ticket)
    {
        await SaveInternal(ticket);
    }

    public async Task Remove(RemoveUniqueIndexTicket ticket)
    {
        try
        {
            await ticket.Index.WriteLock.WaitAsync();

            await RemoveInternal(ticket);
        }
        finally
        {
            ticket.Index.WriteLock.Release();
        }
    }

    private static async Task SaveInternal(SaveUniqueIndexTicket ticket)
    {
        HashSet<BTreeNode<ColumnValue, BTreeTuple?>> deltas = ticket.Index.Put(ticket.Key, ticket.Value);

        await Persist(ticket.Tablespace, ticket.Journal, ticket.Sequence, ticket.SubSequence, ticket.FailureType, ticket.Index, deltas);
    }

    private static async Task RemoveInternal(RemoveUniqueIndexTicket ticket)
    {
        (bool found, HashSet<BTreeNode<ColumnValue, BTreeTuple?>> deltas) = ticket.Index.Remove(ticket.Key);

        if (found)
            await Persist(ticket.Tablespace, ticket.Journal, ticket.Sequence, ticket.SubSequence, ticket.FailureType, ticket.Index, deltas);
    }

    private static async Task Persist(
        BufferPoolHandler tablespace,
        JournalWriter journal,
        uint sequence,
        uint subSequence,
        JournalFailureTypes failureType,
        BTree<ColumnValue, BTreeTuple?> index,
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
            if (node.PageOffset == -1)
                node.PageOffset = await tablespace.GetNextFreeOffset();
        }

        byte[] treeBuffer = new byte[
            SerializatorTypeSizes.TypeInteger32 + // height(4 byte) +
            SerializatorTypeSizes.TypeInteger32 + // size(4 byte)
            SerializatorTypeSizes.TypeInteger32 // root(4 byte)
        ]; 

        int pointer = 0;
        Serializator.WriteInt32(treeBuffer, index.height, ref pointer);
        Serializator.WriteInt32(treeBuffer, index.size, ref pointer);
        Serializator.WriteInt32(treeBuffer, index.root!.PageOffset, ref pointer);

        // Save node modification to journal
        WritePageLog schedule = new(sequence, subSequence, treeBuffer);
        await journal.Append(failureType, schedule);

        // Write to buffer page
        await tablespace.WriteDataToPage(index.PageOffset, sequence, treeBuffer);

        //@todo update nodes concurrently

        foreach (BTreeNode<ColumnValue, BTreeTuple?> node in deltas)
        {
            byte[] nodeBuffer = new byte[
                SerializatorTypeSizes.TypeInteger32 + // keyCount(4 byte) + 
                SerializatorTypeSizes.TypeInteger32 + // pageOffset(4 byte)
                GetKeySizes(node)
            ]; 

            pointer = 0;
            Serializator.WriteInt32(nodeBuffer, node.KeyCount, ref pointer);
            Serializator.WriteInt32(nodeBuffer, node.PageOffset, ref pointer);

            for (int i = 0; i < node.KeyCount; i++)
            {
                BTreeEntry<ColumnValue, BTreeTuple?> entry = node.children[i];

                if (entry is not null)
                {
                    SerializeKey(nodeBuffer, entry.Key, ref pointer);
                    SerializeTuple(nodeBuffer, entry.Value, ref pointer);
                    Serializator.WriteInt32(nodeBuffer, entry.Next is not null ? entry.Next.PageOffset : -1, ref pointer);
                }
                else
                {
                    Serializator.WriteInt8(nodeBuffer, 0, ref pointer);
                    Serializator.WriteInt32(nodeBuffer, 0, ref pointer);
                    Serializator.WriteInt32(nodeBuffer, 0, ref pointer);
                    Serializator.WriteInt32(nodeBuffer, 0, ref pointer);
                }
            }

            // Save modified page to journal
            schedule = new(sequence, subSequence, nodeBuffer);
            await journal.Append(failureType, schedule);

            await tablespace.WriteDataToPage(node.PageOffset, sequence, nodeBuffer);

            //Console.WriteLine("Node {0} at {1} Length={2}", node.Id, node.PageOffset, nodeBuffer.Length);            
        }
    }
}
