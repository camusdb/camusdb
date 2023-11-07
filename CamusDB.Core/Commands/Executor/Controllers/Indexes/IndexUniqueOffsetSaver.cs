
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

        await SaveInternal(ticket.Tablespace, ticket.Index, ticket.Key, ticket.Value, ticket.Deltas);
    }

    private static async Task SaveInternal(
        BufferPoolHandler tablespace,
        BTree<int, int?> index,
        int key,
        int value,
        HashSet<BTreeNode<int, int?>>? deltas
    )
    {
        if (deltas is null)
            deltas = await index.Put(key, value);

        foreach (BTreeNode<int, int?> node in deltas)
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

        await tablespace.WriteDataToPage(index.PageOffset, 0, treeBuffer);

        //@todo update nodes concurrently        

        foreach (BTreeNode<int, int?> node in deltas)
        {
            byte[] nodeBuffer = new byte[
                SerializatorTypeSizes.TypeInteger32 + // key count
                SerializatorTypeSizes.TypeInteger32 + // page offset
                12 * node.KeyCount // 12 bytes * node (key + value + next)
            ];

            pointer = 0;
            Serializator.WriteInt32(nodeBuffer, node.KeyCount, ref pointer);
            Serializator.WriteInt32(nodeBuffer, node.PageOffset, ref pointer);

            for (int i = 0; i < node.KeyCount; i++)
            {
                BTreeEntry<int, int?> entry = node.children[i];

                if (entry is not null)
                {
                    Serializator.WriteInt32(nodeBuffer, entry.Key, ref pointer);
                    Serializator.WriteInt32(nodeBuffer, entry.Value ?? 0, ref pointer);
                    Serializator.WriteInt32(nodeBuffer, entry.Next is not null ? entry.Next.PageOffset : -1, ref pointer);
                    //Console.WriteLine(pointer);
                }
                else
                {
                    Serializator.WriteInt32(nodeBuffer, 0, ref pointer);
                    Serializator.WriteInt32(nodeBuffer, 0, ref pointer);
                    Serializator.WriteInt32(nodeBuffer, 0, ref pointer);
                }
            }

            await tablespace.WriteDataToPage(node.PageOffset, 0, nodeBuffer);

            //Console.WriteLine("Node {0} at {1} Length={2}", node.Id, node.PageOffset, nodeBuffer.Length);
        }
    }
}
