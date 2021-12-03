
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.BufferPool;
using CamusDB.Core.Serializer;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Indexes;

internal sealed class IndexUniqueOffsetSaver : IndexBaseSaver
{
    private readonly IndexSaver indexSaver;

    public IndexUniqueOffsetSaver(IndexSaver indexSaver)
    {
        this.indexSaver = indexSaver;
    }

    public async Task Save(BufferPoolHandler tablespace, BTree<int, int?> index, int key, int value, bool insert = true)
    {
        try
        {
            await index.WriteLock.WaitAsync();

            await SaveInternal(tablespace, index, key, value, insert);
        }
        finally
        {
            index.WriteLock.Release();
        }
    }

    private static async Task SaveInternal(BufferPoolHandler tablespace, BTree<int, int?> index, int key, int value, bool insert)
    {
        if (insert)
            index.Put(key, value);

        foreach (BTreeNode<int, int?> node in index.NodesTraverse())
        {
            if (node.PageOffset == -1)
            {
                node.Dirty = true;
                node.PageOffset = await tablespace.GetNextFreeOffset();
            }

            //Console.WriteLine("Will save node at {0}", node.PageOffset);
        }

        byte[] treeBuffer = new byte[12]; // height + size + root

        int pointer = 0;
        Serializator.WriteInt32(treeBuffer, index.height, ref pointer);
        Serializator.WriteInt32(treeBuffer, index.size, ref pointer);
        Serializator.WriteInt32(treeBuffer, index.root.PageOffset, ref pointer);

        await tablespace.WriteDataToPage(index.PageOffset, treeBuffer);

        //Console.WriteLine("Will save index at {0}", index.PageOffset);

        //@todo update nodes concurrently

        int dirty = 0, noDirty = 0;

        foreach (BTreeNode<int, int?> node in index.NodesTraverse())
        {
            if (!node.Dirty)
            {
                noDirty++;
                //Console.WriteLine("Node {0} at {1} is not dirty", node.Id, node.PageOffset);
                continue;
            }

            byte[] nodeBuffer = new byte[8 + 12 * node.KeyCount];

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

            await tablespace.WriteDataToPage(node.PageOffset, nodeBuffer);

            //Console.WriteLine("Node {0} at {1} Length={2}", node.Id, node.PageOffset, nodeBuffer.Length);
            dirty++;
        }

        //Console.WriteLine("Dirty={0} NoDirty={1}", dirty, noDirty);
    }
}
