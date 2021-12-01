
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.BufferPool;
using CamusDB.Core.Serializer;
using CamusDB.Core.Util.Trees;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class IndexSaver
{
    public async Task Save(BufferPoolHandler tablespace, BTree index, int key, int value)
    {
        try
        {
            await index.WriteLock.WaitAsync();

            await SaveUniqueInternal(tablespace, index, key, value);
        }
        finally
        {
            index.WriteLock.Release();
        }
    }

    public async Task Save(BufferPoolHandler tablespace, BTreeMulti index, int key, int value)
    {
        try
        {
            await index.WriteLock.WaitAsync();

            await SaveMultiInternal(tablespace, index, key, value);
        }
        finally
        {
            index.WriteLock.Release();
        }
    }

    public async Task NoLockingSave(BufferPoolHandler tablespace, BTree index, int key, int value)
    {
        await SaveUniqueInternal(tablespace, index, key, value);
    }

    public async Task NoLockingSave(BufferPoolHandler tablespace, BTreeMulti index, int key, int value)
    {
        await SaveMultiInternal(tablespace, index, key, value);
    }

    private static async Task SaveUniqueInternal(BufferPoolHandler tablespace, BTree index, int key, int value)
    {
        index.Put(key, value);

        foreach (BTreeNode node in index.NodesTraverse())
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
        Serializator.WriteInt32(treeBuffer, index.n, ref pointer);
        Serializator.WriteInt32(treeBuffer, index.root.PageOffset, ref pointer);

        await tablespace.WriteDataToPage(index.PageOffset, treeBuffer);

        //Console.WriteLine("Will save index at {0}", index.PageOffset);

        //@todo update nodes concurrently

        int dirty = 0, noDirty = 0;

        foreach (BTreeNode node in index.NodesTraverse())
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
                BTreeEntry entry = node.children[i];

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

    private async Task SaveMultiInternal(BufferPoolHandler tablespace, BTreeMulti index, int key, int value)
    {
        index.Put(key, value);

        foreach (BTreeMultiNode node in index.NodesTraverse())
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
        Serializator.WriteInt32(treeBuffer, index.n, ref pointer);
        Serializator.WriteInt32(treeBuffer, index.root.PageOffset, ref pointer);

        await tablespace.WriteDataToPage(index.PageOffset, treeBuffer);

        //Console.WriteLine("Will save index at {0}", index.PageOffset);

        //@todo update nodes concurrently

        int dirty = 0, noDirty = 0;

        foreach (BTreeMultiNode node in index.NodesTraverse())
        {
            if (!node.Dirty)
            {
                noDirty++;
                //Console.WriteLine("Node {0} at {1} is not dirty", node.Id, node.PageOffset);
                continue;
            }

            byte[] nodeBuffer = new byte[8 + 12 * node.KeyCount]; // 8 node entries + 12 int (4 byte) * nodeKeyCount

            pointer = 0;
            Serializator.WriteInt32(nodeBuffer, node.KeyCount, ref pointer);
            Serializator.WriteInt32(nodeBuffer, node.PageOffset, ref pointer);

            for (int i = 0; i < node.KeyCount; i++)
            {
                BTreeMultiEntry entry = node.children[i];

                if (entry is null)
                {
                    Serializator.WriteInt32(nodeBuffer, 0, ref pointer);
                    Serializator.WriteInt32(nodeBuffer, 0, ref pointer);
                    Serializator.WriteInt32(nodeBuffer, 0, ref pointer);
                    continue;
                }

                BTree? subTree = entry.Value;

                if (subTree is null)
                {
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInternalOperation,
                        "Internal multi index valus is null"
                    );
                }

                if (subTree.PageOffset == -1)
                    subTree.PageOffset = await tablespace.GetNextFreeOffset();

                await Save(tablespace, subTree, value, 0);

                Serializator.WriteInt32(nodeBuffer, entry.Key, ref pointer);
                Serializator.WriteInt32(nodeBuffer, subTree.PageOffset, ref pointer);
                Serializator.WriteInt32(nodeBuffer, entry.Next is not null ? entry.Next.PageOffset : -1, ref pointer);
            }

            await tablespace.WriteDataToPage(node.PageOffset, nodeBuffer);

            //Console.WriteLine("Node {0} at {1} Length={2}", node.Id, node.PageOffset, nodeBuffer.Length);
            dirty++;
        }

        //Console.WriteLine("Dirty={0} NoDirty={1}", dirty, noDirty);
    }
}

