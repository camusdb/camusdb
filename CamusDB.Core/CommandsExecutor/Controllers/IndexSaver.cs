
using CamusDB.Core.BufferPool;
using CamusDB.Core.Serializer;
using CamusDB.Core.Util.Trees;

namespace CamusDB.Core.CommandsExecutor.Controllers;

public sealed class IndexSaver
{
    public async Task Save(BufferPoolHandler tablespace, BTree index, int key, int value)
    {
        try
        {
            await index.WriteLock.WaitAsync();

            //Console.WriteLine("---");

            await SaveInternal(tablespace, index, key, value);

            //Console.WriteLine("---");
        }
        finally
        {
            index.WriteLock.Release();
        }
    }

    public async Task NoLockingSave(BufferPoolHandler tablespace, BTree index, int key, int value)
    {
        //Console.WriteLine("---");

        await SaveInternal(tablespace, index, key, value);

        //Console.WriteLine("---");
    }

    private static async Task SaveInternal(BufferPoolHandler tablespace, BTree index, int key, int value)
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

        foreach (BTreeNode node in index.NodesTraverse())
        {
            if (!node.Dirty)
            {
                //Console.WriteLine("Node at {0} is not dirty", node.PageOffset);
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

            //Console.WriteLine("Page={0} Length={1}", node.PageOffset, nodeBuffer.Length);
        }
    }
}

