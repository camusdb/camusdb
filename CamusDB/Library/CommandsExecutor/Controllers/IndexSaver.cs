
using CamusDB.Library.BufferPool;
using CamusDB.Library.Serializer;
using CamusDB.Library.Util.Trees;

namespace CamusDB.Library.CommandsExecutor.Controllers;

public sealed class IndexSaver
{   
    public async Task Save(BufferPoolHandler tablespace, BTree index, int key, int value)
    {
        try
        {
            await index.WriteLock.WaitAsync();            

            index.Put(key, value);

            foreach (Node node in index.NodesTraverse())
            {
                if (node.PageOffset == -1)
                    node.PageOffset = await tablespace.GetNextFreeOffset();
            }

            byte[] treeBuffer = new byte[12]; // height + size + root

            int pointer = 0;
            Serializator.WriteInt32(treeBuffer, index.height, ref pointer);
            Serializator.WriteInt32(treeBuffer, index.n, ref pointer);
            Serializator.WriteInt32(treeBuffer, index.root.PageOffset, ref pointer);

            await tablespace.WriteDataToPage(index.PageOffset, treeBuffer);

            foreach (Node node in index.NodesTraverse())
            {
                byte[] nodeBuffer = new byte[4 + 4 + 4 * 12];

                pointer = 0;
                Serializator.WriteInt32(nodeBuffer, node.KeyCount, ref pointer);
                Serializator.WriteInt32(nodeBuffer, node.PageOffset, ref pointer);

                for (int i = 0; i < 4; i++)
                {
                    Entry entry = node.children[i];

                    if (entry is not null)
                    {
                        Serializator.WriteInt32(nodeBuffer, entry.Key, ref pointer);
                        Serializator.WriteInt32(nodeBuffer, entry.Value ?? 0, ref pointer);
                        Serializator.WriteInt32(nodeBuffer, entry.Next is not null ? entry.Next.PageOffset : -1, ref pointer);
                    }
                    else
                    {
                        Serializator.WriteInt32(nodeBuffer, 0, ref pointer);
                        Serializator.WriteInt32(nodeBuffer, 0, ref pointer);
                        Serializator.WriteInt32(nodeBuffer, 0, ref pointer);
                    }
                }

                await tablespace.WriteDataToPage(node.PageOffset, nodeBuffer);

                Console.WriteLine("Page={0} Length={1}", node.PageOffset, nodeBuffer.Length);
            }
        }
        finally
        {
            index.WriteLock.Release();
        }
    }
}

