
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

internal sealed class IndexUniqueSaver : IndexBaseSaver
{
    private readonly IndexSaver indexSaver;

    public IndexUniqueSaver(IndexSaver indexSaver)
    {
        this.indexSaver = indexSaver;
    }

    public async Task Save(BufferPoolHandler tablespace, BTree<ColumnValue, BTreeTuple?> index, ColumnValue key, BTreeTuple value, bool insert = true)
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

    public async Task NoLockingSave(BufferPoolHandler tablespace, BTree<ColumnValue, BTreeTuple?> index, ColumnValue key, BTreeTuple value, bool insert = true)
    {
        await SaveInternal(tablespace, index, key, value, insert);
    }

    private static async Task SaveInternal(BufferPoolHandler tablespace, BTree<ColumnValue, BTreeTuple?> index, ColumnValue key, BTreeTuple value, bool insert)
    {
        if (insert)
            index.Put(key, value);

        foreach (BTreeNode<ColumnValue, BTreeTuple?> node in index.NodesTraverse())
        {
            if (node.PageOffset == -1)
            {
                node.Dirty = true;
                node.PageOffset = await tablespace.GetNextFreeOffset();
            }

            //Console.WriteLine("Will save node at {0}", node.PageOffset);
        }

        byte[] treeBuffer = new byte[12]; // height(4 byte) + size(4 byte) + root(4 byte)

        int pointer = 0;
        Serializator.WriteInt32(treeBuffer, index.height, ref pointer);
        Serializator.WriteInt32(treeBuffer, index.size, ref pointer);
        Serializator.WriteInt32(treeBuffer, index.root.PageOffset, ref pointer);

        await tablespace.WriteDataToPage(index.PageOffset, treeBuffer);

        //Console.WriteLine("Will save index at {0}", index.PageOffset);

        //@todo update nodes concurrently

        int dirty = 0, noDirty = 0;

        foreach (BTreeNode<ColumnValue, BTreeTuple?> node in index.NodesTraverse())
        {
            if (!node.Dirty)
            {
                noDirty++;
                //Console.WriteLine("Node {0} at {1} is not dirty", node.Id, node.PageOffset);
                continue;
            }

            byte[] nodeBuffer = new byte[8 + GetKeySizes(node)]; // keyCount(4 byte) + pageOffset(4 byte)

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

            await tablespace.WriteDataToPage(node.PageOffset, nodeBuffer);

            //Console.WriteLine("Node {0} at {1} Length={2}", node.Id, node.PageOffset, nodeBuffer.Length);
            dirty++;
        }

        //Console.WriteLine("Dirty={0} NoDirty={1}", dirty, noDirty);
    }
}
