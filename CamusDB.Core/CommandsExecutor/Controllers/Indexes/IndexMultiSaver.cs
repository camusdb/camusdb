
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

internal sealed class IndexMultiSaver : IndexBaseSaver
{
    private readonly IndexSaver indexSaver;

    public IndexMultiSaver(IndexSaver indexSaver)
    {
        this.indexSaver = indexSaver;
    }

    public async Task Save(BufferPoolHandler tablespace, BTreeMulti<ColumnValue> index, ColumnValue key, int value)
    {
        try
        {
            await index.WriteLock.WaitAsync();

            await SaveInternal(tablespace, index, key, value);
        }
        finally
        {
            index.WriteLock.Release();
        }
    }

    public async Task NoLockingSave(BufferPoolHandler tablespace, BTreeMulti<ColumnValue> index, ColumnValue key, int value)
    {
        await SaveInternal(tablespace, index, key, value);
    }

    private async Task SaveInternal(BufferPoolHandler tablespace, BTreeMulti<ColumnValue> index, ColumnValue key, int value)
    {
        ColumnValue columnValue = new(ColumnType.Integer, value.ToString());

        index.Put(key, value);

        foreach (BTreeMultiNode<ColumnValue> node in index.NodesTraverse())
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

        foreach (BTreeMultiNode<ColumnValue> node in index.NodesTraverse())
        {
            if (!node.Dirty)
            {
                noDirty++;
                //Console.WriteLine("Node {0} at {1} is not dirty", node.Id, node.PageOffset);
                continue;
            }

            byte[] nodeBuffer = new byte[8 + GetKeySizes(node)]; // 8 node entries + 12 int (4 byte) * nodeKeyCount

            pointer = 0;
            Serializator.WriteInt32(nodeBuffer, node.KeyCount, ref pointer);
            Serializator.WriteInt32(nodeBuffer, node.PageOffset, ref pointer);

            for (int i = 0; i < node.KeyCount; i++)
            {
                BTreeMultiEntry<ColumnValue> entry = node.children[i];

                if (entry is null)
                {
                    Serializator.WriteInt32(nodeBuffer, 0, ref pointer);
                    Serializator.WriteInt32(nodeBuffer, 0, ref pointer);
                    Serializator.WriteInt32(nodeBuffer, 0, ref pointer);
                    continue;
                }

                BTree<int>? subTree = entry.Value;

                if (subTree is null)
                {
                    SerializeKey(nodeBuffer, entry.Key, ref pointer);
                    Serializator.WriteInt32(nodeBuffer, -1, ref pointer);
                    Serializator.WriteInt32(nodeBuffer, entry.Next is not null ? entry.Next.PageOffset : -1, ref pointer);
                    continue;
                }

                //Console.WriteLine("Read Tree={0} PageOffset={1}", subTree.Id, subTree.PageOffset);

                if (subTree.PageOffset == -1)
                    subTree.PageOffset = await tablespace.GetNextFreeOffset();

                await indexSaver.Save(tablespace, subTree, value, 0, false);

                //Console.WriteLine("Write Tree={0} PageOffset={1}", subTree.Id, subTree.PageOffset);

                //Serializator.WriteInt32(nodeBuffer, entry.Key, ref pointer);
                SerializeKey(nodeBuffer, entry.Key, ref pointer);
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
