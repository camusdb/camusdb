
using CamusDB.Library.BufferPool;
using CamusDB.Library.Serializer;
using CamusDB.Library.Util.Trees;

namespace CamusDB.Library.CommandsExecutor.Controllers;

public sealed class IndexReader
{
    public async Task<BTree> Read(BufferPoolHandler tablespace, int offset)
    {
        //Console.WriteLine("***");

        BTree index = new(offset);

        byte[] data = await tablespace.GetDataFromPage(offset);
        if (data.Length > 0)
        {
            int pointer = 0;

            index.height = Serializator.ReadInt32(data, ref pointer);
            index.n = Serializator.ReadInt32(data, ref pointer);

            int rootPageOffset = Serializator.ReadInt32(data, ref pointer);

            Console.WriteLine("NumberNodes={0} PageOffset={1} RootOffset={2}", index.n, index.PageOffset, rootPageOffset);

            if (rootPageOffset > -1)
            {
                Node? node = await GetNode(tablespace, rootPageOffset);
                if (node is not null)
                    index.root = node;
            }
        }

        /*foreach (Entry entry in index.EntriesTraverse())
        {
            Console.WriteLine("Index RowId={0} PageOffset={1}", entry.Key, entry.Value);
        }*/

        //Console.WriteLine("***");

        return index;
    }

    private async Task<Node?> GetNode(BufferPoolHandler tablespace, int offset)
    {
        byte[] data = await tablespace.GetDataFromPage(offset);
        if (data.Length == 0)
            return null;

        Node node = new(-1);

        int pointer = 0;
        node.KeyCount = Serializator.ReadInt32(data, ref pointer);
        node.PageOffset = Serializator.ReadInt32(data, ref pointer);

        //Console.WriteLine("KeyCount={0} PageOffset={1}", node.KeyCount, node.PageOffset);

        for (int i = 0; i < node.KeyCount; i++)
        {
            Entry entry = new(0, null, null);

            entry.Key = Serializator.ReadInt32(data, ref pointer);
            entry.Value = Serializator.ReadInt32(data, ref pointer);

            int nextPageOffset = Serializator.ReadInt32(data, ref pointer);
            //Console.WriteLine("Children={0} Key={1} Value={2} NextOffset={3}", i, entry.Key, entry.Value, nextPageOffset);

            if (nextPageOffset > -1)
                entry.Next = await GetNode(tablespace, nextPageOffset);
                    
            node.children[i] = entry;
        }

        return node;
    }
}
