
using CamusDB.Library.BufferPool;
using CamusDB.Library.Serializer;
using CamusDB.Library.Util.Trees;

namespace CamusDB.Library.CommandsExecutor.Controllers;

public sealed class IndexReader
{
    public async Task<BTree> Read(BufferPoolHandler tablespace, int offset)
    {
        BTree index = new(offset);

        byte[] data = await tablespace.GetDataFromPage(offset);
        if (data.Length > 0)
        {
            //Console.WriteLine(data.Length);

            int pointer = 0;

            index.height = Serializator.ReadInt32(data, ref pointer);
            index.n = Serializator.ReadInt32(data, ref pointer);
            index.PageOffset = Serializator.ReadInt32(data, ref pointer);

            Node? node = await GetNode(tablespace, index.PageOffset);
            if (node is not null)
                index.root = node;
        }

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

        Console.WriteLine("KeyCount={0}", node.KeyCount);

        for (int i = 0; i < node.KeyCount; i++)
        {
            Entry entry = new(0, null, null);

            entry.Key = Serializator.ReadInt32(data, ref pointer);
            entry.Value = Serializator.ReadInt32(data, ref pointer);

            int nextPageOffset = Serializator.ReadInt32(data, ref pointer);
            Console.WriteLine(nextPageOffset);

            Console.WriteLine("Key={0} Value={1}", entry.Key, entry.Value);

            node.children[i] = entry;
        }

        return node;
    }
}
