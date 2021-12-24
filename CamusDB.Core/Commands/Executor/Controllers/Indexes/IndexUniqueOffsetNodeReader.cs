
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.BufferPool;
using CamusDB.Core.Serializer;
using CamusDB.Core.Util.Trees;

namespace CamusDB.Core.CommandsExecutor.Controllers.Indexes;

public sealed class IndexUniqueOffsetNodeReader : IBTreeNodeReader<int, int?>
{
    private readonly BufferPoolHandler bufferpool;

    public IndexUniqueOffsetNodeReader(BufferPoolHandler bufferpool)
    {
        this.bufferpool = bufferpool;
    }

    public async Task<BTreeNode<int, int?>?> GetNode(int offset)
    {
        byte[] data = await bufferpool.GetDataFromPage(offset);
        if (data.Length == 0)
            return null;

        BTreeNode<int, int?> node = new(-1);

        int pointer = 0;
        node.KeyCount = Serializator.ReadInt32(data, ref pointer);
        node.PageOffset = Serializator.ReadInt32(data, ref pointer);

        //Console.WriteLine("KeyCount={0} PageOffset={1}", node.KeyCount, node.PageOffset);

        for (int i = 0; i < node.KeyCount; i++)
        {
            BTreeEntry<int, int?> entry = new(
                key: Serializator.ReadInt32(data, ref pointer),
                value: Serializator.ReadInt32(data, ref pointer),
                next: null
            );

            entry.NextPageOffset = Serializator.ReadInt32(data, ref pointer);

            node.children[i] = entry;
        }

        return node;
    }
}
