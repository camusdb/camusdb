
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.BufferPool;
using CamusDB.Core.Serializer;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Util.Time;
using CamusDB.Core.Util.Trees;

namespace CamusDB.Core.CommandsExecutor.Controllers.Indexes;

public sealed class IndexUniqueOffsetNodeReader : IBTreeNodeReader<ObjectIdValue, ObjectIdValue>
{
    private readonly BufferPoolManager bufferpool;

    public IndexUniqueOffsetNodeReader(BufferPoolManager bufferpool)
    {
        this.bufferpool = bufferpool;
    }

    public async Task<BTreeNode<ObjectIdValue, ObjectIdValue>?> GetNode(ObjectIdValue offset)
    {
        byte[] data = await bufferpool.GetDataFromPage(offset);
        if (data.Length == 0)
            return null;

        BTreeNode<ObjectIdValue, ObjectIdValue> node = new(-1);

        int pointer = 0;
        node.KeyCount = Serializator.ReadInt32(data, ref pointer);
        node.PageOffset = Serializator.ReadObjectId(data, ref pointer);

        //Console.WriteLine("KeyCount={0} PageOffset={1}", node.KeyCount, node.PageOffset);

        for (int i = 0; i < node.KeyCount; i++)
        {
            BTreeEntry<ObjectIdValue, ObjectIdValue> entry = new(
                key: Serializator.ReadObjectId(data, ref pointer),
                reader: this,
                next: null
            );

            HLCTimestamp timestamp = Serializator.ReadHLCTimestamp(data, ref pointer);
            ObjectIdValue value = Serializator.ReadObjectId(data, ref pointer);

            if (!timestamp.IsNull())
                entry.SetValue(
                    timestamp: timestamp,
                    commitState: BTreeCommitState.Committed,
                    value: value 
                );

            entry.NextPageOffset = Serializator.ReadObjectId(data, ref pointer);

            node.children[i] = entry;
        }

        return node;
    }
}
