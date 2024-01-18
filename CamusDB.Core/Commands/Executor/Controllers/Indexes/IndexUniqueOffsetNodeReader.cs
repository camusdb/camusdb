
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
using CamusDB.Core.Util.Trees.Experimental;

namespace CamusDB.Core.CommandsExecutor.Controllers.Indexes;

public sealed class IndexUniqueOffsetNodeReader : IBPlusTreeNodeReader<ObjectIdValue, ObjectIdValue>
{
    private readonly BufferPoolManager bufferpool;

    public IndexUniqueOffsetNodeReader(BufferPoolManager bufferpool)
    {
        this.bufferpool = bufferpool;
    }

    public async Task<BPlusTreeNode<ObjectIdValue, ObjectIdValue>?> GetNode(ObjectIdValue offset)
    {
        byte[] data = await bufferpool.GetDataFromPage(offset).ConfigureAwait(false);
        if (data.Length == 0)
            return null;

        BPlusTreeNode<ObjectIdValue, ObjectIdValue> node = new(); // new(-1, BTreeUtils.GetNodeCapacity<ObjectIdValue, ObjectIdValue>());

        int pointer = 0;
        int keyCount = Serializator.ReadInt32(data, ref pointer);
        node.PageOffset = Serializator.ReadObjectId(data, ref pointer);

        //Console.WriteLine("Node Read KeyCount={0} PageOffset={1}", node.KeyCount, node.PageOffset);

        for (int i = 0; i < keyCount; i++)
        {
            BPlusTreeEntry<ObjectIdValue, ObjectIdValue> entry = new(
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

            node.Entries.Add(entry);
        }

        return node;
    }
}
