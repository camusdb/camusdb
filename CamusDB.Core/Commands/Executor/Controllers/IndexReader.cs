
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.BufferPool;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Indexes;
using CamusDB.Core.Util.Trees.Experimental;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class IndexReader
{    
    private readonly IndexUniqueReader indexReader;

    private readonly IndexUniqueOffsetReader indexOffsetReader;

    public IndexReader()
    {
        indexReader = new(this);
        indexOffsetReader = new(this);
    }

    public async Task<BPlusTree<ObjectIdValue, ObjectIdValue>> ReadOffsets(BufferPoolManager tablespace, ObjectIdValue offset)
    {
        return await indexOffsetReader.ReadOffsets(tablespace, offset);
    }

    public async Task<BPTree<CompositeColumnValue, ColumnValue, BTreeTuple>> Read(BufferPoolManager tablespace, ObjectIdValue offset)
    {
        return await indexReader.Read(tablespace, offset);
    }
}
