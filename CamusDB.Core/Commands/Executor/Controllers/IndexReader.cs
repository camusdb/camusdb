﻿
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

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class IndexReader
{    
    private readonly IndexUniqueReader indexUniqueReader;

    private readonly IndexUniqueOffsetReader indexUniqueOffsetReader;

    public IndexReader()
    {
        indexUniqueReader = new(this);
        indexUniqueOffsetReader = new(this);
    }

    public async Task<BTree<ObjectIdValue, ObjectIdValue>> ReadOffsets(BufferPoolManager tablespace, ObjectIdValue offset)
    {
        return await indexUniqueOffsetReader.ReadOffsets(tablespace, offset);
    }

    public async Task<BTree<CompositeColumnValue, BTreeTuple>> ReadUnique(BufferPoolManager tablespace, ObjectIdValue offset)
    {
        return await indexUniqueReader.ReadUnique(tablespace, offset);
    }}
