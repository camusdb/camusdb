
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.BufferPool;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Indexes;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class IndexSaver
{
    private readonly IndexUniqueSaver indexUniqueSaver;

    private readonly IndexMultiSaver indexMultiSaver;

    private readonly IndexUniqueOffsetSaver indexUniqueOffsetSaver;

    public IndexSaver()
    {
        indexUniqueSaver = new(this);
        indexMultiSaver = new(this);
        indexUniqueOffsetSaver = new(this);
    }

    public async Task Save(BufferPoolHandler tablespace, BTree<int, int?> index, int key, int value, bool insert = true)
    {
        await indexUniqueOffsetSaver.Save(tablespace, index, key, value, insert);
    }

    public async Task Save(BufferPoolHandler tablespace, BTree<ColumnValue, BTreeTuple?> index, ColumnValue key, BTreeTuple value, bool insert = true)
    {
        await indexUniqueSaver.Save(tablespace, index, key, value, insert);
    }

    public async Task Save(BufferPoolHandler tablespace, BTreeMulti<ColumnValue> index, ColumnValue key, BTreeTuple value)
    {
        await indexMultiSaver.Save(tablespace, index, key, value);
    }

    public async Task NoLockingSave(BufferPoolHandler tablespace, BTree<ColumnValue, BTreeTuple?> index, ColumnValue key, BTreeTuple value, bool insert = true)
    {
        await indexUniqueSaver.NoLockingSave(tablespace, index, key, value);
    }

    public async Task NoLockingSave(BufferPoolHandler tablespace, BTreeMulti<ColumnValue> index, ColumnValue key, BTreeTuple value)
    {
        await indexMultiSaver.NoLockingSave(tablespace, index, key, value);
    }

    public async Task Remove(BufferPoolHandler tablespace, BTree<ColumnValue, BTreeTuple?> index, ColumnValue key)
    {
        await indexUniqueSaver.Remove(tablespace, index, key);
    }
}

