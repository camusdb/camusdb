
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.BufferPool;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
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

    public async Task Save(SaveUniqueOffsetIndexTicket ticket)
    {
        await indexUniqueOffsetSaver.Save(ticket);
    }

    public async Task Save(SaveUniqueIndexTicket ticket)
    {
        await indexUniqueSaver.Save(ticket);
    }

    public async Task Save(BufferPoolHandler tablespace, BTreeMulti<ColumnValue> index, ColumnValue key, BTreeTuple value)
    {
        await indexMultiSaver.Save(tablespace, index, key, value);
    }

    public async Task NoLockingSave(SaveUniqueIndexTicket ticket)
    {
        await indexUniqueSaver.NoLockingSave(ticket);
    }

    public async Task NoLockingSave(BufferPoolHandler tablespace, BTreeMulti<ColumnValue> index, ColumnValue key, BTreeTuple value)
    {
        await indexMultiSaver.NoLockingSave(tablespace, index, key, value);
    }

    public async Task Remove(RemoveUniqueIndexTicket ticket)
    {
        await indexUniqueSaver.Remove(ticket);
    }

    public async Task Remove(BufferPoolHandler tablespace, BTreeMulti<ColumnValue> index, ColumnValue key)
    {
        await indexMultiSaver.Remove(tablespace, index, key);
    }
}

