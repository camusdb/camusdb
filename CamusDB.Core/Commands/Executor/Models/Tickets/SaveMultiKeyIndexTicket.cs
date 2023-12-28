
using CamusDB.Core.BufferPool;
using CamusDB.Core.BufferPool.Models;
using CamusDB.Core.Util.Trees;

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct SaveMultiKeyIndexTicket
{
    public BufferPoolHandler Tablespace { get; }

    public BTreeMulti<ColumnValue> Index { get; }

    public ColumnValue MultiKeyValue { get; }

    public BTreeTuple RowTuple { get; }

    public List<IDisposable> Locks { get; }

    public List<BufferPageOperation> ModifiedPages { get; }

    public SaveMultiKeyIndexTicket(
        BufferPoolHandler tablespace,
        BTreeMulti<ColumnValue> multiIndex,
        ColumnValue multiKeyValue,
        BTreeTuple rowTuple,
        List<IDisposable> locks,
        List<BufferPageOperation> modifiedPages
    )
    {
        Tablespace = tablespace;
        Index = multiIndex;
        MultiKeyValue = multiKeyValue;
        RowTuple = rowTuple;
        Locks = locks;
        ModifiedPages = modifiedPages;
    }
}

