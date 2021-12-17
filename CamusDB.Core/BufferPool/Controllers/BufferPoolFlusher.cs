
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Journal;
using CamusDB.Core.Journal.Models;
using CamusDB.Core.BufferPool.Models;
using CamusDB.Core.Journal.Models.Logs;

namespace CamusDB.Core.BufferPool.Controllers;

public sealed class BufferPoolFlusher
{
    private bool disposed = false;

    private readonly BufferPoolHandler bufferPool;

    private readonly JournalManager journal;

    public BufferPoolFlusher(BufferPoolHandler bufferPool, JournalManager journal)
    {
        this.bufferPool = bufferPool;
        this.journal = journal;
    }

    public async Task PeriodicallyFlush()
    {
        while (!disposed)
        {
            await Task.Delay(1000);
            await FlushPages();
        }
    }

    public async Task FlushPages()
    {
        List<BufferPage> pagesToFlush = new();

        foreach (KeyValuePair<int, BufferPage> keyValuePair in bufferPool.Pages)
        {
            if (!keyValuePair.Value.Dirty)
                continue;
            
            keyValuePair.Value.Dirty = false;
            pagesToFlush.Add(keyValuePair.Value);
        }

        if (pagesToFlush.Count == 0)
        {
            await journal.Writer.Flush();
            return;
        }

        Console.WriteLine("Flushed {0} dirty pages", pagesToFlush.Count);

        bufferPool.Flush();

        await journal.Writer.Append(
            JournalFailureTypes.None,
            new FlushedPagesLog(0)
        );

        await journal.Writer.Flush();
    }

    public void Dispose()
    {
        disposed = true;
    }
}
