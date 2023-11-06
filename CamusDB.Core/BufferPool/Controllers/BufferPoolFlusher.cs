﻿
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.BufferPool.Models;
using Config = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Core.BufferPool.Controllers;

/**
 * BufferPoolFlusher
 * 
 * PeriodicallyFlush runs every X ms interval and checks for dirty pages 
 * in the buffer pool and flushes changes to disk, it also flushes
 * the journal writer to disk.
 */
public sealed class BufferPoolFlusher
{
    private bool disposed = false;

    private readonly BufferPoolHandler bufferPool;

    //private readonly JournalManager journal;

    public BufferPoolFlusher(BufferPoolHandler bufferPool)
    {
        this.bufferPool = bufferPool;        
    }

    public async Task PeriodicallyFlush()
    {
        while (!disposed)
        {
            await Task.Delay(Config.FlushToDiskInterval);
            await FlushPages();
        }
    }

    public async Task FlushPages()
    {
        await Task.Yield();

        /*List<BufferPage> pagesToFlush = new();

        foreach (KeyValuePair<int, BufferPage> keyValuePair in bufferPool.Pages)
        {
            if (!keyValuePair.Value.Dirty)
                continue;
            
            keyValuePair.Value.Dirty = false;
            pagesToFlush.Add(keyValuePair.Value);
        }

        if (pagesToFlush.Count == 0)
        {
            //await journal.Writer.Flush();
            return;
        }

        Console.WriteLine(
            "Flushed DirtyPages={0} TotalPages={1} TotalMemory={2}",            
            pagesToFlush.Count,
            bufferPool.Pages.Count,
            GC.GetTotalMemory(false)
        );

        bufferPool.Flush();*/        
    }

    public void Dispose()
    {
        disposed = true;
    }
}
