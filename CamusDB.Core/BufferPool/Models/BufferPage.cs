
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.BufferPool.Models;

public sealed class BufferPage
{
    public int Offset { get; }

    public int RefCount { get; }

    public byte[] Buffer { get; set; }    
    
    public bool Dirty { get; set; }

    private int refCount = 0;

    private SemaphoreSlim? semaphore;

    private readonly object semaphoreLock = new();

    public BufferPage(int offset, byte[] buffer)
    {
        Offset = offset;
        Buffer = buffer;
    }

    public async Task LockAsync()
    {
        lock (semaphoreLock)
        {
            if (semaphore is null)
                semaphore = new(1, 1);
        }

        await semaphore.WaitAsync();
    }

    public void Unlock()
    {
        if (semaphore is not null)
            semaphore.Release();
    }

    public void IncreaseCount()
    {
        Interlocked.Increment(ref refCount);
    }

    public void DecreaseCount()
    {
        Interlocked.Decrement(ref refCount);
    }
}
