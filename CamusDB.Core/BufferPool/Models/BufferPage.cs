﻿
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

    public byte[] Buffer { get; set; }

    public DateTime LastAccessTime { get; set; }
    
    public bool Dirty { get; set; }

    public SemaphoreSlim Semaphore { get; } = new(1, 1);

    public BufferPage(int offset, byte[] buffer)
    {
        Offset = offset;
        Buffer = buffer;
    }
}
