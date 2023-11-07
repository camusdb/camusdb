
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Nito.AsyncEx;

namespace CamusDB.Core.BufferPool.Models;

public sealed class BufferPage
{
    public int Offset { get; }

    public Lazy<byte[]> Buffer { get; set; }

    public bool Dirty { get; set; }

    private readonly AsyncReaderWriterLock readerWriterLock = new();

    public BufferPage(int offset, Lazy<byte[]> buffer)
    {
        Offset = offset;
        Buffer = buffer;
    }

    public async Task<IDisposable> ReaderLockAsync()
    {
        return await readerWriterLock.ReaderLockAsync();
    }

    public async Task<IDisposable> WriterLockAsync()
    {
        return await readerWriterLock.WriterLockAsync();
    }
}
