
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Nito.AsyncEx;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.BufferPool.Models;

/// <summary>
/// Represents a page in the buffer pool.
/// </summary>
public sealed class BufferPage
{
    public ObjectIdValue Offset { get; }

    public Lazy<byte[]> Buffer { get; set; }

    public bool Dirty { get; set; }

    public int Accesses { get; set; }

    public ulong LastAccess { get; set; }

    private readonly AsyncReaderWriterLock readerWriterLock = new();

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="offset"></param>
    /// <param name="buffer"></param>
    public BufferPage(ObjectIdValue offset, Lazy<byte[]> buffer)
    {
        Offset = offset;
        Buffer = buffer;
    }

    /// <summary>
    /// Acquires a read lock. Multiple read locks can be acquired as long as the write lock is not.
    /// Read locks are shared.
    /// </summary>
    /// <returns></returns>
    public async Task<IDisposable> ReaderLockAsync()
    {
        return await readerWriterLock.ReaderLockAsync();
    }

    /// <summary>
    /// Acquires a write lock. Only one write lock can be acquired while other locks are not.
    /// Write locks are exclusive.
    /// </summary>
    /// <returns></returns>
    public async Task<IDisposable> WriterLockAsync()
    {
        return await readerWriterLock.WriterLockAsync();
    }
}
