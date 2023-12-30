
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.ObjectIds;
using Nito.AsyncEx;

namespace CamusDB.Core.Util.Trees;

/// <summary>
/// Helper B-tree node data type
/// </summary>
/// <typeparam name="TKey"></typeparam>
/// <typeparam name="TValue"></typeparam>
public sealed class BTreeNode<TKey, TValue>
{
    private static int CurrentId = -1;

    public int Id;

    public int KeyCount;         // number of children

    public ObjectIdValue PageOffset;       // on-disk offset

    public BTreeEntry<TKey, TValue>[] children = new BTreeEntry<TKey, TValue>[BTreeConfig.MaxChildren];   // the array of children

    private readonly AsyncReaderWriterLock readerWriterLock = new(); // read/write locks prevent concurrent mutations to the node

    /// <summary>
    /// Create a node with k children
    /// </summary>
    /// <param name="keyCount"></param>
    public BTreeNode(int keyCount)
    {
        //Console.WriteLine("Allocated new node {0}", keyCount);
        Id = Interlocked.Increment(ref CurrentId);
        KeyCount = keyCount;
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
