
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Nito.AsyncEx;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Util.Time;

namespace CamusDB.Core.Util.Trees;

/// <summary>
/// Helper B-tree node data type
/// </summary>
/// <typeparam name="TKey"></typeparam>
/// <typeparam name="TValue"></typeparam>
public sealed class BTreeNode<TKey, TValue> where TKey : IComparable<TKey> where TValue : IComparable<TValue>
{
    private static int CurrentId = -1;

    public int Id;

    public int KeyCount; // number of children

    public HLCTimestamp CreatedAt = HLCTimestamp.Zero; // the time this node was created

    public int NumberAccesses = 0;

    public int NumberReads = 0; // number of times this node has been accessed

    public int NumberWrites = 0;

    public HLCTimestamp LastAccess;

    public ObjectIdValue PageOffset;       // on-disk offset

    public BTreeEntry<TKey, TValue>[] children;  // = new BTreeEntry<TKey, TValue>[BTreeConfig.MaxChildren];   // the array of children

    private readonly AsyncReaderWriterLock readerWriterLock = new(); // read/write locks prevent concurrent mutations to the node    

    /// <summary>
    /// Create a node with k children
    /// </summary>
    /// <param name="keyCount"></param>
    public BTreeNode(int keyCount, int capacity)
    {
        //Console.WriteLine("Allocated new node {0}", keyCount);
        Id = Interlocked.Increment(ref CurrentId);

        KeyCount = keyCount;
        children = new BTreeEntry<TKey, TValue>[capacity];
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
