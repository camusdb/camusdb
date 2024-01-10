
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
/// B-tree node data type
/// </summary>
/// <typeparam name="TKey"></typeparam>
/// <typeparam name="TValue"></typeparam>
public sealed class BTreeNode<TKey, TValue> where TKey : IComparable<TKey> where TValue : IComparable<TValue>
{
    private static int CurrentId = -1;

    public int Id; // unique identifier for this node

    public int KeyCount; // number of children    

    public int NumberAccesses = 0; // number of times this node has been accessed

    public int NumberReads = 0; // number of times this node has been read

    public int NumberWrites = 0; // number of times this node has been written

    public HLCTimestamp LastAccess = HLCTimestamp.Zero; // hlc timestamp of the last access to the node

    public HLCTimestamp CreatedAt = HLCTimestamp.Zero; // hlc time when the node was created

    public ObjectIdValue PageOffset; // on-disk offset

    public BTreeEntry<TKey, TValue>[] children;  // the array of children leafs

    public bool Mark = false; // mark flag tells which nodes must be released

    private readonly AsyncReaderWriterLock readerWriterLock = new(); // read/write locks prevent concurrent mutations to the node    

    /// <summary>
    /// Create a node with k children
    /// </summary>
    /// <param name="keyCount"></param>
    /// <param name="capacity"></param>
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
