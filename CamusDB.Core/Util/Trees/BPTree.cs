
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Util.Time;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Util.Trees.Experimental;

namespace CamusDB.Core.Util.Trees;

/**
 * BP+Tree 
 *  
 * BPTree or Prefixed is a variation of the B+Tree that allows searches by the prefix of the composite key.
 */
public sealed class BPTree<TKey, TSubKey, TValue> : BPlusTree<TKey, TValue>
    where TKey : IComparable<TKey>, IPrefixComparable<TSubKey>
    where TValue : IComparable<TValue>
{
    /// <summary>
    /// Initializes an empty B-tree.
    /// </summary>
    /// <param name="rootOffset"></param>
    /// <param name="maxNodeCapacity"></param>
    /// <param name="reader"></param>
    public BPTree(ObjectIdValue rootOffset, IBPlusTreeNodeReader<TKey, TValue>? reader = null) : base(rootOffset, reader)
    {

    }

    /// <summary>
    /// Returns the first value associated with the given key.
    /// </summary>
    /// <param name="txType"></param>
    /// <param name="txnid"></param>
    /// <param name="key"></param>
    /// <returns>the value associated with the given key if the key is in the symbol table
    /// and {@code null} if the key is not in the symbol table</returns>
    public async IAsyncEnumerable<TValue> GetPrefix(TransactionType txType, HLCTimestamp txnid, TSubKey key)
    {
        using IDisposable readerLock = await ReaderLockAsync();

        BPlusTreeNode<TKey, TValue>? node = await root.Next.ConfigureAwait(false);

        if (root is null)
        {
            Console.WriteLine("root is null");
            yield break;
        }

        await foreach (TValue value in GetPrefixInternal(root, txType, txnid, key))
            yield return value;
    }

    private async IAsyncEnumerable<TValue> GetPrefixInternal(BPlusTreeEntry<TKey, TValue> parent, TransactionType txType, HLCTimestamp txnid, TSubKey key)
    {        
        BPlusTreeNode<TKey, TValue>? node = await parent.Next.ConfigureAwait(false);

        if (node is null)
            yield break;

        // external node
        if (node.Type == BTreeLeafType.External)
        {
            for (int i = 0; i < node.Entries.Count; i++)
            {
                BPlusTreeEntry<TKey, TValue> entry = node.Entries[i];

                //Console.WriteLine("Z {0} {1}", key, entry.Key);

                // verify if key can be seen by MVCC
                if (!entry.CanBeSeenBy(txnid))
                    continue;

                //Console.WriteLine("X {0} {1}", key, entry.Key);

                if (IsPrefixed(key, entry.Key))
                {
                    TValue? value = entry.GetValue(txType, txnid);
                    if (value is not null)
                        yield return value;
                }
            }
        }

        // internal node
        else
        {
            for (int i = 0; i < node.Entries.Count; i++)
            {
                BPlusTreeEntry<TKey, TValue> entry = node.Entries[i];

                if (IsPrefixLess(key, entry.Key) || i == (node.Entries.Count - 1))                
                {                    
                    await foreach (TValue value in GetPrefixInternal(entry, txType, txnid, key))
                        yield return value;
                }
            }
        }
    }

    private static bool IsPrefixLess(TSubKey? key1, TKey key2)
    {
        return key2.IsPrefixedBy(key1) < 0;
    }

    private static bool IsPrefixed(TSubKey? key1, TKey key2)
    {
        return key2.IsPrefixedBy(key1) == 0;
    }
}