
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Util.Time;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Util.Trees;

/**
 * BP+Tree 
 *  
 * BPTree or Prefixed is a variation of the B+Tree that allows searches by the prefix of the composite key.
 */
public sealed class BPTree<TKey, TSubKey, TValue> : BTree<TKey, TValue>
    where TKey : IComparable<TKey>, IPrefixComparable<TSubKey>
    where TValue : IComparable<TValue>

{
    /// <summary>
    /// Initializes an empty B-tree.
    /// </summary>
    /// <param name="rootOffset"></param>
    /// <param name="maxNodeCapacity"></param>
    /// <param name="reader"></param>
    public BPTree(ObjectIdValue rootOffset, IBTreeNodeReader<TKey, TValue>? reader = null) : base(rootOffset, reader)
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

        if (root is null)
        {
            Console.WriteLine("root is null");
            yield break;
        }

        await foreach (TValue value in GetPrefixInternal(root, txType, txnid, key, height))
            yield return value;
    }

    private async IAsyncEnumerable<TValue> GetPrefixInternal(BTreeNode<TKey, TValue>? node, TransactionType txType, HLCTimestamp txnid, TSubKey key, int ht)
    {
        if (node is null)
            yield break;

        BTreeEntry<TKey, TValue>[] children = node.children;

        // external node
        if (ht == 0)
        {
            for (int j = 0; j < node.KeyCount; j++)
            {
                BTreeEntry<TKey, TValue> entry = children[j];

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
            for (int j = 0; j < node.KeyCount; j++)
            {
                if (j + 1 == node.KeyCount || IsPrefixLess(key, children[j + 1].Key))
                {
                    BTreeEntry<TKey, TValue> entry = children[j];

                    await foreach (TValue value in GetPrefixInternal(await entry.Next, txType, txnid, key, ht - 1))
                        yield return value;
                }
            }
        }
    }

    private bool IsPrefixLess(TSubKey? key1, TKey key2)
    {
        return key2.IsPrefixedBy(key1) < 0;
    }

    private bool IsPrefixed(TSubKey? key1, TKey key2)
    {
        return key2.IsPrefixedBy(key1) == 0;
    }
}