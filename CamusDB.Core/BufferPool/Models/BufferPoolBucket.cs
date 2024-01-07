
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.ObjectIds;
using System.Collections.Concurrent;

namespace CamusDB.Core.BufferPool.Models;

/// <summary>
/// Buckets hold a single concurrent dictionary.
/// </summary>
public sealed class BufferPoolBucket
{
    private readonly ConcurrentDictionary<ObjectIdValue, Lazy<BufferPage>> pages = new();

    public int NumberPages => pages.Count;

    public ConcurrentDictionary<ObjectIdValue, Lazy<BufferPage>> Pages => pages;

    internal void Clear()
    {
        pages.Clear();
    }
}
