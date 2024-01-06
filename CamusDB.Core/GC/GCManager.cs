
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Nito.AsyncEx;
using System.Collections.Concurrent;

using CamusDB.Core.Util.Time;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.BufferPool;
using CamusDB.Core.BufferPool.Models;
using CamusDB.Core.BufferPool.Utils;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

using CamusConfig = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Core.GC;

public sealed class GCManager : IDisposable
{
    //private const int VersionRetentionPeriod = 3600000;

    private const int VersionRetentionPeriod = 30000;

    private readonly BufferPoolManager bufferPool;

    private readonly HybridLogicalClock hlc;

    private readonly LC logicalClock;

    private readonly SortedDictionary<ulong, List<BufferPage>> lruPages = new(new DescendingComparer<ulong>());

    private readonly ConcurrentDictionary<string, AsyncLazy<TableDescriptor>> tableDescriptors;

    private readonly Timer pagesReleaser;

    private readonly Timer indexReleaser;

    public GCManager(BufferPoolManager bufferPool, HybridLogicalClock hybridLogicalClock, LC logicalClock, ConcurrentDictionary<string, AsyncLazy<TableDescriptor>> tableDescriptors)
    {
        this.bufferPool = bufferPool;
        this.hlc = hybridLogicalClock;
        this.logicalClock = logicalClock;
        this.tableDescriptors = tableDescriptors;

        pagesReleaser = new(ReleasePages, null, TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(30));
        indexReleaser = new(ReleaseIndexNodesAndEntries, null, TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(30));
    }

    /// <summary>
    /// This method is called every 30 seconds and it marks old pages as candidates for release
    /// </summary>
    /// <param name="state"></param>
    private void ReleasePages(object? state)
    {
        ConcurrentDictionary<ObjectIdValue, Lazy<BufferPage>> pages = bufferPool.Pages;

        try
        {
            float percent = pages.Count / (float)CamusConfig.BufferPoolSize;
            if (percent < 0.8)
                return;

            ulong ticks = logicalClock.Increment(pages.Count);
            ulong threshold = Math.Max(655360, ticks - 655360); // @todo this number must be choosen based on the actual activity of the database
            int numberToFree = (int)(CamusConfig.BufferPoolSize * (1 - (percent > 0.8 ? 0.8 : percent)));

            foreach (KeyValuePair<ObjectIdValue, Lazy<BufferPage>> keyValuePair in pages)
            {
                Lazy<BufferPage> page = keyValuePair.Value;

                if (!page.IsValueCreated)
                    continue;

                if (page.Value.CreatedAt > threshold) // @todo this number must be choosen based on the actual activity of the database
                    continue;

                if (lruPages.TryGetValue(page.Value.Accesses, out List<BufferPage>? pagesAccesses))
                    pagesAccesses.Add(page.Value);
                else
                    lruPages.TryAdd(page.Value.Accesses, new() { page.Value });
            }

            List<ObjectIdValue> pagesToRelease = new(numberToFree);

            foreach (KeyValuePair<ulong, List<BufferPage>> keyValue in lruPages)
            {
                foreach (BufferPage page in keyValue.Value)
                {
                    if (page.LastAccess > threshold) // @todo this number must be choosen based on the actual activity of the database
                        continue;

                    pagesToRelease.Add(page.Offset);
                }

                if (pagesToRelease.Count >= numberToFree)
                    break;
            }

            int numberFreed = 0;

            foreach (ObjectIdValue offset in pagesToRelease)
            {
                pages.TryRemove(offset, out _);

                numberFreed++;

                if (numberFreed > numberToFree)
                    break;
            }

            if (numberFreed > 0)
                Console.WriteLine("Total pages freed: {0}, remaining: {1}", numberFreed, pages.Count);

            lruPages.Clear();
        }
        catch (Exception ex)
        {
            Console.WriteLine("ReleasePages: {0}", ex.Message, ex.StackTrace);
        }
    }

    /// <summary>
    /// This method is called every 30 seconds and it marks old pages as candidates for release
    /// </summary>
    /// <param name="state"></param>
    private async void ReleaseIndexNodesAndEntries(object? state)
    {
        try
        {
            HLCTimestamp timestamp = await hlc.SendOrLocalEvent() - VersionRetentionPeriod;

            foreach (KeyValuePair<string, AsyncLazy<TableDescriptor>> keyValueDescriptor in tableDescriptors)
            {
                if (!keyValueDescriptor.Value.IsStarted)
                    continue;

                TableDescriptor tableDescriptor = await keyValueDescriptor.Value;

                foreach (KeyValuePair<string, TableIndexSchema> index in tableDescriptor.Indexes)
                {
                    BTree<ColumnValue, BTreeTuple?>? xindex = index.Value.UniqueRows;
                    if (xindex is not null)
                    {
                        BTreeMutationDeltas<ColumnValue, BTreeTuple?> deltas = new();

                        await xindex.Mark(timestamp, deltas);

                        if (deltas.Entries.Count == 0)
                            continue;

                        foreach (BTreeEntry<ColumnValue, BTreeTuple?> entry in deltas.Entries)
                            entry.RemoveExpired(timestamp);
                    }
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("ReleaseIndexNodesAndEntries: {0}", ex.Message, ex.StackTrace);
        }
    }

    public void Dispose()
    {
        indexReleaser?.Dispose();
        pagesReleaser?.Dispose();
    }
}
