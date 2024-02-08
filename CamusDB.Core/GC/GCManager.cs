
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
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Util.Comparers;
using CamusDB.Core.CommandsExecutor.Models;

using Microsoft.Extensions.Logging;
using CamusConfig = CamusDB.Core.CamusDBConfig;
using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.GC;

/// <summary>
/// The GC (garbage collector) is a process that runs in the background trying to free pages,
/// indices, and data structures that have not been accessed or used recently.
/// 
/// Page Release:
/// An LRU(Least Recently Used) algorithm is used to find the least accessed pages over a
/// period of time. These are marked as candidates to be dereferenced from the buffer pool.
/// This allows them to eventually be freed by dotnet's GC. It's quite possible that the
/// pages are already in generation 2, so they will not be freed immediately.
/// 
/// MVCC Version Release:
/// During normal operation, multiple versions can be added that end up taking up additional memory
/// and disk space.A process marks, expires, and removes these versions from the B-tree leaves
/// and deletes old versions from the disk.
///
/// Stage 1: Mark
/// Upon creation of an MVCC version, its mark flag is initially at 0 (false). During the Mark stage,
/// this flag is set for every accessible object (or those leafs that are visible by transactions) is switched to 1 (true).
/// To execute this task, a tree traversal is necessary, and utilizing a depth-first search method is effective.
/// In this scenario, each leaf is viewed as a vertex, and the process involves visiting all vertices (objects)
/// that can be accessed from a given vertex (object). This continues until all accessible vertices have been explored.
///
/// Stage 2: Sweep
/// True to its name, this stage involves "clearing out" the leafs that are not accessed recently,
/// effectively freeing up heap memory from these leafs. It removes all leafs from the heap memory
/// whose mark flag remains false, while for all accessible objects (those that can be reached),
/// the mark flag is maintained as true.
/// 
/// Subsequently, the mark flag for all reachable leafs is reset to false in preparation
/// for potentially re-running the algorithm. This sets the stage for another cycle through the
/// labeling stage, where all accessible leafs will be identified and marked again.
///
/// This method has several disadvantages, the most notable being that the entire index
/// must be suspended during collection; no mutation of the working set can be allowed. 
/// </summary>
public sealed class GCManager : IDisposable
{
    //private const int VersionRetentionPeriod = 3600000;

    private const int VersionRetentionPeriod = 30000; // 5 minutes

    private const int MaxVersionsToRemove = 32;

    private readonly BufferPoolManager bufferPool;

    private readonly HybridLogicalClock hlc;

    private readonly LC logicalClock;

    private readonly ILogger<ICamusDB> logger;

    private readonly SortedDictionary<ulong, List<BufferPage>> lruPages = new(new DescendingComparer<ulong>());

    private readonly ConcurrentDictionary<string, AsyncLazy<TableDescriptor>> tableDescriptors;

    private readonly Timer pagesReleaser;

    private readonly Timer indexReleaser;

    private bool releasing;

    public GCManager(
        BufferPoolManager bufferPool,
        HybridLogicalClock hybridLogicalClock,
        LC logicalClock,
        ConcurrentDictionary<string, AsyncLazy<TableDescriptor>> tableDescriptors,
        ILogger<ICamusDB> logger
    )
    {
        this.bufferPool = bufferPool;
        this.hlc = hybridLogicalClock;
        this.logicalClock = logicalClock;
        this.tableDescriptors = tableDescriptors;
        this.logger = logger;

        pagesReleaser = new(ReleasePages, null, TimeSpan.FromSeconds(CamusConfig.GCPagesIntervalSeconds), TimeSpan.FromSeconds(CamusConfig.GCPagesIntervalSeconds));
        indexReleaser = new(ReleaseIndexNodesAndEntries, null, TimeSpan.FromSeconds(CamusConfig.GCIndexIntervalSeconds), TimeSpan.FromSeconds(CamusConfig.GCIndexIntervalSeconds));
    }

    /// <summary>
    /// This method is called every 60 seconds and it marks old pages as candidates for release
    /// </summary>
    /// <param name="state"></param>
    private void ReleasePages(object? state)
    {
        if (releasing)
            return;

        try
        {
            for (int i = 0; i < CamusConfig.NumberBuckets; i++)
                ReleaseBucketPages(bufferPool.Buckets[i].Pages);
        }
        catch (Exception ex)
        {
            logger.LogError("ReleasePages: {Message}\n{StackTrace}", ex.Message, ex.StackTrace);
        }
        finally
        {
            releasing = false;
        }
    }

    /// <summary>
    /// Releases each individual bucket one by one
    /// </summary>    
    /// <param name="pages"></param>
    private void ReleaseBucketPages(ConcurrentDictionary<ObjectIdValue, Lazy<BufferPage>> pages)
    {
        // Counting the elements in the concurrent dictionary requires locking
        // all the buckets and can cause high contention.
        int numPages = pages.Count;

        float percent = numPages / (float)CamusConfig.BufferPoolSize;
        if (percent < CamusConfig.GCMaxPercentToStartPagesRelease)
            return;

        lruPages.Clear();

        ulong ticks = logicalClock.Increment(numPages);
        ulong threshold = Math.Max(655360, ticks - 655360); // @todo this number must be choosen based on the actual activity of the database

        int numberToFree;

        if (numPages >= CamusConfig.BufferPoolSize)
            numberToFree = (int)(CamusConfig.BufferPoolSize * CamusConfig.GCPercentToReleasePerCycleMax);
        else
            numberToFree = (int)(CamusConfig.BufferPoolSize * CamusConfig.GCPercentToReleasePerCycleMin);

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
            logger.LogInformation("Total pages freed: {NumberFreed}, remaining: {Count}", numberFreed, pages.Count);

        lruPages.Clear();
    }

    /// <summary>
    /// This method is called every 30 seconds and it marks old pages as candidates for release
    /// </summary>
    /// <param name="state"></param>
    private async void ReleaseIndexNodesAndEntries(object? state)
    {
        try
        {
            HLCTimestamp timestamp = await hlc.SendOrLocalEvent().ConfigureAwait(false) - VersionRetentionPeriod;

            foreach (KeyValuePair<string, AsyncLazy<TableDescriptor>> keyValueDescriptor in tableDescriptors)
            {
                if (!keyValueDescriptor.Value.IsStarted)
                    continue;

                TableDescriptor tableDescriptor = await keyValueDescriptor.Value.ConfigureAwait(false);

                BTree<ObjectIdValue, ObjectIdValue>? tableIndex = tableDescriptor.Rows;

                if (tableIndex is not null)
                {
                    using (await tableIndex.WriterLockAsync().ConfigureAwait(false))
                    {
                        BTreeMutationDeltas<ObjectIdValue, ObjectIdValue> deltas = new();

                        bool hasExpiredEntries = await tableIndex.Mark(timestamp, deltas).ConfigureAwait(false);

                        if (hasExpiredEntries)
                        {
                            foreach (BTreeEntry<ObjectIdValue, ObjectIdValue> entry in deltas.Entries)
                            {
                                List<ObjectIdValue>? removed = entry.RemoveExpired(timestamp, MaxVersionsToRemove);

                                if (removed is not null && removed.Count > 0)
                                {
                                    foreach (ObjectIdValue pageOffset in removed)
                                    {
                                        if (!pageOffset.IsNull())
                                            await bufferPool.DeletePage(pageOffset).ConfigureAwait(false);
                                    }
                                }
                            }
                        }
                        else
                        {
                            await tableIndex.Sweep().ConfigureAwait(false);
                        }
                    }
                }

                foreach (KeyValuePair<string, TableIndexSchema> index in tableDescriptor.Indexes)
                {
                    BPTree<CompositeColumnValue, ColumnValue, BTreeTuple> btreeIndex = index.Value.BTree;

                    BTreeMutationDeltas<CompositeColumnValue, BTreeTuple> deltas = new();

                    using (await btreeIndex.WriterLockAsync().ConfigureAwait(false))
                    {
                        bool hasExpiredEntries = await btreeIndex.Mark(timestamp, deltas).ConfigureAwait(false);

                        if (hasExpiredEntries)
                        {
                            foreach (BTreeEntry<CompositeColumnValue, BTreeTuple> entry in deltas.Entries)
                                entry.RemoveExpired(timestamp, MaxVersionsToRemove);
                        }
                        else
                        {
                            await btreeIndex.Sweep().ConfigureAwait(false);
                        }
                    }
                }
            }
        }
        catch (Exception ex)
        {
            logger.LogError("ReleaseIndexNodesAndEntries: {Message}\n{StackTrace}", ex.Message, ex.StackTrace);
        }
    }

    public void Dispose()
    {
        indexReleaser?.Dispose();
        pagesReleaser?.Dispose();
    }
}
