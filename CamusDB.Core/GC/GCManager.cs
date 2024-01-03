
using System.Collections.Concurrent;

using CamusDB.Core.Util.Time;
using CamusDB.Core.BufferPool;
using CamusDB.Core.BufferPool.Models;
using CamusDB.Core.BufferPool.Utils;
using CamusDB.Core.Util.ObjectIds;

using CamusConfig = CamusDB.Core.CamusDBConfig;
using BConfig = CamusDB.Core.BufferPool.Models.BufferPoolConfig;

namespace CamusDB.Core.GC;

public sealed class GCManager : IDisposable 
{
    private readonly BufferPoolManager bufferPool;

    private readonly LC logicalClock;

    private readonly SortedDictionary<ulong, List<BufferPage>> lruPages = new(new DescendingComparer<ulong>());

    private readonly Timer pagesReleaser;

    private readonly Timer indexReleaser;

    public GCManager(BufferPoolManager bufferPool, LC logicalClock)
    {
        this.bufferPool = bufferPool;
        this.logicalClock = logicalClock;

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
                Console.WriteLine("Total pages freed: {0}, remaining: {1}", numberFreed, Pages.Count);

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
    private void ReleaseIndexNodesAndEntries(object? state)
    {
        try
        {

        }
        catch (Exception ex)
        {
            Console.WriteLine("ReleasePages: {0}", ex.Message, ex.StackTrace);
        }
    }

    public void Dispose()
    {
        indexReleaser.Dispose();
        pagesReleaser.Dispose();
    }
}
