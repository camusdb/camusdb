﻿
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;

using CamusDB.Core.Storage;
using CamusDB.Core.Serializer;
using CamusDB.Core.Util.Hashes;
using CamusDB.Core.BufferPool.Models;
using CamusDB.Core.Util.Time;
using CamusDB.Core.Util.ObjectIds;

using CamusConfig = CamusDB.Core.CamusDBConfig;
using BConfig = CamusDB.Core.BufferPool.Models.BufferPoolConfig;

namespace CamusDB.Core.BufferPool;

/*
 * BufferPoolHandler
 *
 * A buffer pool is an area of main memory that has been allocated by the database manager
 * for the purpose of caching table and index data as it is read from storage.
 *
 * When a row of data in a table is first accessed, the database manager places the page that
 * contains that data into a buffer pool. Pages stay in the buffer pool until the database
 * is shut down or until the space occupied by the page is required by another page.
 *
 * Pages are loaded on demand and are organized in the following layout:
 *
 * +--------------------+
 * | version (2 bytes)  |
 * +--------------------+---------------------------------+
 * | checksum (4 bytes unsigned integer)                  |
 * +----------+----------+----------+---------------------+
 * | last wrote journal seq (4 bytes unsigned integer)    |
 * +----------+----------+----------+----------------------------------------+
 * | next page offset (12 bytes integer)                                     |
 * +----------+----------+----------+----------------------------------------+
 * | data length (4 bytes integer)                        |
 * +----------+----------+----------+---------------------+
 */
public sealed class BufferPoolHandler : IDisposable
{
    private readonly StorageManager storage;

    private readonly LC logicalClock;

    private readonly SortedDictionary<ulong, ObjectIdValue> lruPages = new();

    private readonly ConcurrentDictionary<ObjectIdValue, Lazy<BufferPage>> pages = new();

    private readonly Timer releaser;

    public int NumberPages => pages.Count;

    public ConcurrentDictionary<ObjectIdValue, Lazy<BufferPage>> Pages => pages;

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="storage"></param>
    /// <param name="logicalClock"></param>
    public BufferPoolHandler(StorageManager storage, LC logicalClock)
    {
        this.storage = storage;
        this.logicalClock = logicalClock;

        releaser = new(ReleasePages, null, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5));
    }

    /// <summary>
    /// This method is called every 5 seconds and it marks old pages as candidates for release
    /// </summary>
    /// <param name="state"></param>
    private void ReleasePages(object? state)
    {
        float percent = Pages.Count / (float)CamusConfig.BufferPoolSize;
        if (percent < 0.8)
            return;

        ulong ticks = logicalClock.GetTicks();
        int numberToFree = (int)(CamusConfig.BufferPoolSize * (1 - (percent > 0.8 ? 0.8 : percent)));

        lruPages.Clear();

        foreach (KeyValuePair<ObjectIdValue, Lazy<BufferPage>> keyValuePair in Pages)
        {
            Lazy<BufferPage> page = keyValuePair.Value;

            if (!page.IsValueCreated)
                continue;

            if (page.Value.LastAccess > (ticks - 65536)) // @todo this number must be choosen based on the actual activity of the database
                continue;

            if (lruPages.Count > (numberToFree * 2)) // fill the red-black tree with twice the pages to release
                break;

            lruPages.Add(page.Value.LastAccess, keyValuePair.Key);
        }

        int numberFreed = 0;

        foreach (KeyValuePair<ulong, ObjectIdValue> keyValue in lruPages)
        {
            Pages.TryRemove(keyValue.Value, out _);

            numberFreed++;

            if (numberFreed > numberToFree)
                break;
        }

        Console.WriteLine("Total pages freed: {0}, remaining: {1}", numberFreed, Pages.Count);
    }

    private BufferPage LoadPage(ObjectIdValue offset)
    {
        return new BufferPage(offset, new Lazy<byte[]>(() => ReadFromDisk(offset)));
    }

    private byte[] ReadFromDisk(ObjectIdValue offset)
    {
        return storage.Read(offset);
    }

    // Load a page reading its contents
    public BufferPage ReadPage(ObjectIdValue offset)
    {
        Lazy<BufferPage> lazyBufferPage = pages.GetOrAdd(offset, (x) => new Lazy<BufferPage>(() => LoadPage(offset)));
        BufferPage bufferPage = lazyBufferPage.Value;
        bufferPage.Accesses++;
        bufferPage.LastAccess = logicalClock.Increment();
        return bufferPage;
    }

    public async Task<(int, List<BufferPage>, List<IDisposable>)> GetPagesToRead(ObjectIdValue offset)
    {
        int length = 0;
        List<BufferPage> pages = new();
        List<IDisposable> disposables = new();

        do
        {
            BufferPage memoryPage = ReadPage(offset);
            pages.Add(memoryPage);

            disposables.Add(await memoryPage.ReaderLockAsync());

            byte[] pageBuffer = memoryPage.Buffer.Value; // get a pointer to the buffer to get a consistent read

            int pointer = BConfig.LengthOffset;
            length += Serializator.ReadInt32(pageBuffer, ref pointer);

            pointer = BConfig.NextPageOffset;
            offset = Serializator.ReadObjectId(pageBuffer, ref pointer);

        } while (!offset.IsNull());

        return (length, pages, disposables);
    }

    public async Task<(int, List<BufferPage>, List<IDisposable>)> GetPagesToWrite(ObjectIdValue offset)
    {
        int length = 0;
        List<BufferPage> pages = new();
        List<IDisposable> disposables = new();

        do
        {
            BufferPage memoryPage = ReadPage(offset);
            pages.Add(memoryPage);

            disposables.Add(await memoryPage.WriterLockAsync());

            byte[] pageBuffer = memoryPage.Buffer.Value; // get a pointer to the buffer to get a consistent read

            int pointer = BConfig.LengthOffset;
            length += Serializator.ReadInt32(pageBuffer, ref pointer);

            pointer = BConfig.NextPageOffset;
            offset = Serializator.ReadObjectId(pageBuffer, ref pointer);

        } while (!offset.IsNull());

        return (length, pages, disposables);
    }

    public async Task<byte[]> GetDataFromPage(ObjectIdValue offset)
    {
        (int length, List<BufferPage> pages, List<IDisposable> disposables) = await GetPagesToRead(offset);

        try
        {
            return GetDataFromPageDirect(length, pages);
        }
        finally
        {
            foreach (IDisposable disposable in disposables)
                disposable.Dispose();
        }
    }

    public byte[] GetDataFromPageDirect(int length, List<BufferPage> pages)
    {
        if (length == 0)
            return Array.Empty<byte>();

        ObjectIdValue offset;
        byte[] data = new byte[length];

        uint checksum;
        int bufferOffset = 0;

        foreach (BufferPage memoryPage in pages)
        {
            byte[] pageBuffer = memoryPage.Buffer.Value; // get a pointer to the buffer to get a consistent read

            int pointer = BConfig.LengthOffset;
            int dataLength = Serializator.ReadInt32(pageBuffer, ref pointer);

            if (dataLength > (pageBuffer.Length - BConfig.DataOffset))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidPageLength,
                    "Page has an invalid data length"
                );

            pointer = BConfig.NextPageOffset;
            offset = Serializator.ReadObjectId(pageBuffer, ref pointer);

            pointer = BConfig.ChecksumOffset;
            checksum = Serializator.ReadUInt32(pageBuffer, ref pointer);

            /*if (checksum > 0) // check checksum only if available
            {
                byte[] pageData = new byte[dataLength]; // @todo avoid this allocation
                Buffer.BlockCopy(pageBuffer, BConfig.DataOffset, pageData, 0, dataLength);

                uint dataChecksum = XXHash.Compute(pageData, 0, dataLength);

                if (dataChecksum != checksum)
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidPageChecksum,
                        "Page has an invalid data checksum"
                    );
            }*/

            Buffer.BlockCopy(pageBuffer, BConfig.DataOffset, data, bufferOffset, dataLength);
            bufferOffset += dataLength;
        }

        return data;
    }

    public void FlushPage(BufferPage memoryPage)
    {
        storage.Write(memoryPage.Offset, memoryPage.Buffer.Value);
    }

    public static int WritePageHeader(byte[] pageBuffer, uint checksum, uint lastSequence, ObjectIdValue nextPage, int length)
    {
        int pointer = 0;
        Serializator.WriteInt16(pageBuffer, CamusConfig.PageLayoutVersion, ref pointer);  // layout version (2 byte integer)        
        Serializator.WriteUInt32(pageBuffer, checksum, ref pointer);                 // checksum (4 bytes unsigned integer)        
        Serializator.WriteUInt32(pageBuffer, lastSequence, ref pointer);             // lastWroteSequence (4 bytes unsigned integer)        
        Serializator.WriteObjectId(pageBuffer, nextPage, ref pointer);               // next page (12 bytes objectid)        
        Serializator.WriteInt32(pageBuffer, length, ref pointer);                    // data length (4 bytes integer)                
        return pointer;
    }

    public ObjectIdValue GetNextFreeOffset()
    {
        return ObjectIdGenerator.Generate();
    }

    public ObjectIdValue GetNextRowId()
    {
        return ObjectIdGenerator.Generate();
    }

    public async Task<ObjectIdValue> WriteDataToFreePage(byte[] data)
    {
        ObjectIdValue freeOffset = GetNextFreeOffset();
        await WriteDataToPage(freeOffset, 0, data);
        return freeOffset;
    }

    public async Task WriteDataToPage(ObjectIdValue offset, uint sequence, byte[] data, int startOffset = 0)
    {
        if (offset.IsNull())
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidPageOffset,
                "Invalid page offset"
            );

        if (startOffset < 0)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidPageOffset,
                "Start offset can't be negative"
            );

        BufferPage page = ReadPage(offset);

        using IDisposable writeLock = await page.WriterLockAsync();

        int length;
        ObjectIdValue nextPage = new(0, 0, 0);

        // Calculate remaining data length less the page's header
        if (((data.Length - startOffset) + BConfig.DataOffset) < CamusConfig.PageSize)
            length = data.Length - startOffset;
        else
            length = CamusConfig.PageSize - BConfig.DataOffset;

        int remaining = (data.Length - startOffset) - length;

        if (remaining > 0)
            nextPage = GetNextFreeOffset();

        // Create a buffer to calculate the checksum
        byte[] pageData = new byte[length];
        Buffer.BlockCopy(data, startOffset, pageData, 0, length);

        uint checksum = XXHash.Compute(pageData, 0, length);

        // Create a new page buffer to replace the existing one
        byte[] pageBuffer = new byte[CamusConfig.PageSize];

        int pointer = WritePageHeader(pageBuffer, checksum, sequence, nextPage, length);

        if (!nextPage.IsNull() && nextPage == offset)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Cannot write recursively to the same page"
            );

        Buffer.BlockCopy(data, startOffset, pageBuffer, pointer, length);
        storage.Write(offset, pageBuffer);

        // Replace buffer, this helps to get readers consistent copies
        page.Dirty = true;
        page.Buffer = new Lazy<byte[]>(pageBuffer);

        /*Console.WriteLine(
            "Wrote {0} bytes to page {1}/{2} from buffer staring at {3}, remaining {4}, next page {5}",
            length,
            offset,
            offset,
            startOffset,
            remaining,
            nextPage
        );*/

        if (!nextPage.IsNull())
            await WriteDataToPage(nextPage, sequence, data, startOffset + length);
    }

    public void ApplyPageOperations(List<BufferPageOperation> modifiedPages)
    {
        Console.WriteLine("Wrote {0} pages in the batch", modifiedPages.Count);

        storage.WriteBatch(modifiedPages);
    }

    public void WriteDataToPageBatch(List<BufferPageOperation> pagesToWrite, ObjectIdValue offset, uint sequence, byte[] data, int startOffset = 0)
    {
        //Console.WriteLine(offset);

        if (offset.IsNull())
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidPageOffset,
                "Invalid page offset"
            );

        if (startOffset < 0)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidPageOffset,
                "Start offset can't be negative"
            );

        BufferPage page = ReadPage(offset);

        //using IDisposable writeLock = await page.WriterLockAsync();

        int length;
        ObjectIdValue nextPage = new(0, 0, 0);

        // Calculate remaining data length less the page's header
        if (((data.Length - startOffset) + BConfig.DataOffset) < CamusConfig.PageSize)
            length = data.Length - startOffset;
        else
            length = CamusConfig.PageSize - BConfig.DataOffset;

        int remaining = (data.Length - startOffset) - length;

        if (remaining > 0)
            nextPage = GetNextFreeOffset();

        // Create a buffer to calculate the checksum
        byte[] pageData = new byte[length];
        Buffer.BlockCopy(data, startOffset, pageData, 0, length);

        uint checksum = XXHash.Compute(pageData, 0, length);

        // Create a new page buffer to replace the existing one
        byte[] pageBuffer = new byte[CamusConfig.PageSize];

        int pointer = WritePageHeader(pageBuffer, checksum, sequence, nextPage, length);

        if (!nextPage.IsNull() && nextPage == offset)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Cannot write recursively to the same page"
            );

        Buffer.BlockCopy(data, startOffset, pageBuffer, pointer, length);
        //storage.Write(Config.PageSize * offset, pageBuffer);
        pagesToWrite.Add(new BufferPageOperation(BufferPageOperationType.InsertOrUpdate, offset, 0, pageBuffer));

        // Replace buffer, this helps to get readers consistent copies
        page.Dirty = true;
        page.Buffer = new Lazy<byte[]>(pageBuffer);

        /*Console.WriteLine(
            "Wrote {0} bytes to page {1}/{2} from buffer staring at {3}, remaining {4}, next page {5}",
            length,
            offset,
            Config.PageSize * offset,
            startOffset,
            remaining,
            nextPage
        );*/

        if (!nextPage.IsNull())
            WriteDataToPageBatch(pagesToWrite, nextPage, sequence, data, startOffset + length);
    }

    public async Task DeletePage(ObjectIdValue offset)
    {
        if (offset.IsNull())
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidPageOffset,
                "Invalid page offset"
            );

        (int length, List<BufferPage> pagesChain, List<IDisposable> disposables) = await GetPagesToWrite(offset);

        try
        {
            List<BufferPageOperation> pagesToDelete = new(pagesChain.Count);

            foreach (BufferPage memoryPage in pagesChain)
            {
                pagesToDelete.Add(new BufferPageOperation(BufferPageOperationType.Delete, memoryPage.Offset, 0, Array.Empty<byte>()));
                pages.TryRemove(memoryPage.Offset, out _);
            }

            storage.WriteBatch(pagesToDelete);
        }
        finally
        {
            foreach (IDisposable disposable in disposables)
                disposable.Dispose();
        }

        Console.WriteLine("Removed {0} pages from disk, length={1}", pagesChain.Count, length);
    }

    public void Flush()
    {
        // storage.Flush();
    }

    public void Clear()
    {
        pages.Clear();
    }

    public void Dispose()
    {
        //offsetUpdaterWriter.Complete();
        releaser?.Dispose();
    }
}