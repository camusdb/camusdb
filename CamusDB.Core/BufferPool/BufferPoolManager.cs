
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Storage;
using CamusDB.Core.Serializer;
using CamusDB.Core.Util.Hashes;
using CamusDB.Core.BufferPool.Models;
using CamusDB.Core.Util.Time;
using CamusDB.Core.Util.ObjectIds;
using Microsoft.Extensions.Logging;

using CamusConfig = CamusDB.Core.CamusDBConfig;
using BConfig = CamusDB.Core.BufferPool.Models.BufferPoolConfig;

namespace CamusDB.Core.BufferPool;

/*
 * BufferPoolManager
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
 * 
 * Pages are stored in concurrent dictionary structures. Due to the way concurrency is controlled, 
 * locks can generate high contention, slowing down the modification of pages. 
 * For this reason, the concept of buckets is introduced. Each bucket contains a concurrent dictionary.
 * 
 * A separate process called GC (Garbage Collection) attempts to free infrequently 
 * used pages with the goal of reducing the memory footprint.
 */
public sealed class BufferPoolManager
{
    private readonly StorageManager storage;

    private readonly LC logicalClock;

    private readonly ILogger<ICamusDB> logger;

    private readonly BufferPoolBucket[] buckets = new BufferPoolBucket[CamusConfig.NumberBuckets];

    /// <summary>
    /// Returns a reference to the array of buckets
    /// </summary>
    public BufferPoolBucket[] Buckets => buckets;

    /// <summary>
    /// Returns the number of pages in all the buckets (probably inconsistent)
    /// </summary>
    public int NumberPages
    {
        get
        {
            int sum = 0;
            for (int i = 0; i < buckets.Length; i++)
                sum += buckets[i].NumberPages;
            return sum;
        }
    }

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="storage"></param>
    /// <param name="logicalClock"></param>
    public BufferPoolManager(StorageManager storage, LC logicalClock, ILogger<ICamusDB> logger)
    {
        this.storage = storage;
        this.logicalClock = logicalClock;
        this.logger = logger;

        for (int i = 0; i < buckets.Length; i++)
            buckets[i] = new BufferPoolBucket();
    }

    private BufferPage LoadPage(ObjectIdValue offset)
    {
        return new BufferPage(offset, new Lazy<byte[]>(() => ReadFromDisk(offset)), logicalClock.Increment());
    }

    private byte[] ReadFromDisk(ObjectIdValue offset)
    {
        return storage.Read(offset);
    }

    /// <summary>
    /// Loads the content of a page.
    /// Pages are organized into buckets, and the buckets have a concurrent dictionary.
    /// Using the hash, the pages are distributed among the buckets.
    /// </summary>
    /// <param name="offset"></param>
    /// <returns></returns>
    public BufferPage ReadPage(ObjectIdValue offset)
    {
        uint hashCode = (uint)offset.GetHashCode();
        BufferPoolBucket poolBucket = buckets[hashCode % CamusConfig.NumberBuckets];

        Lazy<BufferPage> lazyBufferPage = poolBucket.Pages.GetOrAdd(offset, (_) => new Lazy<BufferPage>(() => LoadPage(offset)));

        BufferPage bufferPage = lazyBufferPage.Value;
        bufferPage.Accesses++;
        bufferPage.LastAccess = logicalClock.Increment();

        return bufferPage;
    }

    /// <summary>
    /// Returns the sequence of pages where a record is stored and its read locks.    
    /// NextPageOffset stores the address of the next page that must be read to load the buffer into memory.
    /// </summary>
    /// <param name="offset"></param>
    /// <returns></returns>
    public async Task<(int, List<BufferPage>, List<IDisposable>)> GetPagesToRead(ObjectIdValue offset)
    {
        int length = 0;
        List<BufferPage> pages = new();
        List<IDisposable> disposables = new();

        do
        {
            BufferPage memoryPage = ReadPage(offset);
            pages.Add(memoryPage);

            disposables.Add(await memoryPage.ReaderLockAsync().ConfigureAwait(false));

            byte[] pageBuffer = memoryPage.Buffer.Value; // get a pointer to the buffer to get a consistent read

            int pointer = BConfig.LengthOffset;
            length += Serializator.ReadInt32(pageBuffer, ref pointer);

            pointer = BConfig.NextPageOffset;
            offset = Serializator.ReadObjectId(pageBuffer, ref pointer);

        } while (!offset.IsNull());

        return (length, pages, disposables);
    }

    /// <summary>
    /// Returns the sequence of pages where a record/row is stored and its write locks.
    ///
    /// Obtaining write locks is necessary to prevent the contents of the pages from being modified
    /// in parallel, causing consistency problems in reading.
    /// </summary>
    /// <param name="offset"></param>
    /// <returns></returns>
    public async Task<(int, List<BufferPage>, List<IDisposable>)> GetPagesToWrite(ObjectIdValue offset)
    {
        int length = 0;
        List<BufferPage> pages = new();
        List<IDisposable> disposables = new();

        do
        {
            BufferPage memoryPage = ReadPage(offset);
            pages.Add(memoryPage);

            disposables.Add(await memoryPage.WriterLockAsync().ConfigureAwait(false));

            byte[] pageBuffer = memoryPage.Buffer.Value; // get a pointer to the buffer to get a consistent read

            int pointer = BConfig.LengthOffset;
            length += Serializator.ReadInt32(pageBuffer, ref pointer);

            pointer = BConfig.NextPageOffset;
            offset = Serializator.ReadObjectId(pageBuffer, ref pointer);

        } while (!offset.IsNull());

        return (length, pages, disposables);
    }

    /// <summary>
    /// Returns the sequence of pages where a record is stored and its read locks.
    /// </summary>
    /// <param name="offset"></param>
    /// <returns></returns>
    public async Task<byte[]> GetDataFromPage(ObjectIdValue offset)
    {
        (int length, List<BufferPage> pages, List<IDisposable> disposables) = await GetPagesToRead(offset).ConfigureAwait(false);

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

    /// <summary>
    /// Returns the sequence of pages where a record is stored and without locking any pages.
    /// </summary>
    /// <param name="offset"></param>
    /// <returns></returns>
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

    /// <summary>
    /// Forces a page to be written to disk
    /// </summary>
    /// <param name="memoryPage"></param>
    public void FlushPage(BufferPage memoryPage)
    {
        storage.Write(memoryPage.Offset, memoryPage.Buffer.Value);
    }

    /// <summary>
    /// Writes the page header in the page buffer according to the current page schema/format
    /// </summary>
    /// <param name="pageBuffer"></param>
    /// <param name="checksum"></param>
    /// <param name="lastSequence"></param>
    /// <param name="nextPage"></param>
    /// <param name="length"></param>
    /// <returns></returns>
    public static int WritePageHeader(byte[] pageBuffer, uint checksum, uint lastSequence, ObjectIdValue nextPage, int length)
    {
        int pointer = 0;
        Serializator.WriteInt16(pageBuffer, BConfig.PageLayoutVersion, ref pointer);  // layout version (2 byte integer)        
        Serializator.WriteUInt32(pageBuffer, checksum, ref pointer);                  // checksum (4 bytes unsigned integer)        
        Serializator.WriteUInt32(pageBuffer, lastSequence, ref pointer);              // lastWroteSequence (4 bytes unsigned integer)        
        Serializator.WriteObjectId(pageBuffer, nextPage, ref pointer);                // next page (12 bytes objectid)        
        Serializator.WriteInt32(pageBuffer, length, ref pointer);                     // data length (4 bytes integer)                
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

        using (await page.WriterLockAsync().ConfigureAwait(false))
        {
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
                await WriteDataToPage(nextPage, sequence, data, startOffset + length).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Applies the operations queued in the list to the storage in a single atomic operation.
    /// </summary>
    /// <param name="modifiedPages"></param>
    public void ApplyPageOperations(List<BufferPageOperation> modifiedPages)
    {

        logger.LogInformation("Wrote {Count} pages in the batch", modifiedPages.Count);

        storage.WriteBatch(modifiedPages);
    }

    /// <summary>
    /// Converts a buffer into a sequence of multiple pages that will be subsequently sent to storage.
    /// </summary>
    /// <param name="pagesToWrite"></param>
    /// <param name="offset"></param>
    /// <param name="sequence"></param>
    /// <param name="data"></param>
    /// <param name="startOffset"></param>
    /// <exception cref="CamusDBException"></exception>
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

    /// <summary>
    /// Deletes all the pages associated with a record in a single atomic operation
    /// </summary>
    /// <param name="offset"></param>
    /// <returns></returns>
    /// <exception cref="CamusDBException"></exception>
    public async Task DeletePage(ObjectIdValue offset)
    {
        if (offset.IsNull())
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidPageOffset,
                "Invalid page offset"
            );

        (int length, List<BufferPage> pagesChain, List<IDisposable> disposables) = await GetPagesToWrite(offset).ConfigureAwait(false);

        try
        {
            List<BufferPageOperation> pagesToDelete = new(pagesChain.Count);

            foreach (BufferPage memoryPage in pagesChain)
            {
                pagesToDelete.Add(new BufferPageOperation(BufferPageOperationType.Delete, memoryPage.Offset, 0, Array.Empty<byte>()));

                uint hashCode = (uint)memoryPage.Offset.GetHashCode();
                BufferPoolBucket poolBucket = buckets[hashCode % CamusConfig.NumberBuckets];

                poolBucket.Pages.TryRemove(memoryPage.Offset, out _);
            }

            storage.WriteBatch(pagesToDelete);
        }
        finally
        {
            foreach (IDisposable disposable in disposables)
                disposable.Dispose();
        }

        logger.LogInformation("Removed {Count} pages from disk, length={Length}", pagesChain.Count, length);
    }

    public void Flush()
    {
        // storage.Flush();
    }

    public void Clear()
    {
        for (int i = 0; i < buckets.Length; i++)
            buckets[i].Clear();
    }
}