
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
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.CommandsExecutor.Models.StateMachines;

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

    private readonly ConcurrentDictionary<ObjectIdValue, Lazy<BufferPage>> pages = new();

    public int NumberPages => pages.Count;

    public ConcurrentDictionary<ObjectIdValue, Lazy<BufferPage>> Pages => pages;

    public BufferPoolHandler(StorageManager storage)
    {
        this.storage = storage;
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
        return lazyBufferPage.Value;
    }

    private async Task<(int, List<BufferPage>, List<IDisposable>)> GetDataLength(ObjectIdValue offset)
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

    public async Task<byte[]> GetDataFromPage(ObjectIdValue offset)
    {
        (int length, List<BufferPage> pages, List<IDisposable> disposables) = await GetDataLength(offset);

        try
        {
            if (length == 0)
                return Array.Empty<byte>();

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
        finally
        {
            foreach (IDisposable disposable in disposables)
                disposable.Dispose();
        }
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

    public async Task WriteDataToPages(List<InsertModifiedPage> modifiedPages)
    {
        List<PageToWrite> pagesToWrite = new(modifiedPages.Count);

        foreach (InsertModifiedPage modifiedPage in modifiedPages)
            await WriteDataToPageBatch(pagesToWrite, modifiedPage.Offset, modifiedPage.Sequence, modifiedPage.Buffer);

        Console.WriteLine("Wrote {0} pages in the batch", pagesToWrite.Count);

        storage.WriteBatch(pagesToWrite);
    }

    public async Task WriteDataToPageBatch(List<PageToWrite> pagesToWrite, ObjectIdValue offset, uint sequence, byte[] data, int startOffset = 0)
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
        //storage.Write(Config.PageSize * offset, pageBuffer);
        pagesToWrite.Add(new PageToWrite(offset, pageBuffer));

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
            await WriteDataToPageBatch(pagesToWrite, nextPage, sequence, data, startOffset + length);
    }

    public async Task DeletePage(ObjectIdValue offset)
    {
        if (offset.IsNull())
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidPageOffset,
                "Invalid page offset"
            );

        BufferPage page = ReadPage(offset);

        using IDisposable writeLock = await page.WriterLockAsync();

        storage.Delete(offset);

        pages.TryRemove(offset, out _);
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
    }
}