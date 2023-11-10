
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
using Config = CamusDB.Core.CamusDBConfig;
using BConfig = CamusDB.Core.BufferPool.Models.BufferPoolConfig;
using System.Threading.Channels;
using System.Collections.Concurrent;
using CamusDB.Core.CommandsExecutor.Models.StateMachines;

namespace CamusDB.Core.BufferPool;

/*
 * BufferPoolHandler
 *
 * A buffer pool is an area of main memory that has been allocated by the database manager
 * for the purpose of caching table and index data as it is read from disk.
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
 * +----------+----------+----------+---------------------+
 * | next page offset (4 bytes integer)                   |
 * +----------+----------+----------+---------------------+
 * | data length (4 bytes integer)                        |
 * +----------+----------+----------+---------------------+
 */
public sealed class BufferPoolHandler : IDisposable
{
    private readonly StorageManager storage;

    private readonly ConcurrentDictionary<int, Lazy<BufferPage>> pages = new();

    private readonly SemaphoreSlim semaphore = new(1, 1);

    private readonly ChannelWriter<NextRowIdOffsetUpdate> offsetUpdaterWriter;

    private readonly ChannelReader<NextRowIdOffsetUpdate> offsetUpdaterReader;

    public int NumberPages => pages.Count;

    public ConcurrentDictionary<int, Lazy<BufferPage>> Pages => pages;

    private int nextRowId;

    private int nextFreeOffset;

    public BufferPoolHandler(StorageManager storage)
    {
        this.storage = storage;

        Channel<NextRowIdOffsetUpdate> channel = Channel.CreateUnbounded<NextRowIdOffsetUpdate>(new UnboundedChannelOptions
        {
            SingleWriter = false,
            SingleReader = true,
            AllowSynchronousContinuations = true
        });

        offsetUpdaterReader = channel.Reader;
        offsetUpdaterWriter = channel.Writer;
    }

    public async Task Initialize()
    {
        nextRowId = await GetNextRowIdFromPage();
        nextFreeOffset = await GetNextFreeOffsetFromPage();

        _ = Task.Run(ConsumeUpdateNextOffsetId);
    }

    private async ValueTask ConsumeUpdateNextOffsetId()
    {
        while (await offsetUpdaterReader.WaitToReadAsync())
        {
            while (offsetUpdaterReader.TryRead(out NextRowIdOffsetUpdate operation))
            {
                if (operation.Type == NextRowIdOffsetUpdateType.NextId)
                    await WriteNextRowId(operation.Value);

                if (operation.Type == NextRowIdOffsetUpdateType.NextOffset)
                    await WriteNextFreeOffset(operation.Value);

                Console.WriteLine(operation.Value);
            }
        }
    }

    // Load a page without reading its contents
    public BufferPage GetPage(int offset)
    {
        if (offset < 0)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidPageOffset,
                "Invalid page offset"
            );

        Lazy<BufferPage> lazyBufferPage = pages.GetOrAdd(offset, (x) => new Lazy<BufferPage>(() => LoadPage(offset)));
        return lazyBufferPage.Value;
    }

    private BufferPage LoadPage(int offset)
    {
        return new BufferPage(offset, new Lazy<byte[]>(() => ReadFromDisk(offset)));
    }

    private byte[] ReadFromDisk(int offset)
    {
        return storage.Read(Config.PageSize * offset);
    }

    // Load a page reading its contents
    public BufferPage ReadPage(int offset)
    {
        if (offset < 0)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidPageOffset,
                "Invalid page offset"
            );

        Lazy<BufferPage> lazyBufferPage = pages.GetOrAdd(offset, (x) => new Lazy<BufferPage>(() => LoadPage(offset)));
        return lazyBufferPage.Value;
    }

    public async Task<bool> IsInitialized(int offset)
    {
        if (offset < 0)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidPageOffset,
                "Invalid page offset"
            );

        BufferPage memoryPage = ReadPage(offset);

        using IDisposable readerLock = await memoryPage.ReaderLockAsync();

        byte[] pageBuffer = memoryPage.Buffer.Value; // get a pointer to the buffer to get a consistent read

        int pointer = BConfig.PageLayoutOffset;
        short pageLayout = Serializator.ReadInt16(pageBuffer, ref pointer);

        return pageLayout > 0;
    }

    private async Task<(int, List<BufferPage>, List<IDisposable>)> GetDataLength(int offset)
    {
        int length = 0;
        List<BufferPage> pages = new();
        List<IDisposable> disposables = new();

        do
        {
            //Console.WriteLine(offset);

            BufferPage memoryPage = ReadPage(offset);
            pages.Add(memoryPage);

            disposables.Add(await memoryPage.ReaderLockAsync());

            byte[] pageBuffer = memoryPage.Buffer.Value; // get a pointer to the buffer to get a consistent read

            int pointer = BConfig.LengthOffset;
            length += Serializator.ReadInt32(pageBuffer, ref pointer);

            pointer = BConfig.NextPageOffset;
            offset = Serializator.ReadInt32(pageBuffer, ref pointer);

        } while (offset > 0);

        return (length, pages, disposables);
    }

    public async Task<byte[]> GetDataFromPage(int offset)
    {
        if (offset < 0)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidPageOffset,
                "Invalid page offset"
            );

        (int length, List<BufferPage> pages, List<IDisposable> disposables) = await GetDataLength(offset); // @todo calculated length of page can be different to the read page

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
                offset = Serializator.ReadInt32(pageBuffer, ref pointer);

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

    public async Task<uint> GetSequenceFromPage(int offset)
    {
        BufferPage memoryPage = ReadPage(offset);

        using IDisposable readerLock = await memoryPage.ReaderLockAsync();

        byte[] pageBuffer = memoryPage.Buffer.Value; // get a pointer to the buffer to get a consistent read

        int pointer = BConfig.LastSequenceOffset;
        return Serializator.ReadUInt32(pageBuffer, ref pointer);
    }

    public void FlushPage(BufferPage memoryPage)
    {
        storage.Write(Config.PageSize * memoryPage.Offset, memoryPage.Buffer.Value);
    }

    public void WriteTableSpaceHeader(byte[] pageBuffer)
    {
        int pointer = 0;
        Serializator.WriteInt16(pageBuffer, Config.PageLayoutVersion, ref pointer); // layout version (2 byte integer)
        Serializator.WriteUInt32(pageBuffer, 0, ref pointer); // checksum (4 bytes integer)
        Serializator.WriteInt32(pageBuffer, 8, ref pointer);  // data length (4 bytes integer)
        Serializator.WriteInt32(pageBuffer, 1, ref pointer);  // first page
        Serializator.WriteInt32(pageBuffer, 1, ref pointer);  // first row id
    }

    public static int WritePageHeader(byte[] pageBuffer, uint checksum, uint lastSequence, int nextPage, int length)
    {
        int pointer = 0;
        Serializator.WriteInt16(pageBuffer, Config.PageLayoutVersion, ref pointer);  // layout version (2 byte integer)
        Serializator.WriteUInt32(pageBuffer, checksum, ref pointer);                 // checksum (4 bytes unsigned integer)
        Serializator.WriteUInt32(pageBuffer, lastSequence, ref pointer);             // lastWroteSequence (4 bytes unsigned integer)
        Serializator.WriteInt32(pageBuffer, nextPage, ref pointer);                  // next page (4 bytes integer)
        Serializator.WriteInt32(pageBuffer, length, ref pointer);                    // data length (4 bytes integer)
        return pointer;
    }

    public async Task<int> GetNextFreeOffset()
    {
        //int newFreeOffset = Interlocked.Increment(ref nextFreeOffset);
        //await offsetUpdaterWriter.WriteAsync(new NextRowIdOffsetUpdate(NextRowIdOffsetUpdateType.NextOffset, newFreeOffset));
        //return newFreeOffset;

        return await GetNextFreeOffsetFromPage();
    }

    public async Task<int> GetNextFreeOffsetFromPage()
    {
        BufferPage page = ReadPage(Config.TableSpaceHeaderPage);

        int pointer = BConfig.FreePageOffset - 4, pageOffset = 1;

        using IDisposable writerLock = await page.WriterLockAsync();

        byte[] pageBuffer = page.Buffer.Value; // get a pointer to the buffer to get a consistent read

        int length = Serializator.ReadInt32(pageBuffer, ref pointer);

        if (length == 0)
        {
            // tablespace is not initialized ?
            WriteTableSpaceHeader(pageBuffer);
        }
        else
        {
            pointer = BConfig.FreePageOffset;
            pageOffset = Serializator.ReadInt32(pageBuffer, ref pointer); // retrieve existing offset
        }

        pointer = BConfig.FreePageOffset;
        Serializator.WriteInt32(pageBuffer, pageOffset + 1, ref pointer); // write new offset

        storage.Write(Config.PageSize * Config.TableSpaceHeaderPage, pageBuffer);

        page.Dirty = true;

        //Console.WriteLine("CurrentPageOffset={0} NextPageOffset={1}", pageOffset, pageOffset + 1);

        return pageOffset;
    }

    public async Task WriteNextFreeOffset(int pageOffset)
    {
        BufferPage page = ReadPage(Config.TableSpaceHeaderPage);

        int pointer = BConfig.FreePageOffset - 4;

        using IDisposable writeLock = await page.WriterLockAsync();

        byte[] pageBuffer = page.Buffer.Value; // get a pointer to the buffer to get a consistent read

        int length = Serializator.ReadInt32(pageBuffer, ref pointer);

        if (length == 0)
        {
            // tablespace is not initialized ?
            WriteTableSpaceHeader(pageBuffer);
        }
        else
        {
            pointer = BConfig.FreePageOffset;
            pageOffset = Serializator.ReadInt32(pageBuffer, ref pointer); // retrieve existing offset
        }

        pointer = BConfig.FreePageOffset;
        Serializator.WriteInt32(pageBuffer, pageOffset, ref pointer); // write new offset

        storage.Write(Config.PageSize * Config.TableSpaceHeaderPage, pageBuffer);

        page.Dirty = true;
    }

    public async Task<int> GetNextRowId()
    {
        //int newRowId = Interlocked.Increment(ref nextRowId);
        //await offsetUpdaterWriter.WriteAsync(new NextRowIdOffsetUpdate(NextRowIdOffsetUpdateType.NextId, newRowId));
        //return newRowId;

        return await GetNextRowIdFromPage();
    }

    public async Task<int> GetNextRowIdFromPage()
    {
        BufferPage page = ReadPage(Config.TableSpaceHeaderPage);

        using IDisposable writeLock = await page.WriterLockAsync();

        // @todo page's content can change here?

        byte[] pageBuffer = page.Buffer.Value; // get a pointer to the buffer to get a consistent read

        int pointer = BConfig.FreePageOffset - 4, rowId = 1;

        int length = Serializator.ReadInt32(pageBuffer, ref pointer);

        if (length == 0) // tablespace is not initialized ?
            WriteTableSpaceHeader(pageBuffer);
        else
        {
            pointer = BConfig.RowIdOffset;
            rowId = Serializator.ReadInt32(pageBuffer, ref pointer); // retrieve existing row id
        }

        pointer = BConfig.RowIdOffset;
        Serializator.WriteInt32(pageBuffer, rowId + 1, ref pointer); // write new row id

        //Console.WriteLine("CurrentRowId={0} NextRowId={1}", rowId, rowId + 1);

        storage.Write(Config.PageSize * Config.TableSpaceHeaderPage, pageBuffer);

        page.Dirty = true;
        page.Buffer = new Lazy<byte[]>(pageBuffer);

        return rowId;
    }

    public async Task WriteNextRowId(int rowId)
    {
        BufferPage page = ReadPage(Config.TableSpaceHeaderPage);

        using IDisposable writeLock = await page.WriterLockAsync();

        byte[] pageBuffer = page.Buffer.Value; // get a pointer to the buffer to get a consistent read

        int pointer = BConfig.FreePageOffset - 4;
        int length = Serializator.ReadInt32(pageBuffer, ref pointer);

        if (length == 0) // tablespace is not initialized ?
            WriteTableSpaceHeader(pageBuffer);

        pointer = BConfig.RowIdOffset;
        Serializator.WriteInt32(pageBuffer, rowId, ref pointer); // write new row id

        //Console.WriteLine("CurrentRowId={0} NextRowId={1}", rowId, rowId + 1);

        storage.Write(Config.PageSize * Config.TableSpaceHeaderPage, pageBuffer);

        page.Dirty = true;
        page.Buffer = new Lazy<byte[]>(pageBuffer);
    }

    public async Task<int> WriteDataToFreePage(byte[] data)
    {
        int freeOffset = await GetNextFreeOffset();
        await WriteDataToPage(freeOffset, 0, data);
        return freeOffset;
    }

    public async Task WriteDataToPage(int offset, uint sequence, byte[] data, int startOffset = 0, List<PageToWrite>? pagesToWrite = null)
    {
        if (offset < 0)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidPageOffset,
                "Invalid page offset"
            );

        if (offset == Config.TableSpaceHeaderPage)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Cannot write to tablespace header page"
            );

        if (startOffset < 0)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidPageOffset,
                "Start offset can't be negative"
            );

        BufferPage page = GetPage(offset);

        using IDisposable writeLock = await page.WriterLockAsync();

        int nextPage = 0, length;

        // Calculate remaining data length less the page's header
        if (((data.Length - startOffset) + BConfig.DataOffset) < Config.PageSize)
            length = data.Length - startOffset;
        else
            length = Config.PageSize - BConfig.DataOffset;

        int remaining = (data.Length - startOffset) - length;

        if (remaining > 0)
            nextPage = await GetNextFreeOffset();

        // Create a buffer to calculate the checksum
        byte[] pageData = new byte[length];
        Buffer.BlockCopy(data, startOffset, pageData, 0, length);

        uint checksum = XXHash.Compute(pageData, 0, length);

        // Create a new page buffer to replace the existing one
        byte[] pageBuffer = new byte[Config.PageSize];

        int pointer = WritePageHeader(pageBuffer, checksum, sequence, nextPage, length);

        if (nextPage > 0 && nextPage == offset)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Cannot write recursively to the same page"
            );

        Buffer.BlockCopy(data, startOffset, pageBuffer, pointer, length);
        storage.Write(Config.PageSize * offset, pageBuffer);

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

        if (nextPage > 0)
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

    public async Task WriteDataToPageBatch(List<PageToWrite> pagesToWrite, int offset, uint sequence, byte[] data, int startOffset = 0)
    {
        if (offset < 0)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidPageOffset,
                "Invalid page offset"
            );

        if (offset == Config.TableSpaceHeaderPage)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Cannot write to tablespace header page"
            );

        if (startOffset < 0)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidPageOffset,
                "Start offset can't be negative"
            );

        BufferPage page = GetPage(offset);

        using IDisposable writeLock = await page.WriterLockAsync();

        int nextPage = 0, length;

        // Calculate remaining data length less the page's header
        if (((data.Length - startOffset) + BConfig.DataOffset) < Config.PageSize)
            length = data.Length - startOffset;
        else
            length = Config.PageSize - BConfig.DataOffset;

        int remaining = (data.Length - startOffset) - length;

        if (remaining > 0)
            nextPage = await GetNextFreeOffset();

        // Create a buffer to calculate the checksum
        byte[] pageData = new byte[length];
        Buffer.BlockCopy(data, startOffset, pageData, 0, length);

        uint checksum = XXHash.Compute(pageData, 0, length);

        // Create a new page buffer to replace the existing one
        byte[] pageBuffer = new byte[Config.PageSize];

        int pointer = WritePageHeader(pageBuffer, checksum, sequence, nextPage, length);

        if (nextPage > 0 && nextPage == offset)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Cannot write recursively to the same page"
            );

        Buffer.BlockCopy(data, startOffset, pageBuffer, pointer, length);
        //storage.Write(Config.PageSize * offset, pageBuffer);
        pagesToWrite.Add(new PageToWrite(Config.PageSize * offset, pageBuffer));

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

        if (nextPage > 0)
            await WriteDataToPageBatch(pagesToWrite, nextPage, sequence, data, startOffset + length);
    }

    public async Task CleanPage(int offset)
    {
        if (offset < 0)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidPageOffset,
                "Invalid page offset"
            );

        if (offset == Config.TableSpaceHeaderPage)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Cannot write to tablespace header page"
            );

        BufferPage page = ReadPage(offset);

        using IDisposable writeLock = await page.WriterLockAsync();

        byte[] pageBuffer = new byte[Config.PageSize];

        WritePageHeader(pageBuffer, 0, 0, 0, 0);

        storage.Write(Config.PageSize * offset, pageBuffer);

        page.Buffer = new Lazy<byte[]>(pageBuffer);
        page.Dirty = true;
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