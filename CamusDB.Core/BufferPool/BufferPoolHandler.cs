
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Serializer;
using CamusDB.Core.Util.Hashes;
using System.IO.MemoryMappedFiles;
using CamusDB.Core.BufferPool.Models;
using Config = CamusDB.Core.CamusDBConfig;
using BConfig = CamusDB.Core.BufferPool.Models.BufferPoolConfig;

namespace CamusDB.Core.BufferPool;

/*
 * BufferPoolHandler
 *
 * Organizes a memory-mapped disk file in memory pages. Pages are loaded on demand
 * and are organized in the following layout:
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
    private readonly MemoryMappedFile memoryFile;

    private readonly MemoryMappedViewAccessor accessor;

    private readonly Dictionary<int, BufferPage> pages = new();

    private readonly SemaphoreSlim semaphore = new(1, 1);

    public int NumberPages => pages.Count;

    public Dictionary<int, BufferPage> Pages => pages;

    public BufferPoolHandler(MemoryMappedFile memoryFile)
    {
        this.memoryFile = memoryFile;
        this.accessor = memoryFile.CreateViewAccessor();
    }

    public async Task Initialize()
    {
        // initialize pages
        for (int i = 0; i < Config.InitialPagesRead; i++)
            await ReadPage(i);
    }

    // Load a page without reading its contents
    public async ValueTask<BufferPage> GetPage(int offset)
    {
        if (offset < 0)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidPageOffset,
                "Invalid page offset"
            );

        if (pages.TryGetValue(offset, out BufferPage? page))
            return page;

        try
        {
            await semaphore.WaitAsync(); // prevent other readers from load the same page

            if (pages.TryGetValue(offset, out page))
                return page;

            page = new BufferPage(offset, new byte[Config.PageSize]);
            pages.Add(offset, page);

            return page;
        }
        finally
        {
            semaphore.Release();
        }
    }

    // Load a page reading its contents
    public async ValueTask<BufferPage> ReadPage(int offset)
    {
        if (offset < 0)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidPageOffset,
                "Invalid page offset"
            );

        if (pages.TryGetValue(offset, out BufferPage? page))
            return page;

        try
        {
            await semaphore.WaitAsync(); // prevent other readers from load the same page

            if (pages.TryGetValue(offset, out page))
                return page;

            page = new BufferPage(offset, new byte[Config.PageSize]);
            pages.Add(offset, page);

            accessor.ReadArray<byte>(Config.PageSize * offset, page.Buffer, 0, Config.PageSize);

            // Console.WriteLine("Page {0} read", offset);
        }
        finally
        {
            semaphore.Release();
        }

        return page;
    }

    private async Task<int> GetDataLength(int offset)
    {
        int length = 0;

        do
        {
            BufferPage memoryPage = await ReadPage(offset);

            byte[] pageBuffer = memoryPage.Buffer; // get a pointer to the buffer to get a consistent read

            int pointer = BConfig.LengthOffset;
            length += Serializator.ReadInt32(pageBuffer, ref pointer);

            pointer = BConfig.NextPageOffset;
            offset = Serializator.ReadInt32(pageBuffer, ref pointer);

        } while (offset > 0);

        return length;
    }    

    public async Task<byte[]> GetDataFromPage(int offset)
    {
        if (offset < 0)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidPageOffset,
                "Invalid page offset"
            );

        int length = await GetDataLength(offset); // @todo calculated length of page can be different to the read page

        if (length == 0)
            return Array.Empty<byte>();

        byte[] data = new byte[length];

        uint checksum;
        int bufferOffset = 0;

        do
        {
            BufferPage memoryPage = await ReadPage(offset);

            byte[] pageBuffer = memoryPage.Buffer; // get a pointer to the buffer to get a consistent read

            int pointer = BConfig.LengthOffset;
            int dataLength = Serializator.ReadInt32(pageBuffer, ref pointer);

            pointer = BConfig.NextPageOffset;
            offset = Serializator.ReadInt32(pageBuffer, ref pointer);

            pointer = BConfig.ChecksumOffset;
            checksum = Serializator.ReadUInt32(pageBuffer, ref pointer);

            if (checksum > 0) // check checksum only if available
            {
                byte[] pageData = new byte[dataLength]; // @todo avoid this allocation
                Buffer.BlockCopy(pageBuffer, BConfig.DataOffset, pageData, 0, dataLength);

                uint dataChecksum = XXHash.Compute(pageData, 0, dataLength);

                if (dataChecksum != checksum)
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidPageChecksum,
                        "Page has an invalid data checksum"
                    );
            }

            Buffer.BlockCopy(pageBuffer, BConfig.DataOffset, data, bufferOffset, dataLength);
            bufferOffset += dataLength;

            //Console.WriteLine("Read {0} bytes from page {1}", dataLength, offset);

        } while (offset > 0);

        return data;
    }

    public async Task<uint> GetSequenceFromPage(int offset)
    {
        BufferPage memoryPage = await ReadPage(offset);

        byte[] pageBuffer = memoryPage.Buffer; // get a pointer to the buffer to get a consistent read

        int pointer = BConfig.LastSequenceOffset;
        return Serializator.ReadUInt32(pageBuffer, ref pointer);
    }

    public void FlushPage(BufferPage memoryPage)
    {
        accessor.WriteArray<byte>(Config.PageSize * memoryPage.Offset, memoryPage.Buffer, 0, Config.PageSize);
        accessor.Flush();
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
        BufferPage page = await ReadPage(Config.TableSpaceHeaderPage);

        int pointer = BConfig.FreePageOffset - 4, pageOffset = 1;

        try
        {
            await page.LockAsync();

            byte[] pageBuffer = page.Buffer; // get a pointer to the buffer to get a consistent read

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

            accessor.WriteArray<byte>(Config.PageSize * Config.TableSpaceHeaderPage, pageBuffer, 0, Config.PageSize);

            page.Dirty = true;
        }
        finally
        {
            page.Unlock();
        }

        //Console.WriteLine("CurrentPageOffset={0} NextPageOffset={1}", pageOffset, pageOffset + 1);

        return pageOffset;
    }

    public async Task<int> GetNextRowId()
    {
        BufferPage page = await ReadPage(Config.TableSpaceHeaderPage);

        try
        {
            await page.LockAsync();

            // @todo page's content can change here?

            byte[] pageBuffer = page.Buffer; // get a pointer to the buffer to get a consistent read

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

            accessor.WriteArray<byte>(Config.PageSize * Config.TableSpaceHeaderPage, pageBuffer, 0, Config.PageSize);

            page.Dirty = true;
            page.Buffer = pageBuffer;

            return rowId;
        }
        finally
        {
            page.Unlock();
        }
    }

    public async Task<int> WriteDataToFreePage(byte[] data)
    {
        int freeOffset = await GetNextFreeOffset();
        await WriteDataToPage(freeOffset, 0, data);
        return freeOffset;
    }

    public async Task WriteDataToPage(int offset, uint sequence, byte[] data, int startOffset = 0)
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

        BufferPage page = await GetPage(offset);

        try
        {
            int nextPage = 0, length;

            //await page.Semaphore.WaitAsync();

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
            accessor.WriteArray<byte>(Config.PageSize * offset, pageBuffer, 0, Config.PageSize);

            // Replace buffer, this helps to get readers consistent copies
            page.Buffer = pageBuffer;
            page.Dirty = true;

            Console.WriteLine("Wrote {0} bytes to page {1} from buffer staring at {2}, remaining {3}, next page {4}", length, offset, startOffset, remaining, nextPage);

            if (nextPage > 0)
                await WriteDataToPage(nextPage, sequence, data, startOffset + length);
        }
        finally
        {
            //page.Semaphore.Release();
        }
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

        BufferPage page = await ReadPage(offset);

        try
        {
            //await page.Semaphore.WaitAsync();

            byte[] pageBuffer = new byte[Config.PageSize];

            WritePageHeader(pageBuffer, 0, 0, 0, 0);

            accessor.WriteArray<byte>(Config.PageSize * offset, pageBuffer, 0, Config.PageSize);

            page.Buffer = pageBuffer;
            page.Dirty = true;
        }
        finally
        {
            //page.Semaphore.Release();
        }
    }

    public void Flush()
    {
        accessor.Flush();
    }

    public void Clear()
    {
        pages.Clear();
    }

    public void Dispose()
    {
        if (accessor != null)
            accessor.Dispose();

        if (memoryFile != null)
            memoryFile.Dispose();
    }
}