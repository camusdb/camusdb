
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

namespace CamusDB.Core.BufferPool;

/*
 * BufferPoolHandler
 *
 * Organizes a memory-mapped disk file in memory pages. Pages are loaded on demand
 * and are organized in the following layout:
 *
 * +--------------------+
 * | version (2 bytes)  |
 * +--------------------+--------------------------+
 * | checksum (4 bytes)                            |
 * +----------+----------+----------+--------------+
 * | slot #0  | slot #0  | slot #0  | slot #0      |
 * | data     | next     | next     | data         |
 * | length   | page     | slot     | length       |
 * | (2 byte) | (4 byte) | (1 byte) |              |
 * |          |          |          |              |
 * |          |          |          |              |
 * +----------+----------+-------------------------+
 *
 * Every page has 4 slots numbered from 0 to 3.
 */
public sealed class BufferPoolHandler : IDisposable
{
    private readonly MemoryMappedFile memoryFile;

    private readonly MemoryMappedViewAccessor accessor;

    private readonly Dictionary<int, BufferPage> pages = new();

    private readonly SemaphoreSlim semaphore = new(1, 1);

    public int NumberPages => pages.Count;

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

            int pointer = Config.LengthOffset;
            length += Serializator.ReadInt32(pageBuffer, ref pointer);

            pointer = Config.NextPageOffset;
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

            int pointer = Config.LengthOffset;
            int dataLength = Serializator.ReadInt32(pageBuffer, ref pointer);

            pointer = Config.NextPageOffset;
            offset = Serializator.ReadInt32(pageBuffer, ref pointer);

            pointer = Config.ChecksumOffset;
            checksum = Serializator.ReadUInt32(pageBuffer, ref pointer);

            if (checksum > 0) // check checksum only if available
            {
                byte[] pageData = new byte[dataLength]; // @todo avoid this allocation
                Buffer.BlockCopy(pageBuffer, Config.DataOffset, pageData, 0, dataLength);

                uint dataChecksum = XXHash.Compute(pageData, 0, dataLength);

                if (dataChecksum != checksum)
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidPageChecksum,
                        "Page has an invalid data checksum"
                    );
            }

            Buffer.BlockCopy(pageBuffer, Config.DataOffset, data, bufferOffset, dataLength);
            bufferOffset += dataLength;

            //Console.WriteLine("Read {0} bytes from page {1}", dataLength, offset);

        } while (offset > 0);

        return data;
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

    public static int WritePageHeader(byte[] pageBuffer, int length, int nextPage, uint checksum)
    {
        int pointer = 0;
        Serializator.WriteInt16(pageBuffer, Config.PageLayoutVersion, ref pointer);  // layout version (2 byte integer)
        Serializator.WriteUInt32(pageBuffer, checksum, ref pointer);                 // checksum (4 bytes integer)
        Serializator.WriteInt32(pageBuffer, nextPage, ref pointer);                  // next page (4 bytes integer)
        Serializator.WriteInt32(pageBuffer, length, ref pointer);                    // data length (4 bytes integer)
        return pointer;
    }

    public async Task<int> GetNextFreeOffset()
    {
        BufferPage page = await ReadPage(Config.TableSpaceHeaderPage);

        int pointer = Config.FreePageOffset - 4, pageOffset = 1;

        try
        {
            await page.Semaphore.WaitAsync();

            byte[] pageBuffer = page.Buffer; // get a pointer to the buffer to get a consistent read

            int length = Serializator.ReadInt32(pageBuffer, ref pointer);

            if (length == 0)
            {
                // tablespace is not initialized ?
                WriteTableSpaceHeader(pageBuffer);
            }
            else
            {
                pointer = Config.FreePageOffset;
                pageOffset = Serializator.ReadInt32(pageBuffer, ref pointer); // retrieve existing offset
            }

            pointer = Config.FreePageOffset;
            Serializator.WriteInt32(pageBuffer, pageOffset + 1, ref pointer); // write new offset

            accessor.WriteArray<byte>(Config.PageSize * Config.TableSpaceHeaderPage, pageBuffer, 0, Config.PageSize);
        }
        finally
        {
            page.Semaphore.Release();
        }

        //Console.WriteLine("CurrentPageOffset={0} NextPageOffset={1}", pageOffset, pageOffset + 1);

        return pageOffset;
    }

    public async Task<int> GetNextRowId()
    {
        BufferPage page = await ReadPage(Config.TableSpaceHeaderPage);

        try
        {
            await page.Semaphore.WaitAsync();

            // @todo page's content can change here?

            byte[] pageBuffer = page.Buffer; // get a pointer to the buffer to get a consistent read

            int pointer = Config.FreePageOffset - 4, rowId = 1;

            int length = Serializator.ReadInt32(pageBuffer, ref pointer);

            if (length == 0) // tablespace is not initialized ?
                WriteTableSpaceHeader(pageBuffer);
            else
            {
                pointer = Config.RowIdOffset;
                rowId = Serializator.ReadInt32(pageBuffer, ref pointer); // retrieve existing row id
            }

            pointer = Config.RowIdOffset;
            Serializator.WriteInt32(pageBuffer, rowId + 1, ref pointer); // write new row id

            //Console.WriteLine("CurrentRowId={0} NextRowId={1}", rowId, rowId + 1);

            accessor.WriteArray<byte>(Config.PageSize * Config.TableSpaceHeaderPage, pageBuffer, 0, Config.PageSize);

            page.Buffer = pageBuffer;

            return rowId;
        }
        finally
        {
            page.Semaphore.Release();
        }
    }

    public async Task<int> WriteDataToFreePage(byte[] data)
    {
        int freeOffset = await GetNextFreeOffset();
        await WriteDataToPage(freeOffset, data);
        return freeOffset;
    }

    public async Task WriteDataToPage(int offset, byte[] data, int startOffset = 0)
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
            int nextPage = 0;

            //await page.Semaphore.WaitAsync();

            int length = ((data.Length - startOffset) + Config.DataOffset) < Config.PageSize ? (data.Length - startOffset) : (Config.PageSize - Config.DataOffset);
            int remaining = (data.Length - startOffset) - length;

            if (remaining > 0)
                nextPage = await GetNextFreeOffset();

            // Create a buffer to calculate the checksum
            byte[] pageData = new byte[length];
            Buffer.BlockCopy(data, startOffset, pageData, 0, length);

            uint checksum = XXHash.Compute(pageData, 0, length);

            // Create a new page buffer to replace the existing one
            byte[] pageBuffer = new byte[Config.PageSize];

            int pointer = WritePageHeader(pageBuffer, length, nextPage, checksum);

            if (nextPage > 0 && nextPage == offset)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    "Cannot write recursively to the same page"
                );

            Buffer.BlockCopy(data, startOffset, pageBuffer, pointer, length);
            accessor.WriteArray<byte>(Config.PageSize * offset, pageBuffer, 0, Config.PageSize);

            // Replace buffer, this helps to get readers consistent copies
            page.Buffer = pageBuffer;

            Console.WriteLine("Wrote {0} bytes to page {1} from buffer staring at {2}, remaining {3}, next page {4}", length, offset, startOffset, remaining, nextPage);

            if (nextPage > 0)
                await WriteDataToPage(nextPage, data, startOffset + length);
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

            WritePageHeader(pageBuffer, 0, 0, 0);

            accessor.WriteArray<byte>(Config.PageSize * offset, pageBuffer, 0, Config.PageSize);

            page.Buffer = pageBuffer;
        }
        finally
        {
            //page.Semaphore.Release();
        }
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