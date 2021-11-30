
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.IO.MemoryMappedFiles;
using CamusDB.Core.Serializer;
using CamusDB.Core.BufferPool.Models;
using Config = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Core.BufferPool;

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
        this.accessor = memoryFile.CreateViewAccessor(0, Config.TotalPages * Config.PageSize);
    }

    // Load a page without reading its contents
    public async ValueTask<BufferPage> GetPage(int offset)
    {
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
        if (pages.TryGetValue(offset, out BufferPage? page))
            return page;

        try
        {
            await semaphore.WaitAsync(); // prevent other readers from load the same page

            if (pages.TryGetValue(offset, out page))
                return page;

            page = new BufferPage(offset, new byte[Config.PageSize]);
            pages.Add(offset, page);

            await page.Semaphore.WaitAsync(); // get a consistent read of the page's buffer

            accessor.ReadArray<byte>(Config.PageSize * offset, page.Buffer, 0, Config.PageSize);
        }
        finally
        {
            if (page != null)
                page.Semaphore.Release();

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

            int pointer = Config.LengthOffset;
            length += Serializator.ReadInt32(memoryPage.Buffer, ref pointer);

            pointer = Config.NextPageOffset;
            offset = Serializator.ReadInt32(memoryPage.Buffer, ref pointer);

        } while (offset > 0);

        return length;
    }

    public async Task<byte[]> GetDataFromPage(int offset)
    {
        int length = await GetDataLength(offset);

        if (length == 0)
            return Array.Empty<byte>();

        byte[] data = new byte[length];

        int bufferOffset = 0;

        do
        {
            BufferPage memoryPage = await ReadPage(offset);

            int pointer = Config.LengthOffset;
            int dataLength = Serializator.ReadInt32(memoryPage.Buffer, ref pointer);

            pointer = Config.NextPageOffset;
            offset = Serializator.ReadInt32(memoryPage.Buffer, ref pointer);

            Buffer.BlockCopy(memoryPage.Buffer, Config.DataOffset, data, bufferOffset, dataLength);
            bufferOffset += dataLength;

            Console.WriteLine("Read {0} bytes from page {1}", dataLength, offset);

        } while (offset > 0);

        return data;
    }    

    public void FlushPage(BufferPage memoryPage)
    {
        accessor.WriteArray<byte>(Config.PageSize * memoryPage.Offset, memoryPage.Buffer, 0, Config.PageSize);
        accessor.Flush();
    }

    public void WriteTableSpaceHeader(BufferPage page)
    {
        int pointer = 0;
        Serializator.WriteInt16(page.Buffer, Config.PageLayoutVersion, ref pointer); // layout version (2 byte integer)
        Serializator.WriteInt32(page.Buffer, 0, ref pointer); // checksum (4 bytes integer)                
        Serializator.WriteInt32(page.Buffer, 8, ref pointer); // data length (4 bytes integer)
        Serializator.WriteInt32(page.Buffer, 1, ref pointer); // first page
        Serializator.WriteInt32(page.Buffer, 1, ref pointer); // first row id
    }

    public int WritePageHeader(BufferPage page, int length, int nextPage)
    {
        int pointer = 0;
        Serializator.WriteInt16(page.Buffer, Config.PageLayoutVersion, ref pointer); // layout version (2 byte integer)
        Serializator.WriteInt32(page.Buffer, 0, ref pointer); // checksum (4 bytes integer)
        Serializator.WriteInt32(page.Buffer, nextPage, ref pointer); // next page (4 bytes integer)
        Serializator.WriteInt32(page.Buffer, length, ref pointer); // data length (4 bytes integer)        
        return pointer;
    }

    public async Task<int> GetNextFreeOffset()
    {
        BufferPage page = await ReadPage(Config.TableSpaceHeaderPage);

        int pointer = Config.FreePageOffset - 4, pageOffset = 1;

        try
        {
            await page.Semaphore.WaitAsync();

            int length = Serializator.ReadInt32(page.Buffer, ref pointer);

            if (length == 0) // tablespace is not initialized ?            
                WriteTableSpaceHeader(page);
            else
            {
                pointer = Config.FreePageOffset;
                pageOffset = Serializator.ReadInt32(page.Buffer, ref pointer); // retrieve existing offset
            }

            pointer = Config.FreePageOffset;
            Serializator.WriteInt32(page.Buffer, pageOffset + 1, ref pointer); // write new offset

            accessor.WriteArray<byte>(Config.PageSize * Config.TableSpaceHeaderPage, page.Buffer, 0, Config.PageSize);
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

            int pointer = Config.FreePageOffset - 4, rowId = 1;

            int length = Serializator.ReadInt32(page.Buffer, ref pointer);

            if (length == 0) // tablespace is not initialized ?            
                WriteTableSpaceHeader(page);
            else
            {
                pointer = Config.RowIdOffset;
                rowId = Serializator.ReadInt32(page.Buffer, ref pointer); // retrieve existing row id
            }

            pointer = Config.RowIdOffset;
            Serializator.WriteInt32(page.Buffer, rowId + 1, ref pointer); // write new row id

            //Console.WriteLine("CurrentRowId={0} NextRowId={1}", rowId, rowId + 1);

            accessor.WriteArray<byte>(Config.PageSize * Config.TableSpaceHeaderPage, page.Buffer, 0, Config.PageSize);

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
        if (offset == Config.TableSpaceHeaderPage)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Cannot write to tablespace header page"
            );

        BufferPage page = await GetPage(offset);

        try
        {
            int nextPage = 0;

            // @todo calculate checksum
            // @todo calculate overflow and write to another page

            await page.Semaphore.WaitAsync();

            int length = ((data.Length - startOffset) + Config.DataOffset) < Config.PageSize ? (data.Length - startOffset) : (Config.PageSize - Config.DataOffset);
            int remaining = (data.Length - startOffset) - length;

            if (remaining > 0)
                nextPage = await GetNextFreeOffset();

            int pointer = WritePageHeader(page, length, nextPage);            

            Buffer.BlockCopy(data, startOffset, page.Buffer, pointer, length);
            accessor.WriteArray<byte>(Config.PageSize * offset, page.Buffer, 0, Config.PageSize);

            Console.WriteLine("Wrote {0} bytes to page {1} from buffer staring at {2}, remaining {3}, next page {4}", length, offset, startOffset, remaining, nextPage);

            if (nextPage > 0)
                await WriteDataToPage(nextPage, data, startOffset + length);
        }
        finally
        {
            page.Semaphore.Release();
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