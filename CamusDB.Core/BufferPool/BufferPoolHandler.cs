
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.IO.MemoryMappedFiles;
using CamusDB.Core.Serializer;
using CamusDB.Core.BufferPool.Models;

namespace CamusDB.Core.BufferPool;

public sealed class BufferPoolHandler
{
    public const int LayoutVersion = 1;

    public const int PageSize = 1024;

    public const int TotalPages = 4096;

    public const int TableSpaceHeaderPage = 0;

    public const int DataOffset = 2 + 4 * 3; // 2 version + 4 checksum + 4 next page + 4 data length

    public const int LengthOffset = 2 + 4 * 2; // 2 version + 4 checksum + 4 next page + 4 data length

    public const int RowIdOffset = 14;

    public const int FreePageOffset = 10;

    private readonly MemoryMappedFile memoryFile;

    private readonly MemoryMappedViewAccessor accessor;

    private readonly Dictionary<int, BufferPage> pages = new();

    private readonly SemaphoreSlim semaphore = new(1, 1);

    public BufferPoolHandler(MemoryMappedFile memoryFile)
    {
        this.memoryFile = memoryFile;
        this.accessor = memoryFile.CreateViewAccessor(0, TotalPages * PageSize);
    }

    // Load a page without reading its contents
    private async ValueTask<BufferPage> GetPage(int offset)
    {
        if (pages.TryGetValue(offset, out BufferPage? page))
            return page;

        try
        {
            await semaphore.WaitAsync();

            if (pages.TryGetValue(offset, out page))
                return page;

            page = new BufferPage(offset, new byte[PageSize]);
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
            await semaphore.WaitAsync();

            if (pages.TryGetValue(offset, out page))
                return page;

            page = new BufferPage(offset, new byte[PageSize]);
            pages.Add(offset, page);

            await page.Semaphore.WaitAsync();

            accessor.ReadArray<byte>(PageSize * offset, page.Buffer, 0, PageSize);
        }
        finally
        {
            if (page != null)
                page.Semaphore.Release();

            semaphore.Release();
        }

        return page;
    }

    public async Task<ReadOnlyMemory<byte>> GetDataFromPage(int offset)
    {
        BufferPage memoryPage = await ReadPage(offset);

        int pointer = LengthOffset;

        int length = Serializator.ReadInt32(memoryPage.Buffer, ref pointer);

        //Console.WriteLine("Read {0} bytes from page {1}", length, offset);

        if (length == 0)
            return Array.Empty<byte>();

        return memoryPage.Buffer.AsMemory(DataOffset, length > PageSize ? PageSize : length);
    }

    public async Task<byte[]> GetDataFromPageAsBytes(int offset)
    {
        BufferPage memoryPage = await ReadPage(offset);

        int pointer = LengthOffset;

        int length = Serializator.ReadInt32(memoryPage.Buffer, ref pointer);

        //Console.WriteLine("Read {0} bytes from page {1}", length, offset);

        if (length == 0)
            return Array.Empty<byte>();

        byte[] data = new byte[length];
        Buffer.BlockCopy(memoryPage.Buffer, DataOffset, data, 0, length > PageSize ? PageSize : length);
        return data;
    }

    public async Task WritePages(int offset, byte[] data)
    {
        int numberPages = (int)Math.Ceiling(data.Length / ((double)PageSize));

        int pointer = 0;
        int remaining = data.Length;

        for (int i = offset; i < (offset + numberPages); i++)
        {
            BufferPage page = await GetPage(i);

            try
            {
                await page.Semaphore.WaitAsync();

                if (i == offset) // fist page
                {
                    pointer = WritePageHeader(page, data.Length, i + 1);
                    Buffer.BlockCopy(data, 0, page.Buffer, DataOffset, remaining > PageSize ? PageSize : remaining);
                }
                else
                {
                    Buffer.BlockCopy(data, pointer, page.Buffer, 0, remaining > PageSize ? PageSize : remaining);
                }

                pointer += PageSize;
                remaining -= PageSize;

                accessor.WriteArray<byte>(PageSize * page.Offset, page.Buffer, 0, PageSize);
            }
            finally
            {
                page.Semaphore.Release();
            }
        }

        accessor.Flush();
    }

    public void FlushPage(BufferPage memoryPage)
    {
        accessor.WriteArray<byte>(PageSize * memoryPage.Offset, memoryPage.Buffer, 0, PageSize);
        accessor.Flush();
    }

    public void WriteTableSpaceHeader(BufferPage page)
    {
        int pointer = 0;
        Serializator.WriteInt16(page.Buffer, LayoutVersion, ref pointer); // layout version (2 byte integer)
        Serializator.WriteInt32(page.Buffer, 0, ref pointer); // checksum (4 bytes integer)                
        Serializator.WriteInt32(page.Buffer, 8, ref pointer); // data length (4 bytes integer)
        Serializator.WriteInt32(page.Buffer, 1, ref pointer); // first page
        Serializator.WriteInt32(page.Buffer, 1, ref pointer); // first row id
    }

    public int WritePageHeader(BufferPage page, int length, int nextPage)
    {
        int pointer = 0;
        Serializator.WriteInt16(page.Buffer, LayoutVersion, ref pointer); // layout version (2 byte integer)
        Serializator.WriteInt32(page.Buffer, 0, ref pointer); // checksum (4 bytes integer)
        Serializator.WriteInt32(page.Buffer, nextPage, ref pointer); // next page (4 bytes integer)
        Serializator.WriteInt32(page.Buffer, length, ref pointer); // data length (4 bytes integer)        
        return pointer;
    }

    public async Task<int> GetNextFreeOffset()
    {
        BufferPage page = await ReadPage(TableSpaceHeaderPage);

        int pointer = FreePageOffset - 4, pageOffset = 1;

        try
        {
            await page.Semaphore.WaitAsync();

            int length = Serializator.ReadInt32(page.Buffer, ref pointer);

            if (length == 0) // tablespace is not initialized ?            
                WriteTableSpaceHeader(page);
            else
            {
                pointer = FreePageOffset;
                pageOffset = Serializator.ReadInt32(page.Buffer, ref pointer); // retrieve existing offset
            }

            pointer = FreePageOffset;
            Serializator.WriteInt32(page.Buffer, pageOffset + 1, ref pointer); // write new offset

            accessor.WriteArray<byte>(PageSize * TableSpaceHeaderPage, page.Buffer, 0, PageSize);
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
        BufferPage page = await ReadPage(TableSpaceHeaderPage);

        try
        {
            await page.Semaphore.WaitAsync();

            int pointer = FreePageOffset - 4, rowId = 1;

            int length = Serializator.ReadInt32(page.Buffer, ref pointer);

            if (length == 0) // tablespace is not initialized ?            
                WriteTableSpaceHeader(page);
            else
            {
                pointer = RowIdOffset;
                rowId = Serializator.ReadInt32(page.Buffer, ref pointer); // retrieve existing row id
            }

            pointer = RowIdOffset;
            Serializator.WriteInt32(page.Buffer, rowId + 1, ref pointer); // write new row id

            //Console.WriteLine("CurrentRowId={0} NextRowId={1}", rowId, rowId + 1);

            accessor.WriteArray<byte>(PageSize * TableSpaceHeaderPage, page.Buffer, 0, PageSize);

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
        BufferPage page = await GetPage(offset);

        try
        {
            int nextPage = 0;

            // @todo calculate checksum
            // @todo calculate overflow and write to another page

            await page.Semaphore.WaitAsync();

            int length = ((data.Length - startOffset) + DataOffset) < PageSize ? (data.Length - startOffset) : (PageSize - DataOffset);
            int remaining = (data.Length - startOffset) - length;



            if (remaining > 0)
                nextPage = await GetNextFreeOffset();

            int pointer = WritePageHeader(page, data.Length - startOffset, nextPage);

            /*Console.WriteLine("DataLength={0}", data.Length);
            Console.WriteLine(length);
            Console.WriteLine(pointer);
            Console.WriteLine(PageSize);
            Console.WriteLine(page.Buffer.Length);*/

            if (startOffset > 0)
            {
                Console.WriteLine("Another page is required {0} {1} {2}", startOffset, length, remaining);
            }
            else
            {
                Buffer.BlockCopy(data, 0, page.Buffer, pointer, length);
                accessor.WriteArray<byte>(PageSize * offset, page.Buffer, 0, PageSize);
            }

            Console.WriteLine("Wrote {0} bytes to page {1}, remaining {2}, next page {3}", length, offset, remaining, nextPage);

            if (nextPage > 0)
            {
                Console.WriteLine("Another page is required");
                await WriteDataToPage(nextPage, data, startOffset + length);
            }
        }
        finally
        {
            page.Semaphore.Release();
        }
    }
}