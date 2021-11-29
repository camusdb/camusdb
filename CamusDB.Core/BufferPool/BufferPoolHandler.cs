
using System.IO.MemoryMappedFiles;
using CamusDB.Core.Serializer;
using CamusDB.Core.BufferPool.Models;

namespace CamusDB.Core.BufferPool;

public sealed class BufferPoolHandler
{
    private const int PageSize = 1024;

    private const int TotalPages = 4096;

    private const int TableSpaceHeaderPage = 0;

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

    public async Task<byte[]> GetDataFromPage(int offset)
    {
        BufferPage memoryPage = await ReadPage(offset);

        int pointer = 0;

        int length = Serializator.ReadInt32(memoryPage.Buffer, ref pointer);

        //Console.WriteLine("Read {0} bytes from page {1}", length, offset);

        if (length == 0)
            return Array.Empty<byte>();

        byte[] data = new byte[length];
        Buffer.BlockCopy(memoryPage.Buffer, 4, data, 0, length > PageSize ? PageSize : length);
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
                    Serializator.WriteInt32(page.Buffer, data.Length, ref pointer); // write length to first 4 bytes 
                    Buffer.BlockCopy(data, 0, page.Buffer, 4, remaining > PageSize ? PageSize : remaining);
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

    public async Task<int> GetNextFreeOffset()
    {
        BufferPage page = await ReadPage(TableSpaceHeaderPage);

        int pointer = 0, pageOffset = 1;

        try
        {
            await page.Semaphore.WaitAsync();

            int length = Serializator.ReadInt32(page.Buffer, ref pointer);

            if (length == 0) // tablespace is not initialized ?
            {
                pointer = 0;
                Serializator.WriteInt32(page.Buffer, 4, ref pointer); // data length (4 bytes integer)
                Serializator.WriteInt32(page.Buffer, 0, ref pointer); // current write page offset            
                Serializator.WriteInt32(page.Buffer, 0, ref pointer); // row id
            }
            else
            {
                pointer = 4;
                pageOffset = Serializator.ReadInt32(page.Buffer, ref pointer); // retrieve existing offset
            }

            pointer = 4;
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

            int pointer = 0, rowId = 1;

            int length = Serializator.ReadInt32(page.Buffer, ref pointer);
            if (length == 0) // tablespace is not initialized ?
            {
                pointer = 0;
                Serializator.WriteInt32(page.Buffer, 4, ref pointer); // data length (4 bytes integer)                
                Serializator.WriteInt32(page.Buffer, 0, ref pointer); // first page
                Serializator.WriteInt32(page.Buffer, 0, ref pointer); // first row id
            }
            else
            {
                pointer = 8;
                rowId = Serializator.ReadInt32(page.Buffer, ref pointer); // retrieve existing row id
            }

            pointer = 8;
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

        BufferPage page = await GetPage(freeOffset);

        try
        {
            await page.Semaphore.WaitAsync();

            int pointer = 0;
            Serializator.WriteInt32(page.Buffer, data.Length, ref pointer); // data length (4 bytes integer)

            Buffer.BlockCopy(data, 0, page.Buffer, 4, data.Length);

            accessor.WriteArray<byte>(PageSize * freeOffset, page.Buffer, 0, PageSize);

            //Console.WriteLine("Wrote {0} bytes to page {1}", data.Length, freeOffset);

            return freeOffset;
        }
        finally
        {
            page.Semaphore.Release();
        }
    }

    public async Task WriteDataToPage(int offset, byte[] data)
    {
        BufferPage page = await GetPage(offset);

        try
        {
            await page.Semaphore.WaitAsync();

            int pointer = 0;
            Serializator.WriteInt32(page.Buffer, data.Length, ref pointer); // data length (4 bytes integer)

            Buffer.BlockCopy(data, 0, page.Buffer, 4, data.Length);

            accessor.WriteArray<byte>(PageSize * offset, page.Buffer, 0, PageSize);

            //Console.WriteLine("Wrote {0} bytes to page {1}", data.Length, offset);
        }
        finally
        {
            page.Semaphore.Release();
        }
    }
}