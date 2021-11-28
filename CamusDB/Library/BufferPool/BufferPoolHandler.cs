
using System;
using System.Collections.Concurrent;
using System.IO.MemoryMappedFiles;
using CamusDB.Library.Serializer;
using CamusDB.Library.BufferPool.Models;

namespace CamusDB.Library.BufferPool;

public class BufferPoolHandler
{
    private const int PageSize = 1024;

    private const int TableSpaceHeaderPage = 0;

    private readonly MemoryMappedFile memoryFile;

    private readonly Dictionary<int, MemoryPage> pages = new();

    private readonly SemaphoreSlim semaphore = new(1, 1);

    public BufferPoolHandler(MemoryMappedFile memoryFile)
    {
        this.memoryFile = memoryFile;
    }

    public async Task<MemoryPage> ReadPage(int offset)
    {
        if (pages.TryGetValue(offset, out MemoryPage? page))
            return page;

        try
        {
            await semaphore.WaitAsync();

            if (pages.TryGetValue(offset, out page))
                return page;

            page = new MemoryPage(offset, new byte[PageSize]);

            using var accessor = memoryFile.CreateViewAccessor(offset * PageSize, PageSize);

            accessor.ReadArray<byte>(0, page.Buffer, 0, PageSize);
        }
        finally
        {
            semaphore.Release();
        }

        return page;
    }

    public async Task<byte[]> GetDataFromPage(int offset)
    {
        MemoryPage memoryPage = await ReadPage(offset);

        int pointer = 0;

        int length = Serializator.ReadInt32(memoryPage.Buffer, ref pointer);
        if (length == 0)
            return Array.Empty<byte>();

        //Console.WriteLine(length);

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
            MemoryPage memoryPage = await ReadPage(i);

            if (i == offset) // fist page
            {                
                Serializator.WriteInt32(memoryPage.Buffer, data.Length, ref pointer); // write length to first 4 bytes 
                Buffer.BlockCopy(data, 0, memoryPage.Buffer, 4, remaining > PageSize ? PageSize : remaining);
            }
            else
            {
                Buffer.BlockCopy(data, pointer, memoryPage.Buffer, 0, remaining > PageSize ? PageSize : remaining);
            }

            pointer += PageSize;
            remaining -= PageSize;

            await FlushPage(memoryPage);
        }
    }

    public async Task FlushPage(MemoryPage memoryPage)
    {
        try
        {
            await semaphore.WaitAsync();

            using var accessor = memoryFile.CreateViewAccessor(memoryPage.Offset * PageSize, PageSize);

            accessor.WriteArray<byte>(0, memoryPage.Buffer, 0, PageSize);

            accessor.Flush();
        }
        finally
        {
            semaphore.Release();
        }
    }

    private int GetNextFreePageOffsetInternal()
    {
        using var accessor = memoryFile.CreateViewAccessor(0, PageSize);

        if (!pages.TryGetValue(TableSpaceHeaderPage, out MemoryPage? page))
        {
            page = new MemoryPage(TableSpaceHeaderPage, new byte[PageSize]);
            accessor.ReadArray<byte>(0, page.Buffer, 0, PageSize);
            pages.Add(TableSpaceHeaderPage, page);
        }

        int pointer = 0, pageOffset = 1;

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

        accessor.WriteArray<byte>(0, page.Buffer, 0, PageSize);

        Console.WriteLine("CurrentPageOffset={0} NextPageOffset={1}", pageOffset, pageOffset + 1);

        return pageOffset;
    }

    public async Task<int> GetNextFreeOffset()
    {
        try
        {
            await semaphore.WaitAsync();

            return GetNextFreePageOffsetInternal();
        }
        finally
        {
            semaphore.Release();
        }
    }

    public async Task<int> GetNextRowId()
    {
        try
        {
            await semaphore.WaitAsync();

            using var accessor = memoryFile.CreateViewAccessor(0, PageSize);

            if (!pages.TryGetValue(TableSpaceHeaderPage, out MemoryPage? page))
            {
                page = new MemoryPage(TableSpaceHeaderPage, new byte[PageSize]);
                accessor.ReadArray<byte>(0, page.Buffer, 0, PageSize);
                pages.Add(TableSpaceHeaderPage, page);
            }

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

            Console.WriteLine("CurrentRowId={0} NextRowId={1}", rowId, rowId + 1);

            accessor.WriteArray<byte>(0, page.Buffer, 0, PageSize);

            return rowId;
        }
        finally
        {
            semaphore.Release();
        }
    }

    public async Task<int> WriteDataToFreePage(byte[] data)
    {
        try
        {
            await semaphore.WaitAsync();

            int freeOffset = GetNextFreePageOffsetInternal();

            using var accessor = memoryFile.CreateViewAccessor(freeOffset * PageSize, PageSize);

            if (!pages.TryGetValue(freeOffset, out MemoryPage? page))
            {
                page = new MemoryPage(freeOffset, new byte[PageSize]);
                pages.Add(freeOffset, page);
            }
            
            int pointer = 0;
            Serializator.WriteInt32(page.Buffer, 4, ref pointer); // data length (4 bytes integer)

            Buffer.BlockCopy(data, 0, page.Buffer, 4, data.Length); 

            accessor.WriteArray<byte>(0, page.Buffer, 0, PageSize);

            return freeOffset;
        }
        finally
        {
            semaphore.Release();
        }
    }
}