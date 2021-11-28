
using System;
using System.Collections.Concurrent;
using System.IO.MemoryMappedFiles;
using CamusDB.Library.Serializer;
using CamusDB.Library.BufferPool.Models;

namespace CamusDB.Library.BufferPool;

public class BufferPoolHandler
{
    private const int PageSize = 1024;

    private readonly MemoryMappedFile memoryFile;

    private readonly Dictionary<int, MemoryPage> pages = new();

    private readonly SemaphoreSlim semaphore = new(1, 1);

    private int rowId = 0;

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

            accessor.ReadArray<byte>(offset * PageSize, page.Buffer, 0, PageSize);
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

        int length = Serializator.UnpackInt32(memoryPage.Buffer, ref pointer);
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

            if (i == offset)
            {
                Console.WriteLine(data.Length);
                Serializator.WriteInt32ToBuffer(memoryPage.Buffer, data.Length, ref pointer);
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

    public async Task<int> GetNextFreeOffset()
    {
        try
        {
            await semaphore.WaitAsync();

            using var accessor = memoryFile.CreateViewAccessor(0, PageSize);

            if (!pages.TryGetValue(0, out MemoryPage? page))
            {
                page = new MemoryPage(0, new byte[PageSize]);
                accessor.ReadArray<byte>(0, page.Buffer, 0, PageSize);
                pages.Add(0, page);
            }

            int pointer = 0, pageOffset = 1;

            int length = Serializator.UnpackInt32(page.Buffer, ref pointer);
            if (length == 0)
            {
                pointer = 0;
                Serializator.WriteInt32ToBuffer(page.Buffer, 4, ref pointer);

                pointer = 8;
                Serializator.WriteInt32ToBuffer(page.Buffer, 0, ref pointer);
            }
            else
            {
                pointer = 4;
                pageOffset = Serializator.UnpackInt32(page.Buffer, ref pointer);
            }

            pointer = 4;
            Serializator.WriteInt32ToBuffer(page.Buffer, pageOffset + 1, ref pointer);

            accessor.WriteArray<byte>(0, page.Buffer, 0, PageSize);

            return pageOffset;
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

            if (!pages.TryGetValue(0, out MemoryPage? page))
            {
                page = new MemoryPage(0, new byte[PageSize]);
                accessor.ReadArray<byte>(0, page.Buffer, 0, PageSize);
                pages.Add(0, page);
            }

            int pointer = 8;
            Serializator.WriteInt32ToBuffer(page.Buffer, rowId + 1, ref pointer);

            accessor.WriteArray<byte>(0, page.Buffer, 0, PageSize);

            return rowId++;
        }
        finally
        {
            semaphore.Release();
        }
    }
}