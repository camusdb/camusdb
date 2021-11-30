
using System.IO;
using System.Text;
using NUnit.Framework;
using System.Threading.Tasks;
using CamusDB.Core.BufferPool;
using System.IO.MemoryMappedFiles;
using CamusDB.Core.BufferPool.Models;
using Config = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Tests.BufferPool;

public class TestBufferPool
{
    private const string TableSpacePath = "/tmp/tablespace0";

    [SetUp]
    public void Setup()
    {
        byte[] initialized = new byte[Config.InitialTableSpaceSize];
        File.Delete(TableSpacePath);
        File.WriteAllBytes(TableSpacePath, initialized);
    }

    [Test]
    public async Task TestGetPage()
    {
        using var mmf = MemoryMappedFile.CreateFromFile(TableSpacePath, FileMode.Open);
        using BufferPoolHandler bufferPool = new(mmf);

        BufferPage page = await bufferPool.GetPage(0);

        Assert.AreEqual(page.Buffer.Length, Config.PageSize);
        Assert.AreEqual(page.Offset, 0);

        page = await bufferPool.ReadPage(100);

        Assert.AreEqual(page.Buffer.Length, Config.PageSize);
        Assert.AreEqual(page.Offset, 100);

        Assert.AreEqual(bufferPool.NumberPages, 2);
    }

    [Test]
    public async Task TestReadPage()
    {
        using var mmf = MemoryMappedFile.CreateFromFile(TableSpacePath, FileMode.Open);
        using BufferPoolHandler bufferPool = new(mmf);

        BufferPage page = await bufferPool.ReadPage(0);

        Assert.AreEqual(page.Buffer.Length, Config.PageSize);
        Assert.AreEqual(page.Offset, 0);

        page = await bufferPool.ReadPage(100);

        Assert.AreEqual(page.Buffer.Length, Config.PageSize);
        Assert.AreEqual(page.Offset, 100);

        Assert.AreEqual(bufferPool.NumberPages, 2);
    }

    [Test]
    public async Task TestGetFreePage()
    {
        using var mmf = MemoryMappedFile.CreateFromFile(TableSpacePath, FileMode.Open);
        using BufferPoolHandler bufferPool = new(mmf);

        byte[] data = Encoding.UTF8.GetBytes("some data");

        for (int i = 0; i < 10; i++)
        {
            int freeOffset = await bufferPool.GetNextFreeOffset();
            Assert.AreEqual(i + 1, freeOffset);
        }
    }

    [Test]
    public async Task TestWriteSinglePage()
    {
        using var mmf = MemoryMappedFile.CreateFromFile(TableSpacePath, FileMode.Open);
        using BufferPoolHandler bufferPool = new(mmf);

        byte[] data = Encoding.UTF8.GetBytes("some data");

        await bufferPool.WriteDataToPage(1, data);

        BufferPage page = await bufferPool.ReadPage(1);

        Assert.AreEqual(page.Buffer.Length, Config.PageSize);
        Assert.AreEqual(page.Offset, 1);        
        
        Assert.AreEqual(bufferPool.NumberPages, 1); // page #1
    }

    [Test]
    public async Task TestWriteDataFlushed()
    {
        byte[] data = Encoding.UTF8.GetBytes("some data");

        using (var mmf = MemoryMappedFile.CreateFromFile(TableSpacePath, FileMode.Open))
        {
            using BufferPoolHandler? bufferPool = new(mmf);           
            await bufferPool.WriteDataToPage(1, data);
        }

        using var mmf2 = MemoryMappedFile.CreateFromFile(TableSpacePath, FileMode.Open);        
        using BufferPoolHandler? bufferPool2 = new(mmf2);

        BufferPage page = await bufferPool2.ReadPage(1);

        for (int i = 0; i < data.Length; i++)        
            Assert.AreEqual(page.Buffer[Config.DataOffset + i], data[i]);
    }

    [Test]
    public async Task TestWriteAndReadSinglePage()
    {
        using var mmf = MemoryMappedFile.CreateFromFile(TableSpacePath, FileMode.Open);
        using BufferPoolHandler bufferPool = new(mmf);

        byte[] data = Encoding.UTF8.GetBytes("some data some data");

        await bufferPool.WriteDataToPage(1, data);

        BufferPage page = await bufferPool.ReadPage(1);

        for (int i = 0; i < data.Length; i++)
            Assert.AreEqual(page.Buffer[Config.DataOffset + i], data[i]);
    }

    [Test]
    public async Task TestWriteAndReadDataFromPage()
    {
        using var mmf = MemoryMappedFile.CreateFromFile(TableSpacePath, FileMode.Open);
        using BufferPoolHandler bufferPool = new(mmf);

        byte[] data = Encoding.UTF8.GetBytes("some data some data");

        await bufferPool.WriteDataToPage(1, data);

        byte[] readData = await bufferPool.GetDataFromPage(1);

        for (int i = 0; i < data.Length; i++)
            Assert.AreEqual(readData[i], data[i]);
    }

    [Test]
    public async Task TestWriteLargeData()
    {
        using var mmf = MemoryMappedFile.CreateFromFile(TableSpacePath, FileMode.Open);
        using BufferPoolHandler bufferPool = new(mmf);

        byte[] data = Encoding.UTF8.GetBytes(new string('s', Config.PageSize));        

        int pageOffset = await bufferPool.WriteDataToFreePage(data);

        byte[] readData = await bufferPool.GetDataFromPage(pageOffset);

        Assert.AreEqual(readData.Length, Config.PageSize);
        Assert.AreEqual(data.Length, readData.Length);

        for (int i = 0; i < data.Length; i++)
            Assert.AreEqual(readData[i], data[i]);
    }

    [Test]
    public async Task TestMultiplePageLargeData()
    {
        using var mmf = MemoryMappedFile.CreateFromFile(TableSpacePath, FileMode.Open);
        using BufferPoolHandler bufferPool = new(mmf);

        byte[] data = Encoding.UTF8.GetBytes(new string('s', Config.PageSize * 5));

        int pageOffset = await bufferPool.WriteDataToFreePage(data);

        byte[] readData = await bufferPool.GetDataFromPage(pageOffset);

        Assert.AreEqual(readData.Length, Config.PageSize * 5);
        Assert.AreEqual(data.Length, readData.Length);

        for (int i = 0; i < data.Length; i++)
            Assert.AreEqual(readData[i], data[i]);
    }
}
