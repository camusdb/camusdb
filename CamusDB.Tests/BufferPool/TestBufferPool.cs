
using System.IO;
using System.Text;
using NUnit.Framework;
using System.Threading.Tasks;
using CamusDB.Core.BufferPool;
using System.IO.MemoryMappedFiles;
using CamusDB.Core.BufferPool.Models;
using Config = CamusDB.Core.CamusDBConfig;
using CamusDB.Core.CommandsExecutor.Controllers;

namespace CamusDB.Tests;

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
}
