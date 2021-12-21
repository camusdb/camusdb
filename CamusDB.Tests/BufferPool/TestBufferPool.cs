
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.IO;
using System.Text;
using NUnit.Framework;
using CamusDB.Core.Storage;
using System.Threading.Tasks;
using CamusDB.Core.BufferPool;
using CamusDB.Core.BufferPool.Models;
using Config = CamusDB.Core.CamusDBConfig;
using BConfig = CamusDB.Core.BufferPool.Models.BufferPoolConfig;

namespace CamusDB.Tests.BufferPool;

public class TestBufferPool
{
    private const string TableSpacePath = "/tmp/";

    [SetUp]
    public void Setup()
    {
        File.Delete(Path.Combine(TableSpacePath, "tablespace000"));
    }

    [Test]
    [NonParallelizable]
    public async Task TestGetPage()
    {
        StorageManager tablespaceStorage = new(TableSpacePath, "tablespace");
        await tablespaceStorage.Initialize();

        using BufferPoolHandler bufferPool = new(tablespaceStorage);

        BufferPage page = await bufferPool.GetPage(0);

        Assert.AreEqual(page.Buffer.Length, Config.PageSize);
        Assert.AreEqual(page.Offset, 0);

        page = await bufferPool.ReadPage(100);

        Assert.AreEqual(page.Buffer.Length, Config.PageSize);
        Assert.AreEqual(page.Offset, 100);

        Assert.AreEqual(bufferPool.NumberPages, 2);
    }

    [Test]
    [NonParallelizable]
    public async Task TestReadPage()
    {
        StorageManager tablespaceStorage = new(TableSpacePath, "tablespace");
        await tablespaceStorage.Initialize();

        using BufferPoolHandler bufferPool = new(tablespaceStorage);

        BufferPage page = await bufferPool.ReadPage(0);

        Assert.AreEqual(page.Buffer.Length, Config.PageSize);
        Assert.AreEqual(page.Offset, 0);

        page = await bufferPool.ReadPage(100);

        Assert.AreEqual(page.Buffer.Length, Config.PageSize);
        Assert.AreEqual(page.Offset, 100);

        Assert.AreEqual(bufferPool.NumberPages, 2);
    }

    [Test]
    [NonParallelizable]
    public async Task TestGetFreePage()
    {
        StorageManager tablespaceStorage = new(TableSpacePath, "tablespace");
        await tablespaceStorage.Initialize();

        using BufferPoolHandler bufferPool = new(tablespaceStorage);

        // Initialize tablespace header
        BufferPage page = await bufferPool.ReadPage(Config.TableSpaceHeaderPage);
        bufferPool.WriteTableSpaceHeader(page.Buffer);
        await bufferPool.FlushPage(page); 

        byte[] data = Encoding.UTF8.GetBytes("some data");

        for (int i = 0; i < 10; i++)
        {
            int freeOffset = await bufferPool.GetNextFreeOffset();
            Assert.AreEqual(i + 1, freeOffset);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task TestWriteSinglePage()
    {
        StorageManager tablespaceStorage = new(TableSpacePath, "tablespace");
        await tablespaceStorage.Initialize();

        using BufferPoolHandler bufferPool = new(tablespaceStorage);

        byte[] data = Encoding.UTF8.GetBytes("some data");

        await bufferPool.WriteDataToPage(1, 0, data);

        BufferPage page = await bufferPool.ReadPage(1);

        Assert.AreEqual(page.Buffer.Length, Config.PageSize);
        Assert.AreEqual(page.Offset, 1);

        Assert.AreEqual(bufferPool.NumberPages, 1); // page #1
    }

    [Test]
    [NonParallelizable]
    public async Task TestWriteDataFlushed()
    {
        byte[] data = Encoding.UTF8.GetBytes("some data");

        StorageManager tablespaceStorage = new(TableSpacePath, "tablespace");
        await tablespaceStorage.Initialize();

        using BufferPoolHandler bufferPool = new(tablespaceStorage);        

        await bufferPool.WriteDataToPage(1, 0, data);

        bufferPool.Dispose();

        StorageManager tablespaceStorage2 = new(TableSpacePath, "tablespace");
        await tablespaceStorage2.Initialize();

        using BufferPoolHandler? bufferPool2 = new(tablespaceStorage2);

        BufferPage page = await bufferPool2.ReadPage(1);

        for (int i = 0; i < data.Length; i++)
            Assert.AreEqual(page.Buffer[BConfig.DataOffset + i], data[i]);
    }

    [Test]
    [NonParallelizable]
    public async Task TestWriteAndReadSinglePage()
    {
        StorageManager tablespaceStorage = new(TableSpacePath, "tablespace");
        await tablespaceStorage.Initialize();

        using BufferPoolHandler bufferPool = new(tablespaceStorage);

        byte[] data = Encoding.UTF8.GetBytes("some data some data");

        await bufferPool.WriteDataToPage(1, 0, data);

        BufferPage page = await bufferPool.ReadPage(1);

        for (int i = 0; i < data.Length; i++)
            Assert.AreEqual(page.Buffer[BConfig.DataOffset + i], data[i]);
    }

    [Test]
    [NonParallelizable]
    public async Task TestWriteAndReadDataFromPage()
    {
        StorageManager tablespaceStorage = new(TableSpacePath, "tablespace");
        await tablespaceStorage.Initialize();

        using BufferPoolHandler bufferPool = new(tablespaceStorage);        

        byte[] data = Encoding.UTF8.GetBytes("some data some data");

        await bufferPool.WriteDataToPage(1, 0, data);

        byte[] readData = await bufferPool.GetDataFromPage(1);

        for (int i = 0; i < data.Length; i++)
            Assert.AreEqual(readData[i], data[i]);
    }

    [Test]
    [NonParallelizable]
    public async Task TestWriteLargeData()
    {
        StorageManager tablespaceStorage = new(TableSpacePath, "tablespace");
        await tablespaceStorage.Initialize();

        using BufferPoolHandler bufferPool = new(tablespaceStorage);        

        byte[] data = Encoding.UTF8.GetBytes(new string('s', Config.PageSize));

        int pageOffset = await bufferPool.WriteDataToFreePage(data);

        byte[] readData = await bufferPool.GetDataFromPage(pageOffset);

        Assert.AreEqual(readData.Length, Config.PageSize);
        Assert.AreEqual(data.Length, readData.Length);

        for (int i = 0; i < data.Length; i++)
            Assert.AreEqual(readData[i], data[i]);
    }

    [Test]
    [NonParallelizable]
    public async Task TestMultiplePageLargeData()
    {
        StorageManager tablespaceStorage = new(TableSpacePath, "tablespace");
        await tablespaceStorage.Initialize();

        using BufferPoolHandler bufferPool = new(tablespaceStorage);

        byte[] data = Encoding.UTF8.GetBytes(new string('s', Config.PageSize * 5));

        int pageOffset = await bufferPool.WriteDataToFreePage(data);

        byte[] readData = await bufferPool.GetDataFromPage(pageOffset);

        Assert.AreEqual(readData.Length, Config.PageSize * 5);
        Assert.AreEqual(data.Length, readData.Length);

        for (int i = 0; i < data.Length; i++)
            Assert.AreEqual(readData[i], data[i]);
    }
}
