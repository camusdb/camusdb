
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Text;
using System.Threading.Tasks;
using RocksDbSharp;
using NUnit.Framework;
using CamusDB.Core.Storage;
using CamusDB.Core.BufferPool;
using CamusDB.Core.BufferPool.Models;
using CamusDB.Core.Util.ObjectIds;

using Config = CamusDB.Core.CamusDBConfig;
using BConfig = CamusDB.Core.BufferPool.Models.BufferPoolConfig;

namespace CamusDB.Tests.BufferPool;

public class TestBufferPool
{
    private const string TableSpacePath = "/tmp/";

    [SetUp]
    public void Setup()
    {
        //File.Delete(Path.Combine(TableSpacePath, "tablespace000"));
    }

    private RocksDb GetTempRocksDb()
    {
        DbOptions options = new DbOptions().SetCreateIfMissing(true);

        return RocksDb.Open(options, TableSpacePath + "/" + Guid.NewGuid().ToString());
    }

    [Test]
    [NonParallelizable]
    public void TestGetPage()
    {
        using RocksDb storage = GetTempRocksDb();

        StorageManager tablespaceStorage = new(storage);
        //await tablespaceStorage.Initialize();

        using BufferPoolHandler bufferPool = new(tablespaceStorage);

        ObjectIdValue offset = ObjectIdGenerator.Generate();

        BufferPage page = bufferPool.GetPage(offset);

        Assert.AreEqual(page.Buffer.Value.Length, 4096);
        Assert.AreEqual(page.Offset, offset);

        offset = ObjectIdGenerator.Generate();

        page = bufferPool.ReadPage(offset);

        Assert.AreEqual(page.Buffer.Value.Length, Config.PageSize);
        Assert.AreEqual(page.Offset, offset);

        Assert.AreEqual(bufferPool.NumberPages, 2);
    }

    [Test]
    [NonParallelizable]
    public void TestReadPage()
    {
        using RocksDb storage = GetTempRocksDb();

        StorageManager tablespaceStorage = new(storage);

        using BufferPoolHandler bufferPool = new(tablespaceStorage);

        ObjectIdValue offset = ObjectIdGenerator.Generate();

        BufferPage page = bufferPool.ReadPage(offset);

        Assert.AreEqual(page.Buffer.Value.Length, Config.PageSize);
        Assert.AreEqual(page.Offset, offset);

        offset = ObjectIdGenerator.Generate();

        page = bufferPool.ReadPage(offset);

        Assert.AreEqual(page.Buffer.Value.Length, Config.PageSize);
        Assert.AreEqual(page.Offset, offset);

        Assert.AreEqual(bufferPool.NumberPages, 2);
    }    

    [Test]
    [NonParallelizable]
    public async Task TestWriteSinglePage()
    {
        using RocksDb storage = GetTempRocksDb();

        StorageManager tablespaceStorage = new(storage);

        using BufferPoolHandler bufferPool = new(tablespaceStorage);

        ObjectIdValue offset = ObjectIdGenerator.Generate();

        byte[] data = Encoding.UTF8.GetBytes("some data");

        await bufferPool.WriteDataToPage(offset, 0, data);

        BufferPage page = bufferPool.ReadPage(offset);

        Assert.AreEqual(page.Buffer.Value.Length, Config.PageSize);
        Assert.AreEqual(page.Offset, offset);

        Assert.AreEqual(bufferPool.NumberPages, 1); // page #1
    }

    [Test]
    [NonParallelizable]
    public async Task TestWriteDataFlushed()
    {
        using RocksDb storage = GetTempRocksDb();

        byte[] data = Encoding.UTF8.GetBytes("some data");

        StorageManager tablespaceStorage = new(storage);
        //await tablespaceStorage.Initialize();

        using BufferPoolHandler bufferPool = new(tablespaceStorage);

        ObjectIdValue offset = ObjectIdGenerator.Generate();

        await bufferPool.WriteDataToPage(offset, 0, data);

        bufferPool.Dispose();

        StorageManager tablespaceStorage2 = new(storage);
        //await tablespaceStorage2.Initialize();

        using BufferPoolHandler? bufferPool2 = new(tablespaceStorage2);

        BufferPage page = bufferPool2.ReadPage(offset);

        for (int i = 0; i < data.Length; i++)
            Assert.AreEqual(page.Buffer.Value[BConfig.DataOffset + i], data[i]);
    }

    [Test]
    [NonParallelizable]
    public async Task TestWriteAndReadSinglePage()
    {
        using RocksDb storage = GetTempRocksDb();

        StorageManager tablespaceStorage = new(storage);
        //await tablespaceStorage.Initialize();

        using BufferPoolHandler bufferPool = new(tablespaceStorage);

        byte[] data = Encoding.UTF8.GetBytes("some data some data");

        ObjectIdValue offset = ObjectIdGenerator.Generate();

        await bufferPool.WriteDataToPage(offset, 0, data);

        BufferPage page = bufferPool.ReadPage(offset);

        for (int i = 0; i < data.Length; i++)
            Assert.AreEqual(page.Buffer.Value[BConfig.DataOffset + i], data[i]);
    }

    [Test]
    [NonParallelizable]
    public async Task TestWriteAndReadDataFromPage()
    {
        using RocksDb storage = GetTempRocksDb();

        StorageManager tablespaceStorage = new(storage);

        using BufferPoolHandler bufferPool = new(tablespaceStorage);

        ObjectIdValue offset = ObjectIdGenerator.Generate();

        byte[] data = Encoding.UTF8.GetBytes("some data some data");

        await bufferPool.WriteDataToPage(offset, 0, data);

        byte[] readData = await bufferPool.GetDataFromPage(offset);

        for (int i = 0; i < data.Length; i++)
            Assert.AreEqual(readData[i], data[i]);
    }

    [Test]
    [NonParallelizable]
    public async Task TestWriteLargeData()
    {
        using RocksDb storage = GetTempRocksDb();

        StorageManager tablespaceStorage = new(storage);

        using BufferPoolHandler bufferPool = new(tablespaceStorage);

        byte[] data = Encoding.UTF8.GetBytes(new string('s', Config.PageSize));

        ObjectIdValue pageOffset = await bufferPool.WriteDataToFreePage(data);

        byte[] readData = await bufferPool.GetDataFromPage(pageOffset);

        Assert.AreEqual(Config.PageSize, readData.Length);
        Assert.AreEqual(data.Length, readData.Length);

        for (int i = 0; i < data.Length; i++)
            Assert.AreEqual(readData[i], data[i]);
    }

    [Test]
    [NonParallelizable]
    public async Task TestMultiplePageLargeData()
    {
        using RocksDb storage = GetTempRocksDb();

        StorageManager tablespaceStorage = new(storage);

        using BufferPoolHandler bufferPool = new(tablespaceStorage);

        byte[] data = Encoding.UTF8.GetBytes(new string('s', Config.PageSize * 5));

        ObjectIdValue pageOffset = await bufferPool.WriteDataToFreePage(data);

        byte[] readData = await bufferPool.GetDataFromPage(pageOffset);

        Assert.AreEqual(readData.Length, Config.PageSize * 5);
        Assert.AreEqual(data.Length, readData.Length);

        for (int i = 0; i < data.Length; i++)
            Assert.AreEqual(readData[i], data[i]);
    }
}
