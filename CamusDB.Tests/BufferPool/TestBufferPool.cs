
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using RocksDbSharp;
using NUnit.Framework;

using System;
using System.Text;
using System.Threading.Tasks;

using CamusDB.Core.Storage;
using CamusDB.Core.BufferPool;
using CamusDB.Core.BufferPool.Models;
using CamusDB.Core.Util.ObjectIds;

using CamusConfig = CamusDB.Core.CamusDBConfig;
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
    public void TestReadPage()
    {
        using RocksDb storage = GetTempRocksDb();

        StorageManager tablespaceStorage = new(storage);

        using BufferPoolManager bufferPool = new(tablespaceStorage, new());

        ObjectIdValue offset = ObjectIdGenerator.Generate();

        BufferPage page = bufferPool.ReadPage(offset);

        Assert.AreEqual(page.Buffer.Value.Length, CamusConfig.PageSize);
        Assert.AreEqual(page.Offset, offset);

        offset = ObjectIdGenerator.Generate();

        page = bufferPool.ReadPage(offset);

        Assert.AreEqual(page.Buffer.Value.Length, CamusConfig.PageSize);
        Assert.AreEqual(page.Offset, offset);

        Assert.AreEqual(bufferPool.NumberPages, 2);
    }    

    [Test]
    [NonParallelizable]
    public async Task TestWriteSinglePage()
    {
        using RocksDb storage = GetTempRocksDb();

        StorageManager tablespaceStorage = new(storage);

        using BufferPoolManager bufferPool = new(tablespaceStorage, new());

        ObjectIdValue offset = ObjectIdGenerator.Generate();

        byte[] data = Encoding.Unicode.GetBytes("some data");

        await bufferPool.WriteDataToPage(offset, 0, data);

        BufferPage page = bufferPool.ReadPage(offset);

        Assert.AreEqual(page.Buffer.Value.Length, CamusConfig.PageSize);
        Assert.AreEqual(page.Offset, offset);

        Assert.AreEqual(bufferPool.NumberPages, 1); // page #1
    }

    [Test]
    [NonParallelizable]
    public async Task TestWriteDataFlushed()
    {
        using RocksDb storage = GetTempRocksDb();

        byte[] data = Encoding.Unicode.GetBytes("some data");

        StorageManager tablespaceStorage = new(storage);
        //await tablespaceStorage.Initialize();

        using BufferPoolManager bufferPool = new(tablespaceStorage, new());

        ObjectIdValue offset = ObjectIdGenerator.Generate();

        await bufferPool.WriteDataToPage(offset, 0, data);

        bufferPool.Dispose();

        StorageManager tablespaceStorage2 = new(storage);
        //await tablespaceStorage2.Initialize();

        using BufferPoolManager? bufferPool2 = new(tablespaceStorage2, new());

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

        using BufferPoolManager bufferPool = new(tablespaceStorage, new());

        byte[] data = Encoding.Unicode.GetBytes("some data some data");

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

        using BufferPoolManager bufferPool = new(tablespaceStorage, new());

        ObjectIdValue offset = ObjectIdGenerator.Generate();

        byte[] data = Encoding.Unicode.GetBytes("some data some data");

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

        using BufferPoolManager bufferPool = new(tablespaceStorage, new());

        byte[] data = Encoding.UTF8.GetBytes(new string('s', CamusConfig.PageSize));

        ObjectIdValue pageOffset = await bufferPool.WriteDataToFreePage(data);

        byte[] readData = await bufferPool.GetDataFromPage(pageOffset);

        Assert.AreEqual(CamusConfig.PageSize, readData.Length);
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

        using BufferPoolManager bufferPool = new(tablespaceStorage, new());

        byte[] data = Encoding.UTF8.GetBytes(new string('s', CamusConfig.PageSize * 5));

        ObjectIdValue pageOffset = await bufferPool.WriteDataToFreePage(data);

        byte[] readData = await bufferPool.GetDataFromPage(pageOffset);

        Assert.AreEqual(readData.Length, CamusConfig.PageSize * 5);
        Assert.AreEqual(data.Length, readData.Length);

        for (int i = 0; i < data.Length; i++)
            Assert.AreEqual(readData[i], data[i]);
    }
}
