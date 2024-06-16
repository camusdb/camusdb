
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

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

public class TestBufferPool : BaseTest
{
    private const string TableSpacePath = "/tmp/";

    [SetUp]
    public void Setup()
    {
        //File.Delete(Path.Combine(TableSpacePath, "tablespace000"));
    }

    [Test]
    [NonParallelizable]
    public void TestReadPage()
    {
        StorageManager tablespaceStorage = new(Guid.NewGuid().ToString());

        BufferPoolManager bufferPool = new(tablespaceStorage, new(), logger);

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
        StorageManager tablespaceStorage = new(Guid.NewGuid().ToString());

        BufferPoolManager bufferPool = new(tablespaceStorage, new(), logger);

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
        string name = Guid.NewGuid().ToString();
        
        using StorageManager tablespaceStorage = new(name);

        byte[] data = Encoding.Unicode.GetBytes("some data");
        
        //await tablespaceStorage.Initialize();

        BufferPoolManager bufferPool = new(tablespaceStorage, new(), logger);

        ObjectIdValue offset = ObjectIdGenerator.Generate();

        await bufferPool.WriteDataToPage(offset, 0, data);                

        StorageManager tablespaceStorage2 = new(name);
        //await tablespaceStorage2.Initialize();

        BufferPoolManager? bufferPool2 = new(tablespaceStorage2, new(), logger);

        BufferPage page = bufferPool2.ReadPage(offset);

        for (int i = 0; i < data.Length; i++)
            Assert.AreEqual(page.Buffer.Value[BConfig.DataOffset + i], data[i]);
    }

    [Test]
    [NonParallelizable]
    public async Task TestWriteAndReadSinglePage()
    {
        using StorageManager tablespaceStorage = new(Guid.NewGuid().ToString());
        //await tablespaceStorage.Initialize();

        BufferPoolManager bufferPool = new(tablespaceStorage, new(), logger);

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
        using StorageManager tablespaceStorage = new(Guid.NewGuid().ToString());

        BufferPoolManager bufferPool = new(tablespaceStorage, new(), logger);

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
        using StorageManager tablespaceStorage = new(Guid.NewGuid().ToString());

        BufferPoolManager bufferPool = new(tablespaceStorage, new(), logger);

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
        using StorageManager tablespaceStorage = new(Guid.NewGuid().ToString());

        BufferPoolManager bufferPool = new(tablespaceStorage, new(), logger);

        byte[] data = Encoding.UTF8.GetBytes(new string('s', CamusConfig.PageSize * 5));

        ObjectIdValue pageOffset = await bufferPool.WriteDataToFreePage(data);

        byte[] readData = await bufferPool.GetDataFromPage(pageOffset);

        Assert.AreEqual(readData.Length, CamusConfig.PageSize * 5);
        Assert.AreEqual(data.Length, readData.Length);

        for (int i = 0; i < data.Length; i++)
            Assert.AreEqual(readData[i], data[i]);
    }
}
