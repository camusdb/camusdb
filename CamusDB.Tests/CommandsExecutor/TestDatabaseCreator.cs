
using System.IO;
using System.Text;
using NUnit.Framework;
using System.Threading.Tasks;
using CamusDB.Core.BufferPool;
using System.IO.MemoryMappedFiles;
using CamusDB.Core.BufferPool.Models;
using Config = CamusDB.Core.CamusDBConfig;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.Catalogs;

namespace CamusDB.Tests.CommandsExecutor;

public class TestDatabaseCreator
{
    [SetUp]
    public void Setup()
    {
        string path = Config.DataDirectory + "/test";
        if (Directory.Exists(path))
        {
            File.Delete(path + "/tablespace0");
            File.Delete(path + "/schema");
            File.Delete(path + "/system");
            Directory.Delete(path);
        }
    }

    [Test]
    public async Task TestCreateDatabase()
    {
        CatalogsManager catalogsManager = new();
        CommandExecutor executor = new(catalogsManager);        

        await executor.CreateDatabase("test");

        string path = Config.DataDirectory + "/test";

        Assert.IsTrue(Directory.Exists(path));
        Assert.IsTrue(File.Exists(path + "/tablespace0"));
        Assert.IsTrue(File.Exists(path + "/schema"));
        Assert.IsTrue(File.Exists(path + "/system"));
    }
}

