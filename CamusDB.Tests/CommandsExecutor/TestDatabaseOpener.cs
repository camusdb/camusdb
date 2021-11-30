
using System.IO;
using System.Text;
using NUnit.Framework;
using CamusDB.Core.Catalogs;
using System.Threading.Tasks;
using CamusDB.Core.BufferPool;
using System.IO.MemoryMappedFiles;
using CamusDB.Core.BufferPool.Models;
using Config = CamusDB.Core.CamusDBConfig;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Tests.CommandsExecutor;

public class TestDatabaseOpener
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
    public async Task TestOpenDatabase()
    {
        CommandValidator validator = new();
        CatalogsManager catalogsManager = new();
        CommandExecutor executor = new(validator, catalogsManager);

        await executor.CreateDatabase("test");

        DatabaseDescriptor database = await executor.OpenDatabase("test");

        Assert.AreEqual("test", database.Name);

        Assert.IsInstanceOf<BufferPoolHandler>(database.SchemaSpace);
        Assert.IsInstanceOf<BufferPoolHandler>(database.SystemSpace);
        Assert.IsInstanceOf<BufferPoolHandler>(database.TableSpace);

        Assert.IsInstanceOf<SystemSchema>(database.SystemSchema);
        Assert.IsInstanceOf<Schema>(database.Schema);

        Assert.AreEqual(database.TableDescriptors.Count, 0);
    }
}
