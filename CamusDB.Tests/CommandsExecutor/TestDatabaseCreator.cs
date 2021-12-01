
using System.IO;
using System.Text;
using NUnit.Framework;
using CamusDB.Core.Catalogs;
using System.Threading.Tasks;
using CamusDB.Core.BufferPool;
using CamusDB.Core.CommandsExecutor;
using System.IO.MemoryMappedFiles;
using CamusDB.Core.BufferPool.Models;
using CamusDB.Core.CommandsValidator;
using Config = CamusDB.Core.CamusDBConfig;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

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
        CommandValidator validator = new();
        CatalogsManager catalogsManager = new();
        CommandExecutor executor = new(validator, catalogsManager);

        CreateDatabaseTicket databaseTicket = new(
            name: "test"
        );

        await executor.CreateDatabase(databaseTicket);

        string path = Config.DataDirectory + "/test";

        Assert.IsTrue(Directory.Exists(path));

        string[] tablespaces = new string[] { "tablespace0", "schema", "system" };
        for (int i = 0; i < tablespaces.Length; i++)
        {
            Assert.IsTrue(File.Exists(path + "/" + tablespaces[i]));

            FileInfo fi = new(path + "/" + tablespaces[i]);
            Assert.AreEqual(fi.Length, Config.InitialTableSpaceSize);
        }                       
    }
}

