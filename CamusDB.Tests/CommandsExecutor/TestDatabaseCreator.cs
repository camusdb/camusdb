﻿
using System.IO;
using NUnit.Framework;
using System.Threading.Tasks;

using CamusDB.Tests.Utils;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsValidator;
using Config = CamusDB.Core.CamusDBConfig;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Tests.CommandsExecutor;

public class TestDatabaseCreator
{
    [SetUp]
    public void Setup()
    {
        SetupDb.Remove("test");
    }

    [Test]
    [NonParallelizable]
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

