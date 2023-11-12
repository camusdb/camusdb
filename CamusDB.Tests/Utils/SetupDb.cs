
using System.IO;
using CamusConfig = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Tests.Utils;

public static class SetupDb
{
    public static void Remove(string dbName)
    {
        string path = Path.Combine(CamusConfig.DataDirectory, dbName);
        if (!Directory.Exists(path))
            return;

        string[] fileEntries = Directory.GetFiles(path);
        foreach (string fileName in fileEntries)
            File.Delete(fileName);
        
        Directory.Delete(path);
    }
}
