
namespace CamusDB.Core.CommandsExecutor.Controllers;

public sealed class DatabaseCreator
{
    private const int InitialTableSpaceSize = 1024 * 4096; // 4096 blocks of 1024 size

    public async Task Create(string name)
    {
        name = name.ToLowerInvariant(); // @todo validate database name

        if (Directory.Exists("Data/" + name))
            throw new CamusDBException(CamusDBErrorCodes.DatabaseAlreadyExists, "Database already exists");

        Directory.CreateDirectory("Data/" + name);

        await InitializeDatabaseFiles(name);
    }

    private static async Task InitializeDatabaseFiles(string name)
    {
        byte[] initialized = new byte[InitialTableSpaceSize];

        await Task.WhenAll(new Task[]
        {
            File.WriteAllBytesAsync("Data/" + name + "/tablespace0", initialized),
            File.WriteAllBytesAsync("Data/" + name + "/schema", initialized),
            File.WriteAllBytesAsync("Data/" + name + "/system", initialized)
        });

        // @todo catch IO Exceptions
        // @todo verify tablespaces were created sucessfully

        Console.WriteLine("Database tablespaces created");
    }
}
