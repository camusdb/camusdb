
namespace CamusDB.App.Models;

public sealed class CreateDatabaseResponse
{
    public string Status { get; set; }

    public CreateDatabaseResponse(string status)
    {
        Status = status;
    }
}
