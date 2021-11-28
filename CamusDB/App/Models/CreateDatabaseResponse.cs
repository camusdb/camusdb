
namespace CamusDB.App.Models;

public class CreateDatabaseResponse
{
    public string Status { get; set; }

    public CreateDatabaseResponse(string status)
    {
        Status = status;
    }
}
