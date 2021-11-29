
namespace CamusDB.App.Models;

public sealed class InsertResponse
{
    public string Status { get; set; }

    public InsertResponse(string status)
    {
        Status = status;
    }
}
