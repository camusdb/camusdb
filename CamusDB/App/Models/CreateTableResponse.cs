
namespace CamusDB.App.Models;

public sealed class CreateTableResponse
{
    public string Status { get; set; }

    public string? Code { get; set; }

    public string? Message { get; set; }

    public CreateTableResponse(string status)
    {
        Status = status;
    }

    public CreateTableResponse(string status, string code, string message)
    {
        Status = status;
        Code = code;
        Message = message;
    }
}
