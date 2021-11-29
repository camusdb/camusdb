
namespace CamusDB.App.Models;

public sealed class InsertResponse
{
    public string Status { get; set; }

    public string? Code { get; set; }

    public string? Message { get; set; }

    public InsertResponse(string status)
    {
        Status = status;
    }

    public InsertResponse(string status, string code, string message)
    {
        Status = status;
        Code = code;
        Message = message;
    }
}
