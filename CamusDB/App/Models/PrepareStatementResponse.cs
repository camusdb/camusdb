/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.App.Models;

/// <summary>
/// Reply to <c>/prepare-sql-statement</c>. <see cref="ParameterNames"/> is the binding order the
/// client must follow: the value it later sends at index <c>i</c> binds to the name at index
/// <c>i</c>. Names keep their leading <c>@</c>, so a client that prefers binding by name can map its
/// own arguments onto ordinals without parsing the SQL itself.
/// </summary>
public sealed class PrepareStatementResponse
{
    public string Status { get; set; }

    /// <summary>
    /// Opaque handle, valid on this node only and only for the principal that prepared it. An
    /// execution that is told this id is unknown should prepare again and replay once — that is
    /// expected after an idle period, a restart, or a request routed to another node.
    /// </summary>
    public string? StatementId { get; set; }

    public IReadOnlyList<string>? ParameterNames { get; set; }

    public string? Code { get; set; }

    public string? Message { get; set; }

    public double? ServerTimeMs { get; set; }

    public PrepareStatementResponse(string status, string statementId, IReadOnlyList<string> parameterNames)
    {
        Status = status;
        StatementId = statementId;
        ParameterNames = parameterNames;
    }

    public PrepareStatementResponse(string status, string code, string message)
    {
        Status = status;
        Code = code;
        Message = message;
    }
}

/// <summary>Reply to <c>/close-sql-statement</c>.</summary>
public sealed class CloseStatementResponse
{
    public string Status { get; set; }

    public string? Code { get; set; }

    public string? Message { get; set; }

    public CloseStatementResponse(string status) => Status = status;

    public CloseStatementResponse(string status, string code, string message)
    {
        Status = status;
        Code = code;
        Message = message;
    }
}
