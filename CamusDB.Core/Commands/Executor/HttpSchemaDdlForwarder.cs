
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor;

/// <summary>
/// Production <see cref="ISchemaDdlForwarder"/> that posts DDL tickets to the
/// schema leader's <c>/internal/schema-ddl/*</c> HTTP endpoints.
///
/// <paramref name="endpointResolver"/> maps a Raft leader endpoint string
/// (e.g. <c>"node1:7070"</c>) to the CamusDB HTTP base URI
/// (e.g. <c>http://node1:5095</c>).  In production, register via
/// <c>Program.cs</c> with a resolver that substitutes the HTTP port; in tests,
/// inject the fake server URI directly.
/// </summary>
public sealed class HttpSchemaDdlForwarder : ISchemaDdlForwarder
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
    };

    private readonly HttpClient httpClient;
    private readonly Func<string, Uri> endpointResolver;
    private readonly ILogger<ICamusDB> logger;

    public HttpSchemaDdlForwarder(HttpClient httpClient, Func<string, Uri> endpointResolver, ILogger<ICamusDB> logger)
    {
        this.httpClient = httpClient;
        this.endpointResolver = endpointResolver;
        this.logger = logger;
    }

    public async Task<bool?> ForwardCreateTableAsync(string leader, CreateTableTicket ticket, CancellationToken cancellationToken)
    {
        ForwardCreateTableRequest request = new()
        {
            OperationId = Guid.NewGuid().ToString("N"),
            DatabaseName = ticket.DatabaseName,
            TableName = ticket.TableName,
            Columns = MapColumns(ticket.Columns),
            Constraints = MapConstraints(ticket.Constraints),
            IfNotExists = ticket.IfNotExists,
        };

        return await PostAsync(leader, "create-table", request, cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool?> ForwardAlterTableAsync(string leader, AlterTableTicket ticket, CancellationToken cancellationToken)
    {
        ForwardAlterTableRequest request = new()
        {
            OperationId = Guid.NewGuid().ToString("N"),
            DatabaseName = ticket.DatabaseName,
            TableName = ticket.TableName,
            Operation = ticket.Operation,
            Column = MapColumn(ticket.Column),
        };

        return await PostAsync(leader, "alter-table", request, cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool?> ForwardAlterIndexAsync(string leader, AlterIndexTicket ticket, CancellationToken cancellationToken)
    {
        ForwardAlterIndexRequest request = new()
        {
            OperationId = Guid.NewGuid().ToString("N"),
            DatabaseName = ticket.DatabaseName,
            TableName = ticket.TableName,
            IndexName = ticket.IndexName,
            Columns = ticket.Columns.Select(c => new ColumnIndexInfoRequest { Name = c.Name, Order = c.Order }).ToArray(),
            Operation = ticket.Operation,
            IfNotExists = ticket.IfNotExists,
        };

        return await PostAsync(leader, "alter-index", request, cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool?> ForwardDropTableAsync(string leader, DropTableTicket ticket, CancellationToken cancellationToken)
    {
        ForwardDropTableRequest request = new()
        {
            OperationId = Guid.NewGuid().ToString("N"),
            DatabaseName = ticket.DatabaseName,
            TableName = ticket.TableName,
            IfExists = ticket.IfExists,
        };

        return await PostAsync(leader, "drop-table", request, cancellationToken).ConfigureAwait(false);
    }

    private async Task<bool?> PostAsync<TRequest>(string leader, string operation, TRequest request, CancellationToken cancellationToken)
    {
        Uri baseUri = endpointResolver(leader);
        Uri endpoint = new(baseUri, $"/internal/schema-ddl/{operation}");

        logger.LogDebug("Forwarding DDL {Operation} to {Endpoint}", operation, endpoint);

        using HttpResponseMessage response = await httpClient.PostAsJsonAsync(endpoint, request, JsonOptions, cancellationToken).ConfigureAwait(false);

        if (response.StatusCode == HttpStatusCode.ServiceUnavailable)
            return null;

        SchemaDdlForwardResponse? body = await response.Content.ReadFromJsonAsync<SchemaDdlForwardResponse>(JsonOptions, cancellationToken).ConfigureAwait(false);

        if (body is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, $"Empty response from leader at {endpoint}");

        return body.Status switch
        {
            "ok" => body.Applied,
            "not-leader" => null,
            "failed" => throw new CamusDBException(body.Code ?? CamusDBErrorCodes.InvalidInternalOperation, body.Message ?? "Unknown error from schema leader"),
            _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, $"Unexpected status '{body.Status}' from {endpoint}"),
        };
    }

    private static ColumnInfoRequest MapColumn(ColumnInfo col) => new()
    {
        Name = col.Name,
        Type = col.Type,
        NotNull = col.NotNull,
        Default = col.Default,
    };

    private static ColumnInfoRequest[] MapColumns(ColumnInfo[] cols)
    {
        ColumnInfoRequest[] result = new ColumnInfoRequest[cols.Length];
        for (int i = 0; i < cols.Length; i++)
            result[i] = MapColumn(cols[i]);
        return result;
    }

    private static ConstraintInfoRequest[] MapConstraints(ConstraintInfo[] constraints)
    {
        ConstraintInfoRequest[] result = new ConstraintInfoRequest[constraints.Length];
        for (int i = 0; i < constraints.Length; i++)
        {
            ConstraintInfo c = constraints[i];
            result[i] = new()
            {
                Type = c.Type,
                Name = c.Name,
                Columns = c.Columns.Select(col => new ColumnIndexInfoRequest { Name = col.Name, Order = col.Order }).ToArray(),
            };
        }
        return result;
    }
}
