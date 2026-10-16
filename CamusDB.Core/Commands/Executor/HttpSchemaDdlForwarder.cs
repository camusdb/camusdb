
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
using CamusDB.Core.Storage.Kv;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor;

/// <summary>
/// Production <see cref="ISchemaDdlForwarder"/> that posts DDL tickets to the
/// schema leader's <c>/internal/schema-ddl/*</c> HTTP endpoints.
///
/// Also implements <see cref="ISchemaAckSender"/> so that <see cref="EmbeddedKahuna"/>
/// can deliver follower schema-apply acknowledgements to the leader, and ask a peer for its
/// applied version, over the same HTTP channel without a second transport dependency.
///
/// <paramref name="endpointResolver"/> maps a Raft leader endpoint string
/// (e.g. <c>"node1:7070"</c>) to the CamusDB HTTP base URI
/// (e.g. <c>http://node1:5095</c>).  In production, register via
/// <c>Program.cs</c> with a resolver that substitutes the HTTP port; in tests,
/// inject the fake server URI directly.
/// </summary>
public sealed class HttpSchemaDdlForwarder : ISchemaDdlForwarder, ISchemaAckSender
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
    };

    private const string NodeSecretHeader = "X-Camus-Node-Secret";

    private readonly HttpClient httpClient;

    private readonly Func<string, Uri> endpointResolver;

    private readonly string nodeSecret;

    private readonly ILogger<ICamusDB> logger;

    /// <param name="nodeSecret">
    /// The shared node secret, sent on every request. A peer with authentication on refuses any
    /// <c>/internal/*</c> request without it, so leaving it out works in development and fails every
    /// forwarded DDL statement, schema ack and applied-version probe in production. Empty sends no header.
    /// </param>
    public HttpSchemaDdlForwarder(HttpClient httpClient, Func<string, Uri> endpointResolver, string nodeSecret, ILogger<ICamusDB> logger)
    {
        this.httpClient = httpClient;
        this.endpointResolver = endpointResolver;
        this.nodeSecret = nodeSecret;
        this.logger = logger;
    }

    private async Task<HttpResponseMessage> PostJsonAsync<TRequest>(Uri endpoint, TRequest body, CancellationToken cancellationToken)
    {
        // Not disposed here, as PostAsJsonAsync did not dispose it: disposing it would dispose the body
        // too, and the request holds nothing unmanaged once it is sent.
        HttpRequestMessage request = new(HttpMethod.Post, endpoint)
        {
            Content = JsonContent.Create(body, options: JsonOptions),
        };

        if (!string.IsNullOrEmpty(nodeSecret))
            request.Headers.TryAddWithoutValidation(NodeSecretHeader, nodeSecret);

        return await httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool?> ForwardCreateTableAsync(string leader, CreateTableTicket ticket, string operationId, CancellationToken cancellationToken)
    {
        ForwardCreateTableRequest request = new()
        {
            OperationId = operationId,
            DatabaseName = ticket.DatabaseName,
            TableName = ticket.TableName,
            Columns = MapColumns(ticket.Columns),
            Constraints = MapConstraints(ticket.Constraints),
            CheckConstraints = MapCheckConstraints(ticket.CheckConstraints),
            ForeignKeys = MapForeignKeys(ticket.ForeignKeys),
            IfNotExists = ticket.IfNotExists,
            Comment = ticket.Comment,
        };

        return await PostAsync(leader, "create-table", request, cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool?> ForwardAlterTableAsync(string leader, AlterTableTicket ticket, string operationId, CancellationToken cancellationToken)
    {
        ForwardAlterTableRequest request = new()
        {
            OperationId = operationId,
            DatabaseName = ticket.DatabaseName,
            TableName = ticket.TableName,
            Operation = ticket.Operation,
            Column = MapColumn(ticket.Column),
            NewName = ticket.NewName,
        };

        return await PostAsync(leader, "alter-table", request, cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool?> ForwardAlterIndexAsync(string leader, AlterIndexTicket ticket, string operationId, CancellationToken cancellationToken)
    {
        ForwardAlterIndexRequest request = new()
        {
            OperationId = operationId,
            DatabaseName = ticket.DatabaseName,
            TableName = ticket.TableName,
            IndexName = ticket.IndexName,
            Columns = ticket.Columns.Select(c => new ColumnIndexInfoRequest { Name = c.Name, Order = c.Order }).ToArray(),
            Operation = ticket.Operation,
            IfNotExists = ticket.IfNotExists,
            NewName = ticket.NewName,
        };

        return await PostAsync(leader, "alter-index", request, cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool?> ForwardDropTableAsync(string leader, DropTableTicket ticket, string operationId, CancellationToken cancellationToken)
    {
        ForwardDropTableRequest request = new()
        {
            OperationId = operationId,
            DatabaseName = ticket.DatabaseName,
            TableName = ticket.TableName,
            IfExists = ticket.IfExists,
            Force = ticket.Force,
            AllowMaterializedView = ticket.AllowMaterializedView,
        };

        return await PostAsync(leader, "drop-table", request, cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool?> ForwardTruncateTableAsync(string leader, TruncateTableTicket ticket, string operationId, CancellationToken cancellationToken)
    {
        ForwardTruncateTableRequest request = new()
        {
            OperationId = operationId,
            DatabaseName = ticket.DatabaseName,
            TableName = ticket.TableName,
            RestartIdentity = ticket.RestartIdentity,
        };

        return await PostAsync(leader, "truncate-table", request, cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool?> ForwardRelinkTableAsync(string leader, RelinkTableTicket ticket, string operationId, CancellationToken cancellationToken)
    {
        ForwardRelinkTableRequest request = new()
        {
            OperationId = operationId,
            DatabaseName = ticket.DatabaseName,
            NewTableName = ticket.NewTableName,
            OrphanTableId = ticket.OrphanTableId,
        };

        return await PostAsync(leader, "relink-table", request, cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool?> ForwardAlterConstraintAsync(string leader, AlterConstraintTicket ticket, string operationId, CancellationToken cancellationToken)
    {
        ForwardAlterConstraintRequest request = new()
        {
            OperationId = operationId,
            DatabaseName = ticket.DatabaseName,
            TableName = ticket.TableName,
            ConstraintName = ticket.ConstraintName,
            Expression = ticket.Expression,
            ReferencedColumns = ticket.ReferencedColumns,
            Operation = ticket.Operation,
            ColumnName = ticket.ColumnName,
            Storage = ticket.Storage,
            ForeignKey = ticket.ForeignKey is { } foreignKey ? MapForeignKeys([foreignKey])[0] : null,
        };

        return await PostAsync(leader, "alter-constraint", request, cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool?> ForwardCommentAsync(string leader, CommentTicket ticket, string operationId, CancellationToken cancellationToken)
    {
        ForwardCommentRequest request = new()
        {
            OperationId = operationId,
            DatabaseName = ticket.DatabaseName,
            Target = ticket.Target,
            TableName = ticket.TableName ?? "",
            ElementName = ticket.ElementName,
            // Passed through as-is: null means "remove the comment" and must not collapse to "".
            Comment = ticket.Comment,
        };

        return await PostAsync(leader, "comment", request, cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool?> ForwardRenameTableAsync(string leader, RenameTableTicket ticket, string operationId, CancellationToken cancellationToken)
    {
        ForwardRenameTableRequest request = new()
        {
            OperationId = operationId,
            DatabaseName = ticket.DatabaseName,
            TableName = ticket.TableName,
            NewName = ticket.NewName,
        };

        return await PostAsync(leader, "rename-table", request, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Posts a schema-apply ack from the local follower to the current leader's ack endpoint.
    /// Transport failures are logged at Debug and reported as <see cref="SchemaAckDelivery.Failed"/>;
    /// a 503 or a <c>not-leader</c> body is reported as <see cref="SchemaAckDelivery.NotLeader"/>. The
    /// caller retries on both, so a dropped or misrouted ack does not strand the leader's gate.
    /// </summary>
    async Task<SchemaAckDelivery> ISchemaAckSender.SendSchemaAckAsync(
        string leaderEndpoint,
        string database,
        string nodeEndpoint,
        long schemaVersion,
        CancellationToken cancellationToken)
    {
        SchemaAckRequest request = new()
        {
            Database = database,
            NodeEndpoint = nodeEndpoint,
            AppliedSchemaVersion = schemaVersion,
        };

        Uri baseUri = endpointResolver(leaderEndpoint);
        Uri endpoint = new(baseUri, "/internal/schema-ddl/schema-ack");

        HttpResponseMessage? response = null;
        try
        {
            response = await PostJsonAsync(endpoint, request, cancellationToken).ConfigureAwait(false);

            // The ack endpoint answers 503 not-leader like the DDL endpoints do, so the sender can
            // walk the replica set; any other failure is retried after the leader is resolved again.
            if (response.StatusCode == HttpStatusCode.ServiceUnavailable)
                return SchemaAckDelivery.NotLeader;

            SchemaDdlForwardResponse? body = response.IsSuccessStatusCode
                ? await response.Content.ReadFromJsonAsync<SchemaDdlForwardResponse>(JsonOptions, cancellationToken).ConfigureAwait(false)
                : null;

            switch (body?.Status)
            {
                case "ok":
                    return SchemaAckDelivery.Accepted;
                case "not-leader":
                    return SchemaAckDelivery.NotLeader;
                default:
                    Log.LogSchemaAckRejected(logger, endpoint, (int)response.StatusCode, body?.Status);
                    return SchemaAckDelivery.Failed;
            }
        }
        catch (HttpRequestException ex)
        {
            Log.LogSchemaAckFailed(logger, ex, endpoint);
            return SchemaAckDelivery.Failed;
        }
        catch (TaskCanceledException ex) when (!cancellationToken.IsCancellationRequested)
        {
            Log.LogSchemaAckTimedOut(logger, ex, endpoint);
            return SchemaAckDelivery.Failed;
        }
        catch (System.Text.Json.JsonException ex)
        {
            Log.LogSchemaAckFailed(logger, ex, endpoint);
            return SchemaAckDelivery.Failed;
        }
        finally
        {
            response?.Dispose();
        }
    }

    /// <summary>
    /// Asks a peer's <c>/internal/schema-ddl/schema-applied</c> endpoint for its applied version of a
    /// database. Every transport failure, timeout and malformed answer is reported as
    /// <see cref="SchemaAppliedProbeOutcome.Unreachable"/> and logged at Debug; the gate then keeps
    /// what it already knows about the peer, so a probe can never make the gate wait for less than it
    /// did before probing existed.
    /// </summary>
    async Task<SchemaAppliedProbe> ISchemaAckSender.ProbeSchemaAppliedAsync(
        string nodeEndpoint,
        string database,
        CancellationToken cancellationToken)
    {
        SchemaAppliedProbeRequest request = new() { Database = database };

        Uri baseUri = endpointResolver(nodeEndpoint);
        Uri endpoint = new(baseUri, "/internal/schema-ddl/schema-applied");

        HttpResponseMessage? response = null;
        try
        {
            response = await PostJsonAsync(endpoint, request, cancellationToken).ConfigureAwait(false);

            SchemaAppliedProbeResponse? body = response.IsSuccessStatusCode
                ? await response.Content.ReadFromJsonAsync<SchemaAppliedProbeResponse>(JsonOptions, cancellationToken).ConfigureAwait(false)
                : null;

            if (body is null || !string.Equals(body.Status, "ok", StringComparison.Ordinal))
            {
                Log.LogSchemaAppliedProbeRejected(logger, endpoint, (int)response.StatusCode, body?.Status);
                return SchemaAppliedProbe.Unreachable;
            }

            return body.Open ? SchemaAppliedProbe.OpenAt(body.AppliedSchemaVersion) : SchemaAppliedProbe.NotOpen;
        }
        catch (HttpRequestException ex)
        {
            Log.LogSchemaAppliedProbeFailed(logger, ex, endpoint);
            return SchemaAppliedProbe.Unreachable;
        }
        catch (TaskCanceledException ex) when (!cancellationToken.IsCancellationRequested)
        {
            Log.LogSchemaAppliedProbeFailed(logger, ex, endpoint);
            return SchemaAppliedProbe.Unreachable;
        }
        catch (OperationCanceledException)
        {
            // The gate ended and cancelled its probes; nothing to report.
            return SchemaAppliedProbe.Unreachable;
        }
        catch (System.Text.Json.JsonException ex)
        {
            Log.LogSchemaAppliedProbeFailed(logger, ex, endpoint);
            return SchemaAppliedProbe.Unreachable;
        }
        finally
        {
            response?.Dispose();
        }
    }

    private async Task<bool?> PostAsync<TRequest>(string leader, string operation, TRequest request, CancellationToken cancellationToken)
    {
        Uri baseUri = endpointResolver(leader);
        Uri endpoint = new(baseUri, $"/internal/schema-ddl/{operation}");

        Log.LogForwardingDdl(logger, operation, endpoint);

        // Transport failures at any point — connection refused, DNS failure,
        // mid-election TCP reset, HTTP-client timeout, or a connection drop
        // while reading the response body — are safe to treat as "not-leader":
        // returning null lets TryForwardDdlAsync elect a fresh leader and retry.
        //
        // When the leader already applied the DDL before the connection dropped,
        // the retry carries the same stable operationId.  The leader's
        // DdlOperationIdCache catches the duplicate and replays the cached
        // response, making the operation idempotent end-to-end.
        HttpResponseMessage? response = null;
        try
        {
            response = await PostJsonAsync(endpoint, request, cancellationToken).ConfigureAwait(false);

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
        catch (HttpRequestException ex)
        {
            logger.LogWarning(ex, "DDL forward to {Endpoint} failed with transport error; will retry with new leader", endpoint);
            return null;
        }
        catch (TaskCanceledException ex) when (!cancellationToken.IsCancellationRequested)
        {
            // HTTP-client timeout (not caller-requested cancellation).
            logger.LogWarning(ex, "DDL forward to {Endpoint} timed out; will retry with new leader", endpoint);
            return null;
        }
        finally
        {
            response?.Dispose();
        }
    }

    private static ColumnInfoRequest MapColumn(ColumnInfo col) => new()
    {
        Name = col.Name,
        Type = col.Type,
        NotNull = col.NotNull,
        Default = col.Default,
        MaxLength = col.MaxLength,
        ArrayElementType = col.ArrayElementType,
        DefaultFunction = col.DefaultFunction,
        DefaultSequenceId = col.DefaultSequenceId,
        IdentityAlways = col.IdentityAlways,
        Identity = col.Identity,
        DefaultSequenceName = col.DefaultSequenceName,
        NotNullConstraintName = col.NotNullConstraintName,
        Comment = col.Comment,
        Storage = col.Storage,
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
                Comment = c.Comment,
            };
        }
        return result;
    }

    private static ForeignKeyInfoRequest[] MapForeignKeys(ForeignKeyInfo[] foreignKeys)
    {
        ForeignKeyInfoRequest[] result = new ForeignKeyInfoRequest[foreignKeys.Length];
        for (int i = 0; i < foreignKeys.Length; i++)
        {
            ForeignKeyInfo f = foreignKeys[i];
            result[i] = new()
            {
                Name = f.Name,
                Columns = f.Columns,
                ReferencedTable = f.ReferencedTable,
                ReferencedColumns = f.ReferencedColumns,
                OnDelete = f.OnDelete,
                OnUpdate = f.OnUpdate,
                Match = f.Match,
                Deferrable = f.Deferrable,
                InitiallyDeferred = f.InitiallyDeferred,
            };
        }
        return result;
    }

    private static CheckConstraintInfoRequest[] MapCheckConstraints(CheckConstraintInfo[] checks)
    {
        CheckConstraintInfoRequest[] result = new CheckConstraintInfoRequest[checks.Length];
        for (int i = 0; i < checks.Length; i++)
        {
            CheckConstraintInfo c = checks[i];
            result[i] = new()
            {
                Name = c.Name,
                Expression = c.Expression,
                ReferencedColumns = c.ReferencedColumns,
            };
        }
        return result;
    }
}
