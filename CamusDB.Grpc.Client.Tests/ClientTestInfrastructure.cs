
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

using CamusDB.Grpc;
using CamusDB.Grpc.Client.Batching;

namespace CamusDB.Grpc.Client.Tests;

/// <summary>
/// In-memory <see cref="IBatchTransport"/> standing in for a real gRPC <c>BatchExecute</c> stream. It
/// synthesizes the server's response sequence for each request so the batcher's multiplexing, routing,
/// demux, deadline, and reconnect behavior can be tested without a live server. Records the full
/// requests it received so tests can assert routing affinity and causal-token threading.
/// </summary>
internal sealed class FakeBatchTransport : IBatchTransport
{
    private readonly Channel<BatchExecuteResponse> responses = Channel.CreateUnbounded<BatchExecuteResponse>();
    private volatile bool faulted;
    private Exception? faultEx;

    public long Id { get; }

    /// <summary>Every request written to this transport, in arrival order.</summary>
    public ConcurrentQueue<BatchExecuteRequest> Received { get; } = new();

    /// <summary>When set, requests are recorded but produce no response (to test holds / deadlines).</summary>
    public bool Hold { get; set; }

    public FakeBatchTransport(long id) => Id = id;

    public Task SendAsync(BatchExecuteRequest request, CancellationToken cancellationToken)
    {
        Received.Enqueue(request);
        if (!Hold)
            foreach (BatchExecuteResponse resp in Respond(request))
                responses.Writer.TryWrite(resp);
        return Task.CompletedTask;
    }

    public async IAsyncEnumerable<BatchExecuteResponse> ReadAllAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        while (await responses.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            while (responses.Reader.TryRead(out BatchExecuteResponse? r))
                yield return r;

        if (faulted)
            throw faultEx ?? new IOException("fake stream faulted");
    }

    /// <summary>Makes the reader loop throw after draining, simulating a dropped stream.</summary>
    public void FailStream(Exception ex)
    {
        faultEx = ex;
        faulted = true;
        responses.Writer.TryComplete();
    }

    public ValueTask DisposeAsync()
    {
        responses.Writer.TryComplete();
        return ValueTask.CompletedTask;
    }

    // A deterministic stand-in for the server: the causal token L equals the request id so tests can
    // assert token threading; a QUERY returns a one-column schema + two rows; START mints a handle.
    private static IEnumerable<BatchExecuteResponse> Respond(BatchExecuteRequest req)
    {
        int id = req.RequestId;
        switch (req.Kind)
        {
            case BatchStatementKind.Query:
                ResultSchema schema = new();
                schema.Columns.Add(new ColumnSchema { Name = "n", Type = CamusDB.Grpc.ColumnType.Integer64 });
                yield return new BatchExecuteResponse { RequestId = id, Schema = schema };
                for (int i = 0; i < 2; i++)
                {
                    ResultRow row = new();
                    row.Values.Add(new Value { Int64Value = i });
                    yield return new BatchExecuteResponse { RequestId = id, Row = row };
                }
                yield return new BatchExecuteResponse
                {
                    RequestId = id,
                    QueryComplete = new QueryComplete { Total = 2, CausalTokenL = id, CausalTokenC = 1, CausalTokenN = 1 },
                };
                break;

            case BatchStatementKind.NonQuery:
                yield return new BatchExecuteResponse
                {
                    RequestId = id,
                    NonQuery = new NonQueryReply { AffectedRows = 1, CausalTokenL = id, CausalTokenC = 1, CausalTokenN = 1 },
                };
                break;

            case BatchStatementKind.Start:
                yield return new BatchExecuteResponse
                {
                    RequestId = id,
                    StartReply = new TxnHandle { TxnIdPt = 1000 + id, TxnIdCounter = 1 },
                };
                break;

            case BatchStatementKind.Commit:
                yield return new BatchExecuteResponse
                {
                    RequestId = id,
                    CommitReply = new CommitReply { CausalTokenL = id, CausalTokenC = 1, CausalTokenN = 1 },
                };
                break;

            case BatchStatementKind.Rollback:
                yield return new BatchExecuteResponse { RequestId = id, RollbackReply = new RollbackReply() };
                break;
        }
    }
}
