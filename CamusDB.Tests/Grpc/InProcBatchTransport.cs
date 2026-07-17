
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using CamusDB.App.Grpc;
using CamusDB.Grpc;
using CamusDB.Grpc.Client.Batching;

namespace CamusDB.Tests.Grpc;

/// <summary>
/// Bridges the real client <see cref="GrpcBatcher"/> to a real <see cref="CamusSqlService"/>
/// <c>BatchExecute</c> call over in-memory channels — no gRPC socket. This exercises the whole path
/// (client batcher → server handler → embedded Kahuna → back) in one process, which is only possible
/// because the proto types now live in the shared <c>CamusDB.Grpc.Contracts</c> assembly (so the server
/// host and the client library share one set of message types).
/// </summary>
internal sealed class InProcBatchTransport : IBatchTransport
{
    private readonly ChannelAsyncStreamReader<BatchExecuteRequest> serverReader = new();
    private readonly Channel<BatchExecuteResponse> serverResponses = Channel.CreateUnbounded<BatchExecuteResponse>();
    private readonly Task serverTask;

    public long Id { get; }

    public InProcBatchTransport(long id, CamusSqlService service)
    {
        Id = id;
        ChannelServerStreamWriter<BatchExecuteResponse> writer = new(serverResponses);
        TestServerCallContext ctx = new();
        serverTask = Task.Run(async () =>
        {
            try { await service.BatchExecute(serverReader, writer, ctx); }
            finally { serverResponses.Writer.TryComplete(); }
        });
    }

    public Task SendAsync(BatchExecuteRequest request, CancellationToken cancellationToken)
    {
        serverReader.Push(request);
        return Task.CompletedTask;
    }

    public async IAsyncEnumerable<BatchExecuteResponse> ReadAllAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (BatchExecuteResponse resp in serverResponses.Reader.ReadAllAsync(cancellationToken))
            yield return resp;
    }

    public async ValueTask DisposeAsync()
    {
        serverReader.Complete();
        try { await serverTask; } catch { /* teardown */ }
    }
}
