
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Grpc.Core;

namespace CamusDB.Grpc.Client.Batching;

/// <summary>
/// Real <see cref="IBatchTransport"/> over a gRPC <c>BatchExecute</c> duplex call. One instance owns one
/// long-lived stream; the batcher keeps several (the pool) and multiplexes ops across them.
/// </summary>
internal sealed class GrpcBatchTransport : IBatchTransport
{
    private readonly AsyncDuplexStreamingCall<BatchExecuteRequest, BatchExecuteResponse> call;

    public long Id { get; }

    public GrpcBatchTransport(long id, CamusSql.CamusSqlClient client)
    {
        Id = id;
        call = client.BatchExecute();
    }

    public Task SendAsync(BatchExecuteRequest request, CancellationToken cancellationToken)
        => call.RequestStream.WriteAsync(request, cancellationToken);

    public IAsyncEnumerable<BatchExecuteResponse> ReadAllAsync(CancellationToken cancellationToken)
        => call.ResponseStream.ReadAllAsync(cancellationToken);

    public async ValueTask DisposeAsync()
    {
        try { await call.RequestStream.CompleteAsync().ConfigureAwait(false); }
        catch { /* stream already broken */ }
        call.Dispose();
    }
}
