
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.App.Services;
using Grpc.Core;
using Grpc.Core.Interceptors;

namespace CamusDB.App.Grpc;

/// <summary>
/// Counts in-flight gRPC data calls in the shared <see cref="ForegroundRequestGauge"/> so the
/// auto-analyze scheduler's load signal reflects gRPC foreground work, not just HTTP requests and
/// explicit transactions. Mirrors the HTTP <see cref="ForegroundRequestGaugeMiddleware"/> for the
/// gRPC transport.
///
/// <para>Covers unary calls (InsertRow / ExecuteNonQuery / …) and server-streaming calls
/// (ExecuteQuery / Query / QueryById) for their full duration — those are short-lived per statement,
/// so their lifetime is the right "in-flight" window. Duplex streaming (<c>BatchExecute</c>) is
/// deliberately <b>not</b> counted here: it is a long-lived multiplexing stream that stays open
/// across many statements, so counting the stream itself would pin the gauge above zero for as long
/// as any client holds it open. That path counts each batched op individually instead (see
/// <see cref="CamusSqlService"/>), which is the correct granularity.</para>
/// </summary>
public sealed class ForegroundRequestGaugeInterceptor : Interceptor
{
    private readonly ForegroundRequestGauge gauge;

    public ForegroundRequestGaugeInterceptor(ForegroundRequestGauge gauge)
    {
        this.gauge = gauge;
    }

    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        UnaryServerMethod<TRequest, TResponse> continuation)
    {
        gauge.Increment();
        try
        {
            return await continuation(request, context).ConfigureAwait(false);
        }
        finally
        {
            gauge.Decrement();
        }
    }

    public override async Task ServerStreamingServerHandler<TRequest, TResponse>(
        TRequest request,
        IServerStreamWriter<TResponse> responseStream,
        ServerCallContext context,
        ServerStreamingServerMethod<TRequest, TResponse> continuation)
    {
        gauge.Increment();
        try
        {
            await continuation(request, responseStream, context).ConfigureAwait(false);
        }
        finally
        {
            gauge.Decrement();
        }
    }
}
