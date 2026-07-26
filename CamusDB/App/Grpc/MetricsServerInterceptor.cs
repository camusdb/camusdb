
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using CamusDB.Core.Diagnostics;
using Grpc.Core;
using Grpc.Core.Interceptors;

namespace CamusDB.App.Grpc;

/// <summary>
/// Records request-level metrics (count, in-flight, duration, outcome) for unary and server-streaming
/// gRPC calls, tagged <c>grpc_unary</c>. It deliberately does NOT wrap the duplex <c>BatchExecute</c>
/// stream — that stream is long-lived and multiplexes many logical operations, so its lifetime is the
/// wrong measurement window; <see cref="CamusSqlService"/> instruments each batched op individually
/// (tagged <c>grpc_batch</c>) instead. This mirrors the coverage split already used by
/// <see cref="ForegroundRequestGaugeInterceptor"/>. All recording no-ops when diagnostics are disabled.
/// </summary>
public sealed class MetricsServerInterceptor : Interceptor
{
    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        UnaryServerMethod<TRequest, TResponse> continuation)
    {
        string operation = MapMethod(context.Method);
        long start = Stopwatch.GetTimestamp();
        ServerDiagnostics.AddRequestInFlight(ServerDiagnostics.Tags.Transport.GrpcUnary, 1);
        string outcome = ServerDiagnostics.Tags.Outcome.Ok;
        try
        {
            return await continuation(request, context).ConfigureAwait(false);
        }
        catch (RpcException ex)
        {
            outcome = OutcomeFor(ex);
            throw;
        }
        catch (Exception)
        {
            outcome = ServerDiagnostics.Tags.Outcome.InternalError;
            throw;
        }
        finally
        {
            ServerDiagnostics.AddRequestInFlight(ServerDiagnostics.Tags.Transport.GrpcUnary, -1);
            ServerDiagnostics.RecordRequest(operation, ServerDiagnostics.Tags.Transport.GrpcUnary, outcome,
                Stopwatch.GetElapsedTime(start).TotalMilliseconds);
        }
    }

    public override async Task ServerStreamingServerHandler<TRequest, TResponse>(
        TRequest request,
        IServerStreamWriter<TResponse> responseStream,
        ServerCallContext context,
        ServerStreamingServerMethod<TRequest, TResponse> continuation)
    {
        string operation = MapMethod(context.Method);
        long start = Stopwatch.GetTimestamp();
        ServerDiagnostics.AddRequestInFlight(ServerDiagnostics.Tags.Transport.GrpcUnary, 1);
        string outcome = ServerDiagnostics.Tags.Outcome.Ok;
        try
        {
            await continuation(request, responseStream, context).ConfigureAwait(false);
        }
        catch (RpcException ex)
        {
            outcome = OutcomeFor(ex);
            throw;
        }
        catch (Exception)
        {
            outcome = ServerDiagnostics.Tags.Outcome.InternalError;
            throw;
        }
        finally
        {
            ServerDiagnostics.AddRequestInFlight(ServerDiagnostics.Tags.Transport.GrpcUnary, -1);
            ServerDiagnostics.RecordRequest(operation, ServerDiagnostics.Tags.Transport.GrpcUnary, outcome,
                Stopwatch.GetElapsedTime(start).TotalMilliseconds);
        }
    }

    /// <summary>Maps a gRPC method path (e.g. <c>/camus.CamusSql/ExecuteDdl</c>) to a bounded operation tag.</summary>
    private static string MapMethod(string method)
    {
        int slash = method.LastIndexOf('/');
        string name = slash >= 0 ? method[(slash + 1)..] : method;
        return name switch
        {
            "ExecuteQuery" or "Query" or "QueryById" => ServerDiagnostics.Tags.Operation.Query,
            "ExecuteDdl" => ServerDiagnostics.Tags.Operation.Ddl,
            "StartTransaction" => ServerDiagnostics.Tags.Operation.Begin,
            "CommitTransaction" => ServerDiagnostics.Tags.Operation.Commit,
            "RollbackTransaction" => ServerDiagnostics.Tags.Operation.Rollback,
            _ => ServerDiagnostics.Tags.Operation.NonQuery,
        };
    }

    private static string OutcomeFor(RpcException ex) => ex.StatusCode switch
    {
        StatusCode.Cancelled => ServerDiagnostics.Tags.Outcome.Canceled,
        StatusCode.Internal or StatusCode.Unknown => ServerDiagnostics.Tags.Outcome.InternalError,
        _ => ServerDiagnostics.Tags.Outcome.DomainError,
    };
}
