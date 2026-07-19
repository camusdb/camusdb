
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using NUnit.Framework;
using Grpc.Core;
using Microsoft.Extensions.Hosting;
using CamusDB.Grpc;

namespace CamusDB.Tests.Grpc;

/// <summary>
/// No-op <see cref="IHostApplicationLifetime"/> for unit-level gRPC tests that construct the service
/// directly without a host. Its shutdown tokens never fire, so linking against them is inert — the
/// tests exercise normal streaming; the real server links these to end long-lived streams on shutdown.
/// </summary>
internal sealed class TestHostApplicationLifetime : IHostApplicationLifetime
{
    public static readonly TestHostApplicationLifetime Instance = new();
    public CancellationToken ApplicationStarted => CancellationToken.None;
    public CancellationToken ApplicationStopping => CancellationToken.None;
    public CancellationToken ApplicationStopped => CancellationToken.None;
    public void StopApplication() { }
}

/// <summary>
/// Minimal <see cref="ServerCallContext"/> stub for unit-level tests that invoke
/// <c>CamusSqlService</c> methods directly without a real gRPC transport.
/// Cancellation is driven by the supplied <see cref="CancellationToken"/>; response
/// trailers are captured in a plain <see cref="Metadata"/> object for assertion.
/// </summary>
internal sealed class TestServerCallContext : ServerCallContext
{
    private readonly CancellationToken ct;

    public TestServerCallContext(CancellationToken ct = default)
    {
        this.ct             = ct;
        RequestHeadersCore  = new Metadata();
        ResponseTrailersCore = new Metadata();
    }

    protected override string          MethodCore           => "/camus.CamusSql/Test";
    protected override string          HostCore             => "localhost";
    protected override string          PeerCore             => "localhost";
    protected override DateTime        DeadlineCore         => DateTime.MaxValue;
    protected override Metadata        RequestHeadersCore   { get; }
    protected override CancellationToken CancellationTokenCore => ct;
    protected override Metadata        ResponseTrailersCore { get; }
    protected override Status          StatusCore           { get; set; }
    protected override WriteOptions?   WriteOptionsCore     { get; set; }
    protected override AuthContext     AuthContextCore      { get; } =
        new AuthContext(null!, new Dictionary<string, List<AuthProperty>>());
    protected override IDictionary<object, object> UserStateCore { get; } =
        new Dictionary<object, object>();

    protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders)
        => Task.CompletedTask;

    protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options)
        => throw new NotImplementedException("Propagation tokens not needed in tests");
}

/// <summary>
/// <see cref="IServerStreamWriter{T}"/> stub that accumulates written messages in
/// <see cref="Written"/> so tests can assert on the exact stream sequence without
/// spinning up a real gRPC channel.
/// </summary>
internal sealed class CapturingStreamWriter<T> : IServerStreamWriter<T>
{
    public List<T> Written { get; } = new();

    public WriteOptions? WriteOptions { get; set; }

    public Task WriteAsync(T message)
    {
        Written.Add(message);
        return Task.CompletedTask;
    }

    public Task WriteAsync(T message, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Written.Add(message);
        return Task.CompletedTask;
    }
}

/// <summary>
/// <see cref="IAsyncStreamReader{T}"/> stub that replays a fixed sequence of messages, so a
/// bidirectional-streaming handler (<c>BatchExecute</c>) can be driven without a real gRPC channel.
/// </summary>
internal sealed class FakeAsyncStreamReader<T> : IAsyncStreamReader<T>
{
    private readonly IEnumerator<T> items;

    public FakeAsyncStreamReader(IEnumerable<T> items) => this.items = items.GetEnumerator();

    public T Current { get; private set; } = default!;

    public Task<bool> MoveNext(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (items.MoveNext())
        {
            Current = items.Current;
            return Task.FromResult(true);
        }
        return Task.FromResult(false);
    }
}

/// <summary>
/// Interactive <see cref="IAsyncStreamReader{T}"/> backed by an unbounded channel, so a test can push
/// requests to a running <c>BatchExecute</c> handler <b>after</b> observing earlier responses — needed
/// for the START → statements → COMMIT flow, where the transaction's statements can only be sent once
/// the START reply's handle is known. Call <see cref="Push"/> to enqueue a request and
/// <see cref="Complete"/> to half-close the request stream (which triggers server teardown).
/// </summary>
internal sealed class ChannelAsyncStreamReader<T> : IAsyncStreamReader<T>
{
    private readonly Channel<T> channel = Channel.CreateUnbounded<T>();

    public T Current { get; private set; } = default!;

    public void Push(T item) => channel.Writer.TryWrite(item);

    public void Complete() => channel.Writer.TryComplete();

    public async Task<bool> MoveNext(CancellationToken cancellationToken)
    {
        if (await channel.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false)
            && channel.Reader.TryRead(out T? item))
        {
            Current = item!;
            return true;
        }
        return false;
    }
}

/// <summary>
/// <see cref="IServerStreamWriter{T}"/> stub that accumulates messages and lets a test asynchronously
/// wait for the first message matching a predicate (e.g. "the COMMIT reply for request 4"). Thread-safe:
/// the <c>BatchExecute</c> handler writes from concurrent op tasks.
/// </summary>
internal sealed class ObservingStreamWriter<T> : IServerStreamWriter<T>
{
    private readonly object gate = new();
    private readonly List<T> written = new();
    private readonly List<(Func<T, bool> predicate, TaskCompletionSource<T> tcs)> waiters = new();

    public WriteOptions? WriteOptions { get; set; }

    public IReadOnlyList<T> Written
    {
        get { lock (gate) return written.ToArray(); }
    }

    public Task WriteAsync(T message) => WriteAsync(message, CancellationToken.None);

    public Task WriteAsync(T message, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (gate)
        {
            written.Add(message);
            for (int i = waiters.Count - 1; i >= 0; i--)
            {
                if (waiters[i].predicate(message))
                {
                    waiters[i].tcs.TrySetResult(message);
                    waiters.RemoveAt(i);
                }
            }
        }
        return Task.CompletedTask;
    }

    /// <summary>Completes when a written message first matches <paramref name="predicate"/>.</summary>
    public Task<T> WaitFor(Func<T, bool> predicate)
    {
        lock (gate)
        {
            foreach (T m in written)
                if (predicate(m))
                    return Task.FromResult(m);

            TaskCompletionSource<T> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
            waiters.Add((predicate, tcs));
            return tcs.Task;
        }
    }
}

/// <summary>
/// Server-side <see cref="IServerStreamWriter{T}"/> that forwards writes into an unbounded channel, so
/// an in-process transport can bridge a real <c>BatchExecute</c> server call to a client reader loop.
/// </summary>
internal sealed class ChannelServerStreamWriter<T> : IServerStreamWriter<T>
{
    private readonly Channel<T> channel;

    public ChannelServerStreamWriter(Channel<T> channel) => this.channel = channel;

    public WriteOptions? WriteOptions { get; set; }

    public Task WriteAsync(T message) => WriteAsync(message, CancellationToken.None);

    public Task WriteAsync(T message, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        channel.Writer.TryWrite(message);
        return Task.CompletedTask;
    }
}

/// <summary>
/// Helpers shared across the gRPC service test fixtures.
/// </summary>
internal static class GrpcAssert
{
    /// <summary>
    /// Asserts that <paramref name="messages"/> starts with exactly one <c>ResultSchema</c>
    /// message followed by exactly <paramref name="expectedRowCount"/> <c>ResultRow</c>
    /// messages and nothing else. The <c>ResultSchema</c> must list the expected column names.
    /// </summary>
    public static ResultSchema AssertSchemaFirst(
        List<QueryStreamMessage> messages,
        int expectedRowCount,
        params string[] expectedColumnNames)
    {
        Assert.That(messages.Count, Is.EqualTo(1 + expectedRowCount),
            $"Expected schema + {expectedRowCount} rows, got {messages.Count} messages");

        QueryStreamMessage first = messages[0];
        Assert.That(first.PayloadCase, Is.EqualTo(QueryStreamMessage.PayloadOneofCase.Schema),
            "First message must be a ResultSchema");

        if (expectedColumnNames.Length > 0)
        {
            IEnumerable<string> actualNames = first.Schema.Columns.Select(c => c.Name);
            Assert.That(actualNames, Is.EquivalentTo(expectedColumnNames),
                "Schema column names mismatch");
        }

        for (int i = 1; i < messages.Count; i++)
            Assert.That(messages[i].PayloadCase,
                Is.EqualTo(QueryStreamMessage.PayloadOneofCase.Row),
                $"Message[{i}] must be a ResultRow");

        return first.Schema;
    }

    /// <summary>
    /// Extracts the positional values from the <paramref name="i"/>-th row message (0-based, not
    /// counting the schema), aligning them to the column names from <paramref name="schema"/>.
    /// </summary>
    public static Dictionary<string, Value> RowValues(
        List<QueryStreamMessage> messages, ResultSchema schema, int i = 0)
    {
        // messages[0] is the schema; rows start at messages[1]
        ResultRow row = messages[i + 1].Row;
        Dictionary<string, Value> result = new(schema.Columns.Count);
        for (int col = 0; col < schema.Columns.Count; col++)
            result[schema.Columns[col].Name] = row.Values[col];
        return result;
    }
}
