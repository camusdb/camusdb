/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using System.Threading;
using CamusDB.Client;

namespace CamusDB.Workload.Client;

/// <summary>
/// Owns the client-side transport for a run. The gRPC client multiplexes over a small, fixed stream
/// pool per <see cref="CamusConnection"/> (not tunable via the connection string), so to widen client
/// concurrency past that pool the driver opens several connections and spreads workers across them —
/// otherwise the measured throughput knee could reflect a client-side head-of-line limit rather than
/// the server. Reads and writes use separate connection sets: reads run on connections whose default
/// transaction mode is <c>ReadOnly</c> (genuine read-only autocommit — one round trip, no write lock),
/// while writes drive explicit read/write transactions under the run's configured isolation/locking.
/// </summary>
public sealed class ConnectionSet : IAsyncDisposable
{
    private readonly CamusConnection[] _readConnections;
    private readonly CamusConnection[] _writeConnections;
    private int _readCursor = -1;
    private int _writeCursor = -1;

    private ConnectionSet(CamusConnection[] readConnections, CamusConnection[] writeConnections)
    {
        _readConnections = readConnections;
        _writeConnections = writeConnections;
    }

    public int ConnectionCount => _readConnections.Length + _writeConnections.Length;

    /// <summary>
    /// Opens <paramref name="connections"/> read connections and <paramref name="connections"/> write
    /// connections against the same endpoint. Reads default to read-only; writes carry the settings'
    /// isolation/locking plus ReadWrite so autocommit and explicit transactions share the shape.
    /// </summary>
    public static async Task<ConnectionSet> OpenAsync(
        string endpoint, string database, string protocol, int connections,
        ConnectionSettings settings, CancellationToken ct)
    {
        if (connections < 1)
            connections = 1;

        CamusConnection[] reads = new CamusConnection[connections];
        CamusConnection[] writes = new CamusConnection[connections];

        string suffix = settings.CommonSuffix();
        string readCs = $"Endpoint={endpoint};Database={database};Protocol={protocol};TransactionMode=ReadOnly{suffix}";
        string writeCs = $"Endpoint={endpoint};Database={database};Protocol={protocol};" +
                         $"IsolationLevel={settings.IsolationLevel};TransactionMode=ReadWrite;Locking={settings.Locking}{suffix}";

        for (int i = 0; i < connections; i++)
        {
            reads[i] = new CamusConnection(new CamusConnectionStringBuilder(readCs));
            await OpenWithRetryAsync(reads[i], ct).ConfigureAwait(false);
            writes[i] = new CamusConnection(new CamusConnectionStringBuilder(writeCs));
            await OpenWithRetryAsync(writes[i], ct).ConfigureAwait(false);
        }

        return new ConnectionSet(reads, writes);
    }

    /// <summary>
    /// Opens one connection, riding out a transport transient rather than failing the run.
    ///
    /// <para>This is setup, not measurement: it happens before the measured window and its cost is
    /// wall clock only. A run opens dozens of connections in a row immediately after seeding, which
    /// is when the cluster is least likely to answer promptly — a single unlucky open would otherwise
    /// end the run with a bare cancellation, after the seed had already succeeded.</para>
    /// </summary>
    private static async Task OpenWithRetryAsync(CamusConnection connection, CancellationToken ct)
    {
        long startedAt = Stopwatch.GetTimestamp();
        for (int attempt = 1; ; attempt++)
        {
            try
            {
                await connection.OpenAsync(ct).ConfigureAwait(false);
                return;
            }
            catch (Exception ex)
            {
                // Opening a connection changes nothing, so it retries on the same permissive bar as an
                // idempotent read: a transport failure that arrives carrying a server code is still
                // worth another attempt while the budget lasts.
                bool retryable = Operations.ErrorClassifier.IsRetryableForIdempotentRead(ex);
                if (!retryable || Stopwatch.GetElapsedTime(startedAt) >= OpenBudget || ct.IsCancellationRequested)
                    throw;

                await Task.Delay(Math.Min(250 * attempt, 2000), ct).ConfigureAwait(false);
            }
        }
    }

    /// <summary>How long one connection open keeps retrying a transport transient before giving up.</summary>
    private static readonly TimeSpan OpenBudget = TimeSpan.FromMinutes(2);

    /// <summary>Opens a single connection for setup/reconciliation work (schema, seeding, verification).
    /// Only the settings' common suffix applies — setup transactions keep the client defaults.</summary>
    public static async Task<CamusConnection> OpenSingleAsync(
        string endpoint, string database, string protocol, ConnectionSettings settings, CancellationToken ct)
    {
        string cs = $"Endpoint={endpoint};Database={database};Protocol={protocol}{settings.CommonSuffix()}";
        CamusConnection conn = new(new CamusConnectionStringBuilder(cs));
        await OpenWithRetryAsync(conn, ct).ConfigureAwait(false);
        return conn;
    }

    public CamusConnection NextRead()
        => _readConnections[(uint)Interlocked.Increment(ref _readCursor) % _readConnections.Length];

    public CamusConnection NextWrite()
        => _writeConnections[(uint)Interlocked.Increment(ref _writeCursor) % _writeConnections.Length];

    public async ValueTask DisposeAsync()
    {
        foreach (CamusConnection c in _readConnections)
            await c.DisposeAsync().ConfigureAwait(false);
        foreach (CamusConnection c in _writeConnections)
            await c.DisposeAsync().ConfigureAwait(false);
    }
}
