/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.InteropServices.JavaScript;
using System.Runtime.Versioning;

namespace CamusDB.Wasm;

/// <summary>
/// The JavaScript surface of the browser playground. It holds one CamusDB engine per page and runs
/// a SQL script against it, and it can hold a small cluster of engines instead, so a visitor can
/// watch replication, a failover and a rejoin inside the tab.
///
/// <para>The engine is the real one — the same parser, planner, executor and embedded Kahuna node
/// the server runs — on the browser build of Kahuna, which runs its Raft scheduling as async
/// continuations on the page's event loop instead of on worker threads. Storage and write-ahead
/// log are in memory, so a reload starts empty.</para>
///
/// <para>Single-node mode is the default and is what <see cref="InitAsync"/> starts. Cluster mode
/// starts only when the visitor asks for it (<see cref="StartClusterAsync"/>), because it costs
/// several engines' memory and a longer start. The two modes are independent: the single node
/// keeps its data while a cluster runs, and gets it back when the cluster stops.</para>
///
/// <para>Calls are serialized through one gate. The runtime has one thread, so they could not run
/// in parallel anyway, but without the gate a second call issued before the first one resolves
/// would interleave its statements with the first script's at every await. The gate also keeps a
/// node's start or stop from overlapping a statement that runs on it.</para>
///
/// <para>Each statement is autocommitted the way the REST transport does it; see
/// <see cref="StatementRunner"/>. A script stops at the first failed statement.</para>
/// </summary>
[SupportedOSPlatform("browser")]
public static partial class PlaygroundEngine
{
    /// <summary>The database every statement runs in. It is created at start.</summary>
    public const string DatabaseName = SingleNodeSession.DatabaseName;

    private static readonly SemaphoreSlim gate = new(1, 1);

    private static SingleNodeSession? session;

    private static ClusterSession? cluster;

    /// <summary>
    /// Starts the single-node engine and creates the <see cref="DatabaseName"/> database.
    /// Idempotent: a second call returns once the first engine is up.
    /// </summary>
    [JSExport]
    public static async Task InitAsync()
    {
        await gate.WaitAsync().ConfigureAwait(false);
        try
        {
            session ??= await SingleNodeSession.StartAsync().ConfigureAwait(false);
        }
        finally
        {
            gate.Release();
        }
    }

    /// <summary>
    /// Discards every table and row of the single-node engine by disposing it and starting a new
    /// one. A running cluster is not touched.
    /// </summary>
    [JSExport]
    public static async Task ResetAsync()
    {
        await gate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (session is not null)
                await session.DisposeAsync().ConfigureAwait(false);

            session = null;
            session = await SingleNodeSession.StartAsync().ConfigureAwait(false);
        }
        finally
        {
            gate.Release();
        }
    }

    /// <summary>
    /// Runs every statement of <paramref name="script"/> in order on the single-node engine and
    /// returns a JSON array with one object per statement that ran. A statement that fails ends
    /// the script; its object carries the error, and the statements after it are not run.
    ///
    /// <para>Object shapes: <c>{"sql","ok":true,"columns":[{"name","type"}],"rows":[[…]],"ms"}</c>
    /// for a row-returning statement, <c>{"sql","ok":true,"affected","warning"?,"ms"}</c> for the
    /// rest, and <c>{"sql","ok":false,"code","message","ms"}</c> for a failure. A row is a positional
    /// array aligned to <c>columns</c>, encoded the way the REST API encodes it.</para>
    /// </summary>
    [JSExport]
    public static async Task<string> ExecuteAsync(string script)
        => await ExecuteInDatabaseAsync(script, DatabaseName).ConfigureAwait(false);

    /// <summary>
    /// Runs every statement of <paramref name="script"/> in order against
    /// <paramref name="databaseName"/>. Server-level statements such as <c>CREATE DATABASE</c>,
    /// <c>CREATE DATABASE ... BRANCH FROM</c>, and <c>SHOW DATABASES</c> still resolve their own
    /// target and do not open the context database.
    /// </summary>
    [JSExport]
    public static async Task<string> ExecuteInDatabaseAsync(string script, string? databaseName)
    {
        await gate.WaitAsync().ConfigureAwait(false);
        try
        {
            session ??= await SingleNodeSession.StartAsync().ConfigureAwait(false);

            return await session.ExecuteAsync(script, NormalizeDatabaseName(databaseName)).ConfigureAwait(false);
        }
        finally
        {
            gate.Release();
        }
    }

    // ── Cluster mode ─────────────────────────────────────────────────────────

    /// <summary>
    /// Starts a cluster of <paramref name="nodeCount"/> engines in this tab and creates the
    /// <see cref="DatabaseName"/> database on it. A cluster that already runs is stopped first, so
    /// the call always leaves a cluster of exactly the size that was asked for.
    ///
    /// <para>It takes several seconds: each node elects leaders with the production-like Raft
    /// timings, and they all share the page's one event loop.</para>
    /// </summary>
    [JSExport]
    public static async Task<string> StartClusterAsync(int nodeCount)
    {
        await gate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (cluster is not null)
            {
                await cluster.DisposeAsync().ConfigureAwait(false);
                cluster = null;
            }

            cluster = await ClusterSession.StartAsync(nodeCount).ConfigureAwait(false);

            return await cluster.StatusJsonAsync().ConfigureAwait(false);
        }
        finally
        {
            gate.Release();
        }
    }

    /// <summary>Stops every node of the cluster and frees it. Doing it twice is not an error.</summary>
    [JSExport]
    public static async Task StopClusterAsync()
    {
        await gate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (cluster is not null)
                await cluster.DisposeAsync().ConfigureAwait(false);

            cluster = null;
        }
        finally
        {
            gate.Release();
        }
    }

    /// <summary>
    /// Runs a script on one cluster node, and returns the same JSON array
    /// <see cref="ExecuteAsync"/> returns. Sending DDL to a follower is the interesting case: the
    /// follower forwards it to the schema leader, and every node then applies the committed
    /// change.
    /// </summary>
    [JSExport]
    public static async Task<string> ExecuteOnNodeAsync(int nodeIndex, string script, string? databaseName)
    {
        await gate.WaitAsync().ConfigureAwait(false);
        try
        {
            return await RequireCluster().ExecuteAsync(nodeIndex, script, NormalizeDatabaseName(databaseName)).ConfigureAwait(false);
        }
        finally
        {
            gate.Release();
        }
    }

    /// <summary>
    /// The state of the cluster as JSON:
    /// <c>{"partitionCount", "nodes":[{"index","name","endpoint","running","leads":[…],"blockedTo":[…]}]}</c>.
    /// <c>leads</c> holds the partitions the node believes it leads, and <c>blockedTo</c> the
    /// nodes it cannot send to.
    /// </summary>
    [JSExport]
    public static async Task<string> ClusterStatusAsync()
    {
        await gate.WaitAsync().ConfigureAwait(false);
        try
        {
            return await RequireCluster().StatusJsonAsync().ConfigureAwait(false);
        }
        finally
        {
            gate.Release();
        }
    }

    /// <summary>
    /// Stops one node as if its host had crashed. The call is refused when it would leave fewer
    /// than a majority running, because the cluster would then stop committing and the page would
    /// look broken rather than instructive.
    /// </summary>
    [JSExport]
    public static async Task<string> StopNodeAsync(int nodeIndex)
    {
        await gate.WaitAsync().ConfigureAwait(false);
        try
        {
            ClusterSession running = RequireCluster();
            await running.StopNodeAsync(nodeIndex).ConfigureAwait(false);

            return await running.StatusJsonAsync().ConfigureAwait(false);
        }
        finally
        {
            gate.Release();
        }
    }

    /// <summary>
    /// Starts a stopped node again. It comes back empty and catches up from the others, so a
    /// query on it afterwards returns the rows that were written while it was down.
    /// </summary>
    [JSExport]
    public static async Task<string> StartNodeAsync(int nodeIndex)
    {
        await gate.WaitAsync().ConfigureAwait(false);
        try
        {
            ClusterSession running = RequireCluster();
            await running.StartNodeAsync(nodeIndex).ConfigureAwait(false);

            return await running.StatusJsonAsync().ConfigureAwait(false);
        }
        finally
        {
            gate.Release();
        }
    }

    /// <summary>
    /// Waits until some running node believes it leads <paramref name="partitionId"/>, and returns
    /// its index. The page calls it after it stops a leader, to show when the failover finished.
    /// </summary>
    [JSExport]
    public static async Task<int> LeaderOfPartitionAsync(int partitionId)
    {
        await gate.WaitAsync().ConfigureAwait(false);
        try
        {
            return await RequireCluster().LeaderIndexAsync(partitionId).ConfigureAwait(false);
        }
        finally
        {
            gate.Release();
        }
    }

    /// <summary>
    /// Drops the traffic from one node to another, in that direction only. Both nodes keep
    /// running, which is how a network partition differs from a crash.
    /// </summary>
    [JSExport]
    public static async Task<string> BlockLinkAsync(int from, int to)
    {
        await gate.WaitAsync().ConfigureAwait(false);
        try
        {
            ClusterSession running = RequireCluster();
            running.BlockLink(from, to);

            return await running.StatusJsonAsync().ConfigureAwait(false);
        }
        finally
        {
            gate.Release();
        }
    }

    /// <summary>Restores every blocked link. A stopped node stays stopped.</summary>
    [JSExport]
    public static async Task<string> RestoreLinksAsync()
    {
        await gate.WaitAsync().ConfigureAwait(false);
        try
        {
            ClusterSession running = RequireCluster();
            running.RestoreLinks();

            return await running.StatusJsonAsync().ConfigureAwait(false);
        }
        finally
        {
            gate.Release();
        }
    }

    private static ClusterSession RequireCluster()
        => cluster ?? throw new InvalidOperationException("No cluster is running. Call startClusterAsync first.");

    private static string NormalizeDatabaseName(string? databaseName)
        => string.IsNullOrWhiteSpace(databaseName) ? DatabaseName : databaseName.Trim();
}
