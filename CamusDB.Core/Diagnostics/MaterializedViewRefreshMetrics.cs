/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Diagnostics;

/// <summary>
/// Always-on, process-wide totals of materialized-view rebuild work: how many rebuilds ended and how,
/// what they wrote, how their snapshot holds fared, and what the sweep did with abandoned ones.
///
/// <para>The same events go to the server meter (<see cref="ServerDiagnostics"/>) when diagnostics are
/// enabled, for the Prometheus endpoint; these totals are the in-process surface a test or an operator
/// reads directly. Nothing here carries a view's SQL or a row value — a view body and its rows are
/// user data.</para>
/// </summary>
public static class MaterializedViewRefreshMetrics
{
    private static long published;
    private static long failed;
    private static long conflicted;
    private static long rows;
    private static long mutations;
    private static long bytes;
    private static long holdsLost;
    private static long holdRenewals;
    private static long reclaimedStaging;
    private static long takeoverRestarts;
    private static long takeoverGaveUp;
    private static long recordsOnly;

    /// <summary>Rebuilds whose swap or creation was published.</summary>
    public static long Published => Interlocked.Read(ref published);

    /// <summary>Rebuilds that failed and were discarded.</summary>
    public static long Failed => Interlocked.Read(ref failed);

    /// <summary>Rebuilds refused because the view's definition moved while they ran (CADB0534).</summary>
    public static long Conflicted => Interlocked.Read(ref conflicted);

    /// <summary>Rows rebuilds wrote into staging relations.</summary>
    public static long Rows => Interlocked.Read(ref rows);

    /// <summary>Row and index writes rebuilds made.</summary>
    public static long Mutations => Interlocked.Read(ref mutations);

    /// <summary>Estimated bytes rebuilds wrote.</summary>
    public static long Bytes => Interlocked.Read(ref bytes);

    /// <summary>Rebuilds whose snapshot hold was lost before they finished.</summary>
    public static long HoldsLost => Interlocked.Read(ref holdsLost);

    /// <summary>Snapshot-hold renewals Kahuna confirmed during rebuilds.</summary>
    public static long HoldRenewals => Interlocked.Read(ref holdRenewals);

    /// <summary>Staging relations of abandoned rebuilds removed by a later refresh, a drop or the sweep.</summary>
    public static long ReclaimedStaging => Interlocked.Read(ref reclaimedStaging);

    /// <summary>Abandoned rebuilds the sweep restarted.</summary>
    public static long TakeoverRestarts => Interlocked.Read(ref takeoverRestarts);

    /// <summary>Abandoned rebuilds the sweep gave up on after its attempts ran out.</summary>
    public static long TakeoverGaveUp => Interlocked.Read(ref takeoverGaveUp);

    /// <summary>Records of rebuilds that had already published, removed without rebuilding.</summary>
    public static long RecordsOnly => Interlocked.Read(ref recordsOnly);

    internal static void RecordRebuild(string outcome, long rowCount, long mutationCount, long byteCount, int renewals, bool holdLost)
    {
        switch (outcome)
        {
            case "published": Interlocked.Increment(ref published); break;
            case "conflict": Interlocked.Increment(ref conflicted); break;
            default: Interlocked.Increment(ref failed); break;
        }

        Interlocked.Add(ref rows, rowCount);
        Interlocked.Add(ref mutations, mutationCount);
        Interlocked.Add(ref bytes, byteCount);
        Interlocked.Add(ref holdRenewals, renewals);

        if (holdLost)
            Interlocked.Increment(ref holdsLost);
    }

    internal static void RecordReclaim(string outcome)
    {
        switch (outcome)
        {
            case "staging_dropped": Interlocked.Increment(ref reclaimedStaging); break;
            case "restarted": Interlocked.Increment(ref takeoverRestarts); Interlocked.Increment(ref reclaimedStaging); break;
            case "gave_up": Interlocked.Increment(ref takeoverGaveUp); Interlocked.Increment(ref reclaimedStaging); break;
            case "record_only": Interlocked.Increment(ref recordsOnly); break;
            default: Interlocked.Increment(ref reclaimedStaging); break;
        }

        ServerDiagnostics.RecordMatViewReclaim(outcome);
    }
}
