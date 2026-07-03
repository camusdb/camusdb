
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Diagnostics;

/// <summary>
/// Lightweight, always-on process-wide counters for the branch ancestry read path.
///
/// Branch read operations must walk ancestor levels on a cache miss: a point read (<see cref="Storage.Kv.KvTableStore.GetRow"/>
/// or <see cref="Storage.Kv.KvTableStore.LookupUnique"/>) probes each ancestor level in order until a hit
/// or tombstone is found; a range scan (<see cref="Storage.Kv.KvTableStore.ScanRows"/> or
/// <see cref="Storage.Kv.KvTableStore.ScanIndex"/>) opens one iterator per lineage level and runs a k-way
/// merge. The cost therefore grows with lineage depth.
///
/// <list type="bullet">
/// <item><b>AncestorProbesTotal</b> — how many individual ancestor-level probes have fired in
/// GetRow and LookupUnique since the process started. Each iteration of the ancestry walk counts
/// as one probe.</item>
/// <item><b>ScanIteratorsTotal</b> — how many ancestor-level scan iterators have been opened in
/// ScanRows and ScanIndex branch paths. Each opened ancestor iterator counts once per scan call.
/// A scan over a 3-level chain (self + 2 ancestors) increments by 2.</item>
/// <item><b>DeepLineageWarnings</b> — how many times a <see cref="Storage.Kv.KvTableStore"/> was constructed
/// with a lineage deeper than <see cref="LineageWarningThreshold"/>. A rising count signals that
/// branch chains are approaching a depth where compaction or rebase should be considered.</item>
/// </list>
///
/// All counters are monotonic; use <see cref="Reset"/> only for test isolation.
/// There is no metrics exporter in the codebase yet; these are the in-process surface an operator
/// or test reads directly (and the natural hook point when an exporter is added).
/// </summary>
public static class BranchMetrics
{
    private static long ancestorProbesTotal;
    private static long scanIteratorsTotal;
    private static long deepLineageWarnings;

    /// <summary>
    /// Lineage depths at or beyond this threshold cause a <see cref="DeepLineageWarnings"/> increment
    /// and a log warning when the <see cref="Storage.Kv.KvTableStore"/> is opened.  This is an
    /// operational guideline, not a hard limit — reads still succeed for longer chains.  The default
    /// is deliberately conservative (10 levels) because there is no compaction or rebase yet; a
    /// typical workload uses chains of depth 1–3.  Settable so operators can tune it and tests can
    /// exercise the warning path with a shallow chain.
    /// </summary>
    public static int LineageWarningThreshold = 10;

    /// <summary>Total ancestor-level probes fired by GetRow and LookupUnique ancestry walks.</summary>
    public static long AncestorProbesTotal => Interlocked.Read(ref ancestorProbesTotal);

    /// <summary>Total ancestor-level scan iterators opened by ScanRows and ScanIndex branch paths.</summary>
    public static long ScanIteratorsTotal => Interlocked.Read(ref scanIteratorsTotal);

    /// <summary>
    /// How many table stores were opened with a lineage depth at or above
    /// <see cref="LineageWarningThreshold"/>.
    /// </summary>
    public static long DeepLineageWarnings => Interlocked.Read(ref deepLineageWarnings);

    /// <summary>Records one ancestor-level probe from a GetRow or LookupUnique ancestry walk.</summary>
    internal static void RecordAncestorProbe()
        => Interlocked.Increment(ref ancestorProbesTotal);

    /// <summary>Records <paramref name="count"/> ancestor scan iterators opened in one ScanRows or ScanIndex call.</summary>
    internal static void RecordScanIterators(int count)
        => Interlocked.Add(ref scanIteratorsTotal, count);

    /// <summary>
    /// Records that a table store was opened with a lineage depth at or above
    /// <see cref="LineageWarningThreshold"/>.
    /// </summary>
    internal static void RecordDeepLineageWarning()
        => Interlocked.Increment(ref deepLineageWarnings);

    /// <summary>Resets all counters. For test isolation only — not used in production.</summary>
    public static void Reset()
    {
        Interlocked.Exchange(ref ancestorProbesTotal, 0);
        Interlocked.Exchange(ref scanIteratorsTotal, 0);
        Interlocked.Exchange(ref deepLineageWarnings, 0);
    }
}
