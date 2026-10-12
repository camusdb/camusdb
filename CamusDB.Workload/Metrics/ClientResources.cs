/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;

namespace CamusDB.Workload.Metrics;

/// <summary>One reading of the load generator's own resource use.</summary>
public readonly record struct ClientResourceSnapshot(
    DateTime AtUtc,
    double CpuSeconds,
    long AllocatedBytes,
    int Gen0,
    int Gen1,
    int Gen2,
    double GcPauseSeconds,
    long WorkingSetBytes,
    int ThreadPoolThreads,
    long ThreadPoolQueue);

/// <summary>
/// What the load generator itself spent during the measured window, and whether it was the thing that
/// ran out.
///
/// <para>This exists because the most expensive mistake in a throughput measurement is attributing
/// the client's ceiling to the server. A generator that is CPU-bound, pausing for GC, or simply not
/// allowed enough in-flight work produces a flat throughput curve that looks exactly like a saturated
/// database.</para>
///
/// <para>The thread-pool backlog is judged on how long it lasted, not on its highest single reading.
/// <see cref="PeakThreadPoolQueue"/> is the highest one-second sample and is kept for the reader;
/// <see cref="ThreadPoolQueueP95"/> is the depth 95% of the samples stayed at or under, and
/// <see cref="ThreadPoolBacklogSeconds"/> is the longest unbroken stretch the queue spent above
/// <see cref="ClientResourceSampler.ThreadPoolQueueBar"/>. Only those two raise the warning.</para>
/// </summary>
public sealed record ClientResources(
    double MeasuredSeconds,
    int ProcessorCount,
    double CpuSecondsUsed,
    double CpuUtilization,
    long AllocatedBytes,
    double AllocatedMbPerSecond,
    int Gen0Collections,
    int Gen1Collections,
    int Gen2Collections,
    double GcPauseSeconds,
    double GcPauseFraction,
    long PeakWorkingSetBytes,
    long PeakThreadPoolQueue,
    double ThreadPoolQueueP95,
    double ThreadPoolBacklogSeconds,
    int ThreadPoolBacklogSamples,
    int ThreadPoolQueueSamples,
    string Mode,
    int Workers,
    int Connections,
    int MaxInFlight,
    double RequiredInFlight,
    IReadOnlyList<string> Warnings)
{
    /// <summary>True when nothing suggests the generator limited the result.</summary>
    public bool HeadroomAvailable => Warnings.Count == 0;
}

/// <summary>
/// Samples the load generator's own process once a second for the whole run, so the resource figures
/// can be cut to the measured window afterwards.
///
/// <para>Sampling rather than taking two end-point readings is what makes a peak visible. A
/// thread-pool queue that was 4,000 deep for ten seconds in the middle of the window reads as zero at
/// both ends, and that queue is precisely the evidence that the client, not the server, set the
/// pace.</para>
/// </summary>
public sealed class ClientResourceSampler : IAsyncDisposable
{
    private readonly List<ClientResourceSnapshot> _snapshots = new();
    private readonly TimeSpan _interval;
    private CancellationTokenSource? _cts;
    private Task? _loop;

    public ClientResourceSampler(TimeSpan? interval = null) => _interval = interval ?? TimeSpan.FromSeconds(1);

    public void Start()
    {
        if (_loop is not null)
            return;

        _cts = new CancellationTokenSource();
        Capture();
        _loop = Task.Run(async () =>
        {
            while (!_cts.Token.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(_interval, _cts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                Capture();
            }
        });
    }

    public async Task StopAsync()
    {
        if (_loop is null)
            return;

        _cts!.Cancel();
        try
        {
            await _loop.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Expected on the cancellation this method requested.
        }
        _loop = null;
        Capture();
    }

    private void Capture()
    {
        using Process process = Process.GetCurrentProcess();
        lock (_snapshots)
        {
            _snapshots.Add(new ClientResourceSnapshot(
                AtUtc: DateTime.UtcNow,
                CpuSeconds: process.TotalProcessorTime.TotalSeconds,
                AllocatedBytes: GC.GetTotalAllocatedBytes(precise: false),
                Gen0: GC.CollectionCount(0),
                Gen1: GC.CollectionCount(1),
                Gen2: GC.CollectionCount(2),
                GcPauseSeconds: GC.GetTotalPauseDuration().TotalSeconds,
                WorkingSetBytes: process.WorkingSet64,
                ThreadPoolThreads: ThreadPool.ThreadCount,
                ThreadPoolQueue: ThreadPool.PendingWorkItemCount));
        }
    }

    /// <summary>The thread-pool queue depth above which pending work counts as a backlog.</summary>
    public const long ThreadPoolQueueBar = 100;

    /// <summary>How long the queue must stay above <see cref="ThreadPoolQueueBar"/> without a break
    /// before it counts as the client holding operations back. One or two samples over the bar are
    /// the normal shape of a burst of continuations (a fault heals, a batch of callbacks lands at
    /// once); a generator that is really behind stays behind.</summary>
    public static readonly TimeSpan ThreadPoolBacklogDwell = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Reduces the samples inside the measured window to one verdict. Returns null when the window
    /// caught fewer than two samples, because a delta needs two: a made-up zero would read as "the
    /// client was idle", which is the opposite of "we did not measure".
    /// </summary>
    public ClientResources? Summarize(
        DateTime measureStartUtc, double measuredSeconds, string mode, int workers, int connections,
        int maxInFlight, double throughputOpsPerSec, double meanLatencyMs)
    {
        List<ClientResourceSnapshot> snapshots;
        lock (_snapshots)
            snapshots = _snapshots.ToList();

        return Summarize(snapshots, measureStartUtc, measuredSeconds, mode, workers, connections, maxInFlight,
            throughputOpsPerSec, meanLatencyMs);
    }

    /// <summary>The reduction itself, over any sample list, so the rules can be tested on a shaped
    /// series instead of on whatever the test host's thread pool happened to be doing.</summary>
    public static ClientResources? Summarize(
        IReadOnlyList<ClientResourceSnapshot> snapshots, DateTime measureStartUtc, double measuredSeconds,
        string mode, int workers, int connections, int maxInFlight, double throughputOpsPerSec, double meanLatencyMs)
    {
        DateTime end = measureStartUtc.AddSeconds(measuredSeconds);
        List<ClientResourceSnapshot> window = snapshots
            .Where(s => s.AtUtc >= measureStartUtc && s.AtUtc <= end).OrderBy(s => s.AtUtc).ToList();

        if (window.Count < 2)
            return null;

        ClientResourceSnapshot first = window[0];
        ClientResourceSnapshot last = window[^1];
        double seconds = (last.AtUtc - first.AtUtc).TotalSeconds;
        if (seconds <= 0)
            return null;

        int processors = Environment.ProcessorCount;
        double cpuSeconds = last.CpuSeconds - first.CpuSeconds;
        double utilization = cpuSeconds / (seconds * processors);
        long allocated = last.AllocatedBytes - first.AllocatedBytes;
        double gcPause = last.GcPauseSeconds - first.GcPauseSeconds;
        long peakQueue = window.Max(s => s.ThreadPoolQueue);
        ThreadPoolBacklog backlog = MeasureThreadPoolBacklog(window);

        // Little's Law: sustaining X operations per second that each take L seconds needs X*L of them
        // in flight at once.
        double requiredInFlight = throughputOpsPerSec * (meanLatencyMs / 1000.0);

        List<string> warnings = new();
        if (utilization > 0.80)
            warnings.Add($"the load generator used {utilization:P0} of its {processors} core(s); the measured ceiling may be the client's, not the server's.");
        if (gcPause / seconds > 0.10)
            warnings.Add($"garbage collection paused the generator for {gcPause / seconds:P0} of the window.");
        if (backlog.LongestSeconds >= ThreadPoolBacklogDwell.TotalSeconds)
            warnings.Add($"the generator's thread-pool queue stayed above {ThreadPoolQueueBar} pending item(s) for {backlog.LongestSeconds:F0} s without a break (peak {peakQueue}); operations were waiting on the client before reaching the server.");
        else if (backlog.P95 > ThreadPoolQueueBar)
            warnings.Add($"the generator's thread-pool queue was above {ThreadPoolQueueBar} pending item(s) in more than 5% of samples (p95 {backlog.P95:F0}, {backlog.Samples} of {window.Count} samples, peak {peakQueue}); operations were waiting on the client before reaching the server.");

        // Only open loop can fail this check. A closed-loop run holds exactly `workers` operations in
        // flight by construction, so the required figure always equals the worker count and a warning
        // there would fire on every healthy run — the question "was N workers enough?" is answered by
        // a sweep showing throughput stop rising with N, not by one run's arithmetic.
        bool openLoop = string.Equals(mode, "open", StringComparison.OrdinalIgnoreCase);
        if (openLoop && maxInFlight > 0 && requiredInFlight >= 0.9 * maxInFlight)
            warnings.Add($"sustaining this throughput needs about {requiredInFlight:F0} operations in flight against a cap of {maxInFlight}; the client's --max-in-flight, not the server, is what bounded the result.");

        return new ClientResources(
            MeasuredSeconds: seconds,
            ProcessorCount: processors,
            CpuSecondsUsed: cpuSeconds,
            CpuUtilization: utilization,
            AllocatedBytes: allocated,
            AllocatedMbPerSecond: allocated / 1024.0 / 1024.0 / seconds,
            Gen0Collections: last.Gen0 - first.Gen0,
            Gen1Collections: last.Gen1 - first.Gen1,
            Gen2Collections: last.Gen2 - first.Gen2,
            GcPauseSeconds: gcPause,
            GcPauseFraction: gcPause / seconds,
            PeakWorkingSetBytes: window.Max(s => s.WorkingSetBytes),
            PeakThreadPoolQueue: peakQueue,
            ThreadPoolQueueP95: backlog.P95,
            ThreadPoolBacklogSeconds: backlog.LongestSeconds,
            ThreadPoolBacklogSamples: backlog.Samples,
            ThreadPoolQueueSamples: window.Count,
            Mode: mode,
            Workers: workers,
            Connections: connections,
            MaxInFlight: maxInFlight,
            RequiredInFlight: requiredInFlight,
            Warnings: warnings);
    }

    /// <summary>How the thread-pool queue behaved over a window: the 95th percentile of its depth,
    /// the number of samples above the bar, and the longest unbroken run above the bar in seconds.</summary>
    public readonly record struct ThreadPoolBacklog(double P95, int Samples, double LongestSeconds);

    /// <summary>
    /// Measures the backlog on a time-ordered window. A run above the bar lasts from its first sample
    /// to the first sample that is back at or under the bar; a run still open at the window's end is
    /// closed by the last sample plus the typical gap between samples, so an isolated spike at a
    /// one-second cadence reads as one second, never as zero and never as the whole tail.
    /// </summary>
    public static ThreadPoolBacklog MeasureThreadPoolBacklog(IReadOnlyList<ClientResourceSnapshot> window)
    {
        if (window.Count == 0)
            return new ThreadPoolBacklog(0, 0, 0);

        long[] depths = window.Select(s => s.ThreadPoolQueue).OrderBy(d => d).ToArray();
        int rank = Math.Clamp((int)Math.Ceiling(depths.Length * 0.95) - 1, 0, depths.Length - 1); // nearest rank
        double p95 = depths[rank];

        double cadence = window.Count > 1
            ? (window[^1].AtUtc - window[0].AtUtc).TotalSeconds / (window.Count - 1)
            : 0;

        int above = 0;
        double longest = 0;
        DateTime? runStart = null;
        foreach (ClientResourceSnapshot sample in window)
        {
            if (sample.ThreadPoolQueue > ThreadPoolQueueBar)
            {
                above++;
                runStart ??= sample.AtUtc;
                continue;
            }

            if (runStart is DateTime start)
            {
                longest = Math.Max(longest, (sample.AtUtc - start).TotalSeconds);
                runStart = null;
            }
        }

        if (runStart is DateTime open)
            longest = Math.Max(longest, (window[^1].AtUtc - open).TotalSeconds + cadence);

        return new ThreadPoolBacklog(p95, above, longest);
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
        _cts?.Dispose();
    }
}
