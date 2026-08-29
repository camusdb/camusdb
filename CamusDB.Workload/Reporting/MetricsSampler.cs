/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using System.Text;

namespace CamusDB.Workload.Reporting;

/// <summary>What one node's scraping looked like over the whole collection period.</summary>
public sealed record NodeSampleStats(string Node, string Url, int Succeeded, int Failed, string? LastError);

/// <summary>The outcome of a collection period, written alongside the series as its provenance.</summary>
/// <param name="CollectorFailure">
/// Why collection stopped early, or null when it ran to the end. A series that is short because the
/// collector died reads exactly like a series that is short because the run was — and only one of
/// those means the evidence is incomplete.
/// </param>
public sealed record MetricsSamplerResult(
    int Rounds,
    long RowsWritten,
    string CollectorStartedUtc,
    string CollectorStoppedUtc,
    double IntervalSeconds,
    string? CollectorFailure,
    IReadOnlyList<NodeSampleStats> Nodes);

/// <summary>
/// Scrapes every node's Prometheus endpoint on a fixed interval for the whole run and appends the
/// samples to <c>node-metrics.csv</c>.
///
/// <para>The reason this exists rather than one scrape at the end: a single end-of-run reading of a
/// cumulative counter cannot separate the measured window from seeding and warm-up, cannot show that
/// one node carried the load, and cannot show a queue that grew all run and would have kept growing.
/// Those three questions are the whole point of a cluster capacity measurement.</para>
///
/// <para>Every node in a round is stamped with the same instant, so a cross-node comparison at an
/// instant is meaningful even though the scrapes complete at slightly different times. A scrape that
/// fails is recorded as <c>workload_scrape_ok = 0</c> rather than dropped: a node that stopped
/// answering is a finding, and a silent gap in the series looks exactly like an idle node.</para>
///
/// <para>The collector never fails the run. It is instrumentation, and a run that produced good
/// measured data must not be discarded because a metrics port refused a connection.</para>
/// </summary>
public sealed class MetricsSampler : IAsyncDisposable
{
    private readonly IReadOnlyList<NodeTarget> _targets;
    private readonly TimeSpan _interval;
    private readonly string _csvPath;
    private readonly HttpClient _http;
    private readonly Dictionary<string, string> _lastScrape = new(StringComparer.Ordinal);
    private readonly Dictionary<string, int> _ok = new(StringComparer.Ordinal);
    private readonly Dictionary<string, int> _failed = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string?> _lastError = new(StringComparer.Ordinal);

    private CancellationTokenSource? _cts;
    private Task? _loop;
    private StreamWriter? _writer;
    private DateTime _startedUtc;
    private int _rounds;
    private long _rows;
    private string? _loopFailure;

    /// <summary>
    /// Each node's last successful scrape, verbatim. The report reads one of these for histogram means:
    /// the series keeps every <c>_sum</c> and <c>_count</c> but drops buckets, so a mean is available
    /// from the series while an approximate quantile still needs the raw text.
    /// </summary>
    public IReadOnlyDictionary<string, string> LastScrapes => _lastScrape;

    /// <summary>Smallest interval accepted, so a mistyped value cannot turn the collector into load.</summary>
    public static readonly TimeSpan MinInterval = TimeSpan.FromSeconds(1);

    public MetricsSampler(IReadOnlyList<NodeTarget> targets, TimeSpan interval, string csvPath)
    {
        _targets = targets;
        _interval = interval < MinInterval ? MinInterval : interval;
        _csvPath = csvPath;

        // A scrape must not outlive its round; a node that hangs would otherwise stall every later
        // sample and leave a hole in the series exactly where the interesting behaviour happened.
        _http = new HttpClient { Timeout = _interval };

        foreach (NodeTarget t in targets)
        {
            _ok[t.Name] = 0;
            _failed[t.Name] = 0;
            _lastError[t.Name] = null;
        }
    }

    public void Start()
    {
        if (_loop is not null)
            throw new InvalidOperationException("The metrics sampler is already running.");

        Directory.CreateDirectory(Path.GetDirectoryName(_csvPath)!);
        _writer = new StreamWriter(_csvPath, append: false, Encoding.UTF8);
        _writer.WriteLine(MetricsCsv.Header);
        _startedUtc = DateTime.UtcNow;
        _cts = new CancellationTokenSource();
        _loop = Task.Run(() => LoopAsync(_cts.Token));
    }

    /// <summary>
    /// Stops the loop and flushes. Safe to call when <see cref="Start"/> was never called — a run
    /// configured without node targets still takes this path.
    /// </summary>
    public async Task<MetricsSamplerResult?> StopAsync()
    {
        if (_loop is null)
            return null;

        _cts!.Cancel();
        try
        {
            await _loop.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Expected: the loop observes the cancellation it was asked for.
        }
        catch (Exception ex)
        {
            // The loop died on something else — a full disk, a closed writer. Report it and keep
            // going: this method runs in the run's finally block, and letting it throw would replace
            // the run's real outcome with the collector's, which is the one thing the collector must
            // never do.
            _loopFailure = $"{ex.GetType().Name}: {ex.Message}";
            Console.Error.WriteLine($"  ⚠ metrics collection stopped early: {_loopFailure}");
        }

        try
        {
            await _writer!.FlushAsync().ConfigureAwait(false);
            await _writer.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _loopFailure ??= $"{ex.GetType().Name}: {ex.Message}";
            Console.Error.WriteLine($"  ⚠ metrics file could not be closed cleanly: {ex.GetType().Name}: {ex.Message}");
        }

        _writer = null;
        _loop = null;

        return new MetricsSamplerResult(
            Rounds: _rounds,
            RowsWritten: _rows,
            CollectorStartedUtc: _startedUtc.ToString("O"),
            CollectorStoppedUtc: DateTime.UtcNow.ToString("O"),
            IntervalSeconds: _interval.TotalSeconds,
            CollectorFailure: _loopFailure,
            Nodes: _targets
                .Select(t => new NodeSampleStats(t.Name, t.MetricsUrl.ToString(), _ok[t.Name], _failed[t.Name], _lastError[t.Name]))
                .ToList());
    }

    /// <summary>
    /// Writes each node's last successful scrape verbatim. The series drops histogram buckets to stay
    /// small, so the raw text is what a later question about a latency distribution has to read.
    /// </summary>
    public async Task WriteLastScrapesAsync(string outputDir, CancellationToken ct)
    {
        foreach ((string node, string text) in _lastScrape)
        {
            string safe = string.Concat(node.Select(c => char.IsLetterOrDigit(c) || c is '-' or '_' ? c : '_'));
            await File.WriteAllTextAsync(Path.Combine(outputDir, $"metrics-{safe}.txt"), text, ct).ConfigureAwait(false);
        }
    }

    private async Task LoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            long roundStart = Stopwatch.GetTimestamp();
            await SampleOnceAsync(ct).ConfigureAwait(false);

            TimeSpan elapsed = Stopwatch.GetElapsedTime(roundStart);
            TimeSpan wait = _interval - elapsed;
            if (wait > TimeSpan.Zero)
            {
                try
                {
                    await Task.Delay(wait, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }
    }

    private async Task SampleOnceAsync(CancellationToken ct)
    {
        long instant = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        // Scrape every node concurrently: a serial round would stamp the last node with a reading
        // taken seconds after the first, and the per-node comparison assumes one instant.
        Task<(NodeTarget Target, string? Text, string? Error, double Ms)>[] scrapes = _targets
            .Select(t => ScrapeAsync(t, ct))
            .ToArray();

        var results = await Task.WhenAll(scrapes).ConfigureAwait(false);
        _rounds++;

        foreach (var (target, text, error, ms) in results)
        {
            WriteRow(new MetricPoint(instant, target.Name, "workload_scrape_ok", "", text is null ? 0 : 1));
            WriteRow(new MetricPoint(instant, target.Name, "workload_scrape_ms", "", ms));

            if (text is null)
            {
                _failed[target.Name]++;
                _lastError[target.Name] = error;
                continue;
            }

            _ok[target.Name]++;
            _lastScrape[target.Name] = text;

            foreach (PromSample sample in PrometheusScrape.ParseSamples(text))
            {
                // Histogram buckets are the bulk of a scrape and are not needed for a rate or a
                // backlog; the last raw scrape is kept whole for the questions that do need them.
                if (sample.Name.EndsWith("_bucket", StringComparison.Ordinal) || sample.Name == "target_info")
                    continue;

                WriteRow(new MetricPoint(instant, target.Name, sample.Name, MetricsCsv.CanonicalLabels(sample.Labels), sample.Value));
            }
        }

        await _writer!.FlushAsync(ct).ConfigureAwait(false);
    }

    private void WriteRow(in MetricPoint point)
    {
        _writer!.WriteLine(MetricsCsv.RenderRow(point));
        _rows++;
    }

    private async Task<(NodeTarget Target, string? Text, string? Error, double Ms)> ScrapeAsync(NodeTarget target, CancellationToken ct)
    {
        long started = Stopwatch.GetTimestamp();
        try
        {
            string text = await _http.GetStringAsync(target.MetricsUrl, ct).ConfigureAwait(false);
            return (target, text, null, Stopwatch.GetElapsedTime(started).TotalMilliseconds);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            return (target, null, $"{ex.GetType().Name}: {ex.Message}", Stopwatch.GetElapsedTime(started).TotalMilliseconds);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
        _cts?.Dispose();
        _http.Dispose();
    }
}
