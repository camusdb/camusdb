/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using System.Globalization;
using System.Text;
using CamusDB.Client;
using CamusDB.Workload.Client;
using CamusDB.Workload.Operations;
using CamusDB.Workload.Workload;

namespace CamusDB.Workload.Metrics;

/// <summary>One <c>SELECT COUNT(*)</c> answer from one gateway for one table.</summary>
/// <param name="Rows">The count returned, or null when the statement failed.</param>
/// <param name="Status">"ok", "short" (a count below the expected rows), or "error".</param>
/// <param name="Code">The error code the failure classified to; empty on success.</param>
public sealed record ScanProbeSample(
    DateTime TsUtc,
    string Endpoint,
    string Table,
    long Expected,
    long? Rows,
    double ElapsedMs,
    string Status,
    string Code);

/// <summary>
/// The in-window scan-visibility verdict, written into <c>reconciliation.json</c>.
///
/// <para>A short count is never tolerated: the workload never deletes a row, so a scan that
/// returns fewer rows than exist has failed to see committed data, whatever else was going on. An
/// error is a gateway that could not answer a read-only aggregate at all; under
/// <c>--expect-faults</c> a killed node is expected to refuse for a while, so errors are reported
/// and waived there, but on a fault-free run a read that aborts is the defect this probe exists to
/// catch (a scan answered with a write-conflict code).</para>
/// </summary>
public sealed record ScanProbeResult(
    long Probes,
    long Exact,
    long Short,
    long Errors,
    long InWindowProbes,
    long InWindowShort,
    long InWindowErrors,
    long MinRows,
    double MeanElapsedMs,
    double MaxElapsedMs,
    int Gateways,
    double IntervalSeconds,
    bool ErrorsWaived,
    IReadOnlyList<string> Failures,
    string? ProbeFailure)
{
    public bool Passed => Short == 0 && (Errors == 0 || ErrorsWaived) && ProbeFailure is null && Probes > 0;
}

/// <summary>
/// Runs <c>SELECT COUNT(*)</c> against every gateway on a fixed interval for the whole run and
/// records what each answered.
///
/// <para>Why it exists: the post-run reconciliation counts rows on a quiet cluster and cannot see a
/// scan that drops rows only while writes are in flight — which is exactly what happened
/// (feature e31cf9bc: 151 of 207 in-window counts short on every gateway, 2,000 every time idle).
/// The probe is the run's own witness for that class: it asks each gateway directly (routing off,
/// so the count runs where it was sent), never retries, and records the raw answer. It is not a
/// measurement of throughput and it never fails the run by itself — its verdict is folded into
/// reconciliation, where a short count or a failed read fails the run like any other broken
/// invariant.</para>
///
/// <para>Each gateway gets its own read-only connection with a probe-specific request deadline: an
/// aggregate under load can take seconds, and the load connections' deadline is tuned for point
/// operations. A gateway whose connection cannot be opened is retried on every round and counts as
/// an error until it answers.</para>
/// </summary>
public sealed class ScanProbe : IAsyncDisposable
{
    private readonly IReadOnlyList<string> _endpoints;
    private readonly string _database;
    private readonly string _protocol;
    private readonly ConnectionSettings _settings;
    private readonly Dataset _dataset;
    private readonly TimeSpan _interval;
    private readonly string _csvPath;
    private readonly CamusConnection?[] _connections;
    private readonly List<ScanProbeSample> _samples = [];
    private readonly object _sync = new();

    private CancellationTokenSource? _cts;
    private Task? _loop;
    private StreamWriter? _writer;
    private string? _loopFailure;

    /// <summary>Smallest interval accepted, so a mistyped value cannot turn the probe into load.</summary>
    public static readonly TimeSpan MinInterval = TimeSpan.FromSeconds(1);

    public const string CsvHeader = "ts,endpoint,table,expected,rows,elapsed_ms,status,code";

    public ScanProbe(
        IReadOnlyList<string> endpoints, string database, string protocol, ConnectionSettings settings,
        Dataset dataset, TimeSpan interval, string csvPath)
    {
        _endpoints = endpoints;
        _database = database;
        _protocol = protocol;
        _settings = settings;
        _dataset = dataset;
        _interval = interval < MinInterval ? MinInterval : interval;
        _csvPath = csvPath;
        _connections = new CamusConnection?[endpoints.Count];
    }

    public TimeSpan Interval => _interval;

    /// <summary>Splits the CLI endpoint pool ("http://a:1,http://b:2") into the gateways to probe.</summary>
    public static IReadOnlyList<string> EndpointsOf(string pool) =>
        pool.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Distinct().ToList();

    public void Start()
    {
        if (_loop is not null)
            throw new InvalidOperationException("The scan probe is already running.");

        Directory.CreateDirectory(Path.GetDirectoryName(_csvPath)!);
        _writer = new StreamWriter(_csvPath, append: false, Encoding.UTF8);
        _writer.WriteLine(CsvHeader);
        _cts = new CancellationTokenSource();
        _loop = Task.Run(() => LoopAsync(_cts.Token));
    }

    /// <summary>Stops the loop, flushes the csv and returns every sample taken. Safe when never started.</summary>
    public async Task<IReadOnlyList<ScanProbeSample>> StopAsync()
    {
        if (_loop is null)
            return [];

        _cts!.Cancel();
        try
        {
            await _loop.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // The loop's own stop signal.
        }
        catch (Exception ex)
        {
            _loopFailure ??= $"{ex.GetType().Name}: {ex.Message}";
        }

        await _writer!.FlushAsync().ConfigureAwait(false);
        _writer.Dispose();
        _writer = null;
        _loop = null;

        lock (_sync)
            return _samples.ToList();
    }

    /// <summary>Why the probe loop stopped early, or null when it ran until asked to stop.</summary>
    public string? LoopFailure => _loopFailure;

    private async Task LoopAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                long roundStart = Stopwatch.GetTimestamp();

                for (int e = 0; e < _endpoints.Count && !ct.IsCancellationRequested; e++)
                    for (int t = 0; t < _dataset.TableNames.Count && !ct.IsCancellationRequested; t++)
                        await ProbeOnceAsync(e, t, ct).ConfigureAwait(false);

                TimeSpan remaining = _interval - Stopwatch.GetElapsedTime(roundStart);
                if (remaining > TimeSpan.Zero)
                    await Task.Delay(remaining, ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            // A dead probe must read as "no verdict", never as "no problem": StopAsync surfaces this.
            _loopFailure = $"{ex.GetType().Name}: {ex.Message}";
        }
    }

    private async Task ProbeOnceAsync(int endpointIndex, int tableIndex, CancellationToken ct)
    {
        string endpoint = _endpoints[endpointIndex];
        string table = _dataset.TableNames[tableIndex];
        long expected = _dataset.TableRowCount(tableIndex);
        DateTime ts = DateTime.UtcNow;
        long started = Stopwatch.GetTimestamp();

        try
        {
            CamusConnection conn = _connections[endpointIndex]
                ??= await ConnectionSet.OpenProbeAsync(endpoint, _database, _protocol, _settings, ct).ConfigureAwait(false);

            using CamusCommand cmd = conn.CreateCamusCommand($"SELECT COUNT(*) FROM {table}");
            using CamusDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            long rows = await reader.ReadAsync(ct).ConfigureAwait(false) && !reader.IsDBNull(0) ? reader.GetInt64(0) : 0;

            double elapsed = Stopwatch.GetElapsedTime(started).TotalMilliseconds;
            Record(new ScanProbeSample(ts, endpoint, table, expected, rows, elapsed, rows == expected ? "ok" : "short", ""));
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            // A failed statement leaves the connection where it is; a failed open leaves the slot null so
            // the next round tries again. Either way the round records an error rather than skipping.
            double elapsed = Stopwatch.GetElapsedTime(started).TotalMilliseconds;
            (_, string code) = ErrorClassifier.Classify(ex);
            Record(new ScanProbeSample(ts, endpoint, table, expected, null, elapsed, "error", code));
        }
    }

    private void Record(ScanProbeSample sample)
    {
        lock (_sync)
        {
            _samples.Add(sample);
            _writer?.WriteLine(string.Join(',',
                sample.TsUtc.ToString("O", CultureInfo.InvariantCulture),
                sample.Endpoint,
                sample.Table,
                sample.Expected.ToString(CultureInfo.InvariantCulture),
                sample.Rows?.ToString(CultureInfo.InvariantCulture) ?? "",
                sample.ElapsedMs.ToString("F1", CultureInfo.InvariantCulture),
                sample.Status,
                sample.Code));
        }
    }

    /// <summary>
    /// Folds the samples into the verdict. <paramref name="measureStartUtc"/> and
    /// <paramref name="measureSeconds"/> bound the measured window, reported separately because a
    /// short count during warm-up is exactly as wrong as one inside the window but a reader comparing
    /// against the server's own metrics wants the in-window figure.
    /// </summary>
    public static ScanProbeResult Summarize(
        IReadOnlyList<ScanProbeSample> samples, DateTime measureStartUtc, double measureSeconds,
        int gateways, TimeSpan interval, bool expectFaults, string? probeFailure)
    {
        DateTime windowEnd = measureStartUtc.AddSeconds(measureSeconds);
        bool InWindow(ScanProbeSample s) => measureStartUtc != DateTime.MinValue && s.TsUtc >= measureStartUtc && s.TsUtc <= windowEnd;

        long exact = 0, shortCount = 0, errors = 0, inWindow = 0, inWindowShort = 0, inWindowErrors = 0;
        long minRows = long.MaxValue;
        double sumElapsed = 0, maxElapsed = 0;
        int answered = 0;
        List<string> failures = [];
        List<ScanProbeSample> shortSamples = [];
        Dictionary<string, int> errorCodes = new(StringComparer.Ordinal);

        foreach (ScanProbeSample s in samples)
        {
            bool inside = InWindow(s);
            if (inside)
                inWindow++;

            if (s.Rows is long rows)
            {
                answered++;
                sumElapsed += s.ElapsedMs;
                maxElapsed = Math.Max(maxElapsed, s.ElapsedMs);
                minRows = Math.Min(minRows, rows);
                if (rows == s.Expected)
                {
                    exact++;
                }
                else
                {
                    shortCount++;
                    if (inside)
                        inWindowShort++;
                    shortSamples.Add(s);
                }
            }
            else
            {
                errors++;
                if (inside)
                    inWindowErrors++;
                errorCodes[s.Code] = errorCodes.GetValueOrDefault(s.Code) + 1;
            }
        }

        if (shortCount > 0)
        {
            string sample = string.Join("; ", shortSamples.Take(5).Select(s =>
                $"{s.TsUtc:HH:mm:ss}Z {s.Endpoint} {s.Table}: {s.Rows} of {s.Expected}"));
            failures.Add(
                $"scan probe: {shortCount} of {samples.Count} COUNT(*) probe(s) returned fewer rows than exist " +
                $"(lowest {minRows}); a read-committed scan under load skipped committed rows. First: {sample}.");
        }

        if (errors > 0)
        {
            string codes = string.Join(", ", errorCodes.OrderByDescending(kv => kv.Value).Select(kv => $"{kv.Key}×{kv.Value}"));
            string line = $"scan probe: {errors} of {samples.Count} COUNT(*) probe(s) failed ({codes}); " +
                          "a read-only aggregate could not be answered.";
            failures.Add(expectFaults ? line + " (waived by --expect-faults)" : line);
        }

        if (probeFailure is not null)
            failures.Add($"scan probe: the probe loop stopped early ({probeFailure}); its verdict is incomplete.");
        else if (samples.Count == 0)
            failures.Add("scan probe: no probe was taken; the verdict is missing.");

        return new ScanProbeResult(
            samples.Count, exact, shortCount, errors, inWindow, inWindowShort, inWindowErrors,
            minRows == long.MaxValue ? -1 : minRows,
            answered == 0 ? 0 : sumElapsed / answered, maxElapsed,
            gateways, interval.TotalSeconds,
            ErrorsWaived: expectFaults,
            failures, probeFailure);
    }

    public async ValueTask DisposeAsync()
    {
        if (_loop is not null)
            await StopAsync().ConfigureAwait(false);

        foreach (CamusConnection? c in _connections)
        {
            if (c is not null)
                await c.DisposeAsync().ConfigureAwait(false);
        }
    }
}
