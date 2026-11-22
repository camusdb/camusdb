/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics.Metrics;
using System.Globalization;
using System.Text;

namespace CamusDB.Workload.Metrics;

/// <summary>
/// Collects the driver's round-trip profile for the whole run and writes it as a time series, so the
/// time a statement spends outside the server's handler can be split into the driver's queue, the
/// exchange with the server and the wait for the caller to resume.
///
/// <para>The driver publishes the profile on its own meter and records nothing until a listener
/// enables it, so starting this sampler is what turns the profile on. A driver built before the
/// profile has no such meter, and the file then holds only its header.</para>
///
/// <para><b>Output.</b> <c>client-round-trip.csv</c>, one row per series and sample:
/// <c>unix_ms,metric,kind,stage,value</c>. Every value is a total since the sampler started, like a
/// Prometheus counter, so a reader takes the difference between two samples to get a window. The
/// measured window is in <c>run-meta.json</c>. <c>ops</c> rows have an empty stage; <c>time_ms</c> rows
/// carry the stage.</para>
///
/// <para><b>Cost.</b> The listener runs on the thread that completes each operation. It adds to
/// fixed cells with interlocked operations and allocates nothing, because the generator shares its
/// host with the cluster in most runs and must not become the bottleneck it measures.</para>
/// </summary>
public sealed class ClientRoundTripSampler
{
    /// <summary>The driver's meter for the profile. Spelled out so the workload builds against a
    /// driver that predates it.</summary>
    public const string MeterName = "CamusDB.Client.RoundTrip";

    public const string FileName = "client-round-trip.csv";

    private const string OpsInstrument = "camusdb.client.round_trip.ops";

    private const string TimeInstrument = "camusdb.client.round_trip.time";

    private static readonly string[] Kinds = ["query", "query_txn", "non_query", "start", "commit", "rollback", "prepare", "close"];

    private static readonly string[] Stages = ["queue", "exchange", "send", "resume"];

    private readonly long[] ops = new long[Kinds.Length];

    // Nanoseconds, indexed by kind * Stages.Length + stage. Whole nanoseconds lose nothing that
    // matters at these magnitudes and let the cells be added to without a lock.
    private readonly long[] timeNs = new long[Kinds.Length * Stages.Length];

    private readonly string path;

    private readonly TimeSpan interval;

    private readonly MeterListener listener = new();

    private readonly CancellationTokenSource stop = new();

    private Task? loop;

    public ClientRoundTripSampler(string outputDirectory, TimeSpan interval)
    {
        path = Path.Combine(outputDirectory, FileName);
        this.interval = interval < TimeSpan.FromSeconds(1) ? TimeSpan.FromSeconds(1) : interval;
    }

    /// <summary>Enables the profile and starts writing samples.</summary>
    public void Start()
    {
        File.WriteAllText(path, "unix_ms,metric,kind,stage,value\n");

        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == MeterName)
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>(OnCount);
        listener.SetMeasurementEventCallback<double>(OnTime);
        listener.Start();

        loop = Task.Run(SampleLoopAsync);
    }

    /// <summary>Writes a last sample and turns the profile off.</summary>
    public async Task StopAsync()
    {
        if (loop is null)
            return;

        stop.Cancel();
        try { await loop.ConfigureAwait(false); } catch (OperationCanceledException) { }

        WriteSample();
        listener.Dispose();
        stop.Dispose();
    }

    private void OnCount(Instrument instrument, long value, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
    {
        if (instrument.Name != OpsInstrument)
            return;

        int kind = IndexOf(Kinds, Tag(tags, "camusdb.round_trip.kind"));
        if (kind >= 0)
            Interlocked.Add(ref ops[kind], value);
    }

    private void OnTime(Instrument instrument, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
    {
        if (instrument.Name != TimeInstrument)
            return;

        int kind = IndexOf(Kinds, Tag(tags, "camusdb.round_trip.kind"));
        int stage = IndexOf(Stages, Tag(tags, "camusdb.round_trip.stage"));
        if (kind >= 0 && stage >= 0)
            Interlocked.Add(ref timeNs[kind * Stages.Length + stage], (long)(value * 1_000_000.0));
    }

    private static string? Tag(ReadOnlySpan<KeyValuePair<string, object?>> tags, string name)
    {
        foreach (KeyValuePair<string, object?> tag in tags)
        {
            if (tag.Key == name)
                return tag.Value as string;
        }

        return null;
    }

    private static int IndexOf(string[] names, string? name)
    {
        for (int i = 0; i < names.Length; i++)
        {
            if (names[i] == name)
                return i;
        }

        return -1;
    }

    private async Task SampleLoopAsync()
    {
        using PeriodicTimer timer = new(interval);
        while (await timer.WaitForNextTickAsync(stop.Token).ConfigureAwait(false))
            WriteSample();
    }

    private void WriteSample()
    {
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        StringBuilder sb = new();

        for (int k = 0; k < Kinds.Length; k++)
        {
            long count = Interlocked.Read(ref ops[k]);
            if (count == 0)
                continue;

            sb.Append(now).Append(",ops,").Append(Kinds[k]).Append(",,").Append(count).Append('\n');

            for (int s = 0; s < Stages.Length; s++)
            {
                double ms = Interlocked.Read(ref timeNs[k * Stages.Length + s]) / 1_000_000.0;
                sb.Append(now).Append(",time_ms,").Append(Kinds[k]).Append(',').Append(Stages[s]).Append(',')
                    .Append(ms.ToString("F3", CultureInfo.InvariantCulture)).Append('\n');
            }
        }

        if (sb.Length > 0)
            File.AppendAllText(path, sb.ToString());
    }
}
