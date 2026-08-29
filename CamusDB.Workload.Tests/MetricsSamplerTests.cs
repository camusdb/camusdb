/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using CamusDB.Workload.Reporting;
using NUnit.Framework;

namespace CamusDB.Workload.Tests;

/// <summary>
/// Drives the collector against a real HTTP endpoint, because the behaviour that matters is in the
/// loop rather than in the parsing: that it samples repeatedly, that every node in a round shares one
/// instant, and above all that a node which stops answering leaves an explicit zero in the series
/// instead of a gap. A gap looks exactly like an idle node, and that mistake would hide an outage.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class MetricsSamplerTests
{
    private const string MetricsBody = """
        # HELP camus_request_count_total Server requests handled.
        # TYPE camus_request_count_total counter
        camus_request_count_total{operation="query",outcome="ok"} 120
        camus_request_count_total{operation="commit",outcome="ok"} 30
        kahuna_kv_write_entries_total 900
        kahuna_kv_write_batches_total 30
        camus_request_duration_milliseconds_bucket{le="5"} 100
        camus_request_duration_milliseconds_sum 640
        camus_request_duration_milliseconds_count 150
        target_info{service_name="camusdb"} 1
        """;

    private static int FreePort()
    {
        TcpListener probe = new(IPAddress.Loopback, 0);
        probe.Start();
        int port = ((IPEndPoint)probe.LocalEndpoint).Port;
        probe.Stop();
        return port;
    }

    [Test]
    public async Task Collects_RepeatedSamplesFromALiveNode()
    {
        int port = FreePort();
        HttpListener listener = new();
        listener.Prefixes.Add($"http://localhost:{port}/");
        try
        {
            listener.Start();
        }
        catch (Exception ex)
        {
            Assert.Ignore($"Cannot bind a local HTTP listener here: {ex.Message}");
            return;
        }

        using CancellationTokenSource serving = new();
        Task server = Task.Run(async () =>
        {
            while (!serving.IsCancellationRequested)
            {
                HttpListenerContext context;
                try
                {
                    context = await listener.GetContextAsync().ConfigureAwait(false);
                }
                catch (Exception)
                {
                    return;
                }

                byte[] payload = Encoding.UTF8.GetBytes(MetricsBody);
                context.Response.ContentType = "text/plain";
                context.Response.ContentLength64 = payload.Length;
                await context.Response.OutputStream.WriteAsync(payload).ConfigureAwait(false);
                context.Response.Close();
            }
        });

        string dir = Path.Combine(Path.GetTempPath(), "camus-sampler-" + Guid.NewGuid().ToString("N"));
        string csv = Path.Combine(dir, "node-metrics.csv");
        try
        {
            NodeTarget target = new("camus1", new Uri($"http://localhost:{port}/metrics"));
            await using MetricsSampler sampler = new(new[] { target }, TimeSpan.FromSeconds(1), csv);
            sampler.Start();

            await WaitForRoundsAsync(csv, rounds: 2, budget: TimeSpan.FromSeconds(20)).ConfigureAwait(false);
            MetricsSamplerResult? result = await sampler.StopAsync().ConfigureAwait(false);

            Assert.That(result, Is.Not.Null);
            Assert.That(result!.Nodes.Single().Succeeded, Is.GreaterThanOrEqualTo(2));
            Assert.That(result.Nodes.Single().Failed, Is.Zero);

            NodeMetricsSeries series = NodeMetricsSeries.Load(csv);
            Assert.That(series.Nodes, Is.EqualTo(new[] { "camus1" }));

            // A constant counter yields a zero delta; the point is that the series is queryable at all.
            Assert.That(series.Delta("camus_request_count", MetricsWindow.All, "camus1"), Is.EqualTo(0));
            Assert.That(series.Gauge("kahuna_kv_write_entries", MetricsWindow.All, GaugeAggregate.Last, "camus1"), Is.EqualTo(900));

            // Buckets are dropped to keep the file small; the histogram's sum and count are kept, which
            // is what a mean needs.
            Assert.That(series.Resolve("camus_request_duration_milliseconds_bucket"), Is.Null);
            Assert.That(series.Resolve("camus_request_duration_milliseconds_sum"), Is.Not.Null);
            Assert.That(series.Resolve("target_info"), Is.Null);

            Assert.That(sampler.LastScrapes["camus1"], Does.Contain("kahuna_kv_write_entries_total"));
        }
        finally
        {
            serving.Cancel();
            listener.Stop();
            listener.Close();
            await Task.WhenAny(server, Task.Delay(2000)).ConfigureAwait(false);
            if (Directory.Exists(dir))
                Directory.Delete(dir, recursive: true);
        }
    }

    [Test]
    public async Task RecordsAnUnreachableNodeAsAnExplicitZero()
    {
        // Nothing is listening on this port. The run must still finish, and the series must say the
        // node was not answering rather than simply holding no samples for it.
        int port = FreePort();
        string dir = Path.Combine(Path.GetTempPath(), "camus-sampler-" + Guid.NewGuid().ToString("N"));
        string csv = Path.Combine(dir, "node-metrics.csv");
        try
        {
            NodeTarget target = new("down1", new Uri($"http://localhost:{port}/metrics"));
            await using MetricsSampler sampler = new(new[] { target }, TimeSpan.FromSeconds(1), csv);
            sampler.Start();

            await WaitForRoundsAsync(csv, rounds: 1, budget: TimeSpan.FromSeconds(20)).ConfigureAwait(false);
            MetricsSamplerResult? result = await sampler.StopAsync().ConfigureAwait(false);

            Assert.That(result!.Nodes.Single().Failed, Is.GreaterThanOrEqualTo(1));
            Assert.That(result.Nodes.Single().LastError, Is.Not.Null);

            NodeMetricsSeries series = NodeMetricsSeries.Load(csv);
            Assert.That(series.Gauge("workload_scrape_ok", MetricsWindow.All, GaugeAggregate.Last, "down1"), Is.EqualTo(0));
        }
        finally
        {
            if (Directory.Exists(dir))
                Directory.Delete(dir, recursive: true);
        }
    }

    /// <summary>Waits until the CSV holds at least <paramref name="rounds"/> distinct sample instants.</summary>
    private static async Task WaitForRoundsAsync(string csv, int rounds, TimeSpan budget)
    {
        long started = Stopwatch.GetTimestamp();
        while (Stopwatch.GetElapsedTime(started) < budget)
        {
            if (File.Exists(csv))
            {
                // The sampler is still appending, so read with sharing rather than File.ReadAllText.
                using FileStream stream = new(csv, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                using StreamReader reader = new(stream);
                string text = await reader.ReadToEndAsync().ConfigureAwait(false);
                int instants = MetricsCsv.Parse(text).Select(p => p.UnixMs).Distinct().Count();
                if (instants >= rounds)
                    return;
            }
            await Task.Delay(100).ConfigureAwait(false);
        }

        Assert.Fail($"The sampler did not produce {rounds} round(s) within {budget}.");
    }
}
