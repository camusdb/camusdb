/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using System.Diagnostics.Metrics;
using CamusDB.Workload.Metrics;
using NUnit.Framework;

namespace CamusDB.Workload.Tests;

/// <summary>
/// The sampler of the driver's round-trip profile, driven by a stand-in meter with the driver's meter
/// and instrument names: starting it enables the instruments, it writes totals since its start, it
/// ignores a kind or stage it does not know, and a driver without the meter leaves only the header.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class ClientRoundTripSamplerTests
{
    private string directory = "";

    [SetUp]
    public void CreateDirectory()
    {
        directory = Path.Combine(Path.GetTempPath(), "camus-roundtrip-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
    }

    [TearDown]
    public void DeleteDirectory() => Directory.Delete(directory, recursive: true);

    private string[] Rows() => File.ReadAllLines(Path.Combine(directory, ClientRoundTripSampler.FileName));

    private static TagList Tags(string kind, string? stage = null)
    {
        TagList tags = new() { { "camusdb.round_trip.kind", kind } };
        if (stage is not null)
            tags.Add("camusdb.round_trip.stage", stage);
        return tags;
    }

    [Test]
    public async Task WritesTotalsSinceItsStart()
    {
        using Meter meter = new(ClientRoundTripSampler.MeterName);
        Counter<long> ops = meter.CreateCounter<long>("camusdb.client.round_trip.ops");
        Counter<double> time = meter.CreateCounter<double>("camusdb.client.round_trip.time");

        Assert.That(time.Enabled, Is.False, "nothing collects the profile before the sampler starts");

        ClientRoundTripSampler sampler = new(directory, TimeSpan.FromHours(1));
        sampler.Start();

        Assert.That(time.Enabled, Is.True, "starting the sampler turns the profile on");

        for (int i = 0; i < 3; i++)
        {
            ops.Add(1, Tags("query"));
            time.Add(0.25, Tags("query", "queue"));
            time.Add(1.5, Tags("query", "exchange"));
        }

        ops.Add(1, Tags("commit"));
        time.Add(2.0, Tags("commit", "exchange"));

        // Names the sampler does not know are dropped, not written under another name.
        ops.Add(1, Tags("unknown_kind"));
        time.Add(9.0, Tags("query", "unknown_stage"));

        await sampler.StopAsync();

        string[] rows = Rows();
        Assert.That(rows[0], Is.EqualTo("unix_ms,metric,kind,stage,value"));

        string[] data = rows.Skip(1).Select(r => r[(r.IndexOf(',') + 1)..]).ToArray();
        Assert.That(data, Does.Contain("ops,query,,3"));
        Assert.That(data, Does.Contain("time_ms,query,queue,0.750"));
        Assert.That(data, Does.Contain("time_ms,query,exchange,4.500"));
        Assert.That(data, Does.Contain("time_ms,query,resume,0.000"));
        Assert.That(data, Does.Contain("ops,commit,,1"));
        Assert.That(data, Does.Contain("time_ms,commit,exchange,2.000"));
        Assert.That(data.Any(r => r.Contains("unknown")), Is.False);
        Assert.That(data.Any(r => r.StartsWith("ops,non_query", StringComparison.Ordinal)), Is.False, "a kind with no operation has no rows");

        Assert.That(time.Enabled, Is.False, "stopping the sampler turns the profile off");
    }

    [Test]
    public async Task ADriverWithoutTheProfileLeavesOnlyTheHeader()
    {
        ClientRoundTripSampler sampler = new(directory, TimeSpan.FromHours(1));
        sampler.Start();
        await sampler.StopAsync();

        Assert.That(Rows(), Is.EqualTo(new[] { "unix_ms,metric,kind,stage,value" }));
    }
}
