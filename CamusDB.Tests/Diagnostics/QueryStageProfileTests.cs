/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Threading.Tasks;
using CamusDB.Core.Diagnostics;
using NUnit.Framework;

namespace CamusDB.Tests.Diagnostics;

/// <summary>
/// The per-stage query profile: nested stages, Kahuna time charged to the
/// innermost open stage by call kind, the clock flowing across awaits, and the off switch.
/// Non-parallelizable because <see cref="QueryStageProfile.Enabled"/> is process-wide.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class QueryStageProfileTests
{
    private sealed record Measurement(string Instrument, double Value, Dictionary<string, string?> Tags);

    private static (MeterListener Listener, List<Measurement> Captured) StartListener()
    {
        List<Measurement> captured = new();
        MeterListener listener = new();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == QueryStageProfile.MeterName)
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((inst, value, tags, _) =>
        {
            lock (captured) captured.Add(new Measurement(inst.Name, value, TagsToDict(tags)));
        });
        listener.SetMeasurementEventCallback<double>((inst, value, tags, _) =>
        {
            lock (captured) captured.Add(new Measurement(inst.Name, value, TagsToDict(tags)));
        });
        listener.Start();
        return (listener, captured);
    }

    private static Dictionary<string, string?> TagsToDict(ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        Dictionary<string, string?> d = new();
        foreach (KeyValuePair<string, object?> kv in tags)
            d[kv.Key] = kv.Value?.ToString();
        return d;
    }

    private static Measurement? Find(List<Measurement> captured, string instrument, string stage, string part)
        => captured.FirstOrDefault(m => m.Instrument == instrument && m.Tags["stage"] == stage && m.Tags["part"] == part);

    [TearDown]
    public void ResetGate() => QueryStageProfile.Enabled = false;

    /// <summary>Stands in for the op handler: the clock is opened inside the op's own async method.</summary>
    private static async Task RunProfiledOpAsync(Func<Task> body, string? path)
    {
        QueryStageClock? clock = QueryStageProfile.Begin();
        Assert.That(clock, Is.Not.Null);
        clock!.Path = path;
        await body();
        clock.Publish();
    }

    [Test]
    public async Task NestedStagesAndKahunaCallsAreChargedToTheInnermostStage()
    {
        (MeterListener listener, List<Measurement> captured) = StartListener();
        using (listener)
        {
            QueryStageProfile.Enabled = true;
            await RunProfiledOpAsync(async () =>
            {
                using (QueryStageProfile.Measure(QueryStage.Fetch))
                {
                    using (QueryStageProfile.Measure(QueryStage.IndexLookup))
                    {
                        using (QueryStageProfile.MeasureKahuna(KahunaCallKind.Start))
                            await Task.Delay(2);
                        using (QueryStageProfile.MeasureKahuna(KahunaCallKind.KeyValue))
                            await Task.Delay(2);
                    }

                    using (QueryStageProfile.Measure(QueryStage.RowGet))
                    using (QueryStageProfile.MeasureKahuna(KahunaCallKind.KeyValue))
                        await Task.Delay(2);

                    // Back in the outer stage after the nested ones closed.
                    using (QueryStageProfile.MeasureKahuna(KahunaCallKind.KeyValue))
                        await Task.Yield();
                }
            }, QueryStageProfile.Paths.Txn);
        }

        Measurement? queries = captured.SingleOrDefault(m => m.Instrument == "camus.query.profile.queries");
        Assert.That(queries, Is.Not.Null);
        Assert.That(queries!.Tags["path"], Is.EqualTo("txn"));

        Assert.That(Find(captured, "camus.query.profile.calls", "fetch", "total")!.Value, Is.EqualTo(1));
        Assert.That(Find(captured, "camus.query.profile.calls", "index_lookup", "kahuna_start")!.Value, Is.EqualTo(1));
        Assert.That(Find(captured, "camus.query.profile.calls", "index_lookup", "kahuna_kv")!.Value, Is.EqualTo(1));
        Assert.That(Find(captured, "camus.query.profile.calls", "row_get", "kahuna_kv")!.Value, Is.EqualTo(1));
        Assert.That(Find(captured, "camus.query.profile.calls", "fetch", "kahuna_kv")!.Value, Is.EqualTo(1),
            "a Kahuna call after the nested stages closed belongs to the outer stage again");

        double fetch = Find(captured, "camus.query.profile.time", "fetch", "total")!.Value;
        double index = Find(captured, "camus.query.profile.time", "index_lookup", "total")!.Value;
        double row = Find(captured, "camus.query.profile.time", "row_get", "total")!.Value;
        double indexKahuna = Find(captured, "camus.query.profile.time", "index_lookup", "kahuna_kv")!.Value
            + Find(captured, "camus.query.profile.time", "index_lookup", "kahuna_start")!.Value;
        Assert.That(fetch, Is.GreaterThanOrEqualTo(index + row));
        Assert.That(index, Is.GreaterThanOrEqualTo(indexKahuna));
        Assert.That(indexKahuna, Is.GreaterThan(0));

        Assert.That(captured.Select(m => m.Tags.GetValueOrDefault("stage")).Where(s => s is not null).Distinct(),
            Is.SubsetOf(QueryStageProfile.AllStageNames));
        Assert.That(captured.Select(m => m.Tags.GetValueOrDefault("part")).Where(p => p is not null).Distinct(),
            Is.SubsetOf(QueryStageProfile.Parts.All));
    }

    [Test]
    public async Task AnOpWithoutAPathPublishesNothing()
    {
        (MeterListener listener, List<Measurement> captured) = StartListener();
        using (listener)
        {
            QueryStageProfile.Enabled = true;
            await RunProfiledOpAsync(async () =>
            {
                using (QueryStageProfile.Measure(QueryStage.Resolve))
                    await Task.Yield();
            }, path: null);
        }

        Assert.That(captured, Is.Empty);
    }

    [Test]
    public async Task DisabledOpensNoClockAndRecordsNothing()
    {
        (MeterListener listener, List<Measurement> captured) = StartListener();
        using (listener)
        {
            QueryStageProfile.Enabled = false;
            Assert.That(QueryStageProfile.Begin(), Is.Null);
            using (QueryStageProfile.Measure(QueryStage.Fetch))
            using (QueryStageProfile.MeasureKahuna(KahunaCallKind.KeyValue))
                await Task.Yield();
            Assert.That(QueryStageProfile.Current, Is.Null);
        }

        Assert.That(captured, Is.Empty);
    }

    [Test]
    public async Task WorkThatOutlivesItsOpDoesNotTouchThePublishedClock()
    {
        QueryStageProfile.Enabled = true;
        QueryStageClock? clock = null;
        Task lingering = Task.CompletedTask;
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await RunProfiledOpAsync(() =>
        {
            clock = QueryStageProfile.Current;
            // Inherits the op's async flow and runs after the op published.
            lingering = Task.Run(async () =>
            {
                await release.Task;
                using (QueryStageProfile.Measure(QueryStage.Decode)) { }
            });
            return Task.CompletedTask;
        }, QueryStageProfile.Paths.Autocommit);

        release.SetResult();
        await lingering;

        Assert.That(clock!.Calls[(int)QueryStage.Decode], Is.Zero, "a stage timed after the op published is dropped");

        (MeterListener listener, List<Measurement> captured) = StartListener();
        using (listener)
            clock.Publish();

        Assert.That(captured, Is.Empty, "a clock publishes once");
    }
}
