/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Linq;
using CamusDB.Core.Diagnostics;
using NUnit.Framework;

namespace CamusDB.Tests.Diagnostics;

/// <summary>
/// Verifies the server diagnostics contract without a collector: a <see cref="MeterListener"/> attaches
/// directly to the CamusDB.Server meter, representative operations are recorded, and the captured
/// measurements' counts, values, and (bounded) tags are asserted. Non-parallelizable because
/// <see cref="ServerDiagnostics.Enabled"/> is a process-wide gate.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class ServerDiagnosticsTests
{
    private sealed record Measurement(string Instrument, double Value, Dictionary<string, string?> Tags);

    private static (MeterListener Listener, List<Measurement> Captured) StartListener()
    {
        List<Measurement> captured = new();
        MeterListener listener = new();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == ServerDiagnostics.MeterName)
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
        foreach (var kv in tags)
            d[kv.Key] = kv.Value?.ToString();
        return d;
    }

    [TearDown]
    public void ResetGate() => ServerDiagnostics.Enabled = false;

    [Test]
    public void EnabledRecordsRequestWithBoundedTags()
    {
        (MeterListener listener, List<Measurement> captured) = StartListener();
        using (listener)
        {
            ServerDiagnostics.Enabled = true;
            ServerDiagnostics.RecordRequest(
                ServerDiagnostics.Tags.Operation.Query,
                ServerDiagnostics.Tags.Transport.GrpcBatch,
                ServerDiagnostics.Tags.Outcome.Ok, 12.5);
            listener.RecordObservableInstruments();
        }

        var count = captured.FirstOrDefault(m => m.Instrument == "camus.request.count");
        var duration = captured.FirstOrDefault(m => m.Instrument == "camus.request.duration");
        Assert.That(count, Is.Not.Null, "request count must be recorded");
        Assert.That(duration, Is.Not.Null, "request duration must be recorded");
        Assert.That(count!.Tags["operation"], Is.EqualTo("query"));
        Assert.That(count.Tags["transport"], Is.EqualTo("grpc_batch"));
        Assert.That(count.Tags["outcome"], Is.EqualTo("ok"));
        Assert.That(duration!.Value, Is.EqualTo(12.5).Within(0.001));
    }

    [Test]
    public void DisabledEmitsNothing()
    {
        (MeterListener listener, List<Measurement> captured) = StartListener();
        using (listener)
        {
            ServerDiagnostics.Enabled = false;
            ServerDiagnostics.RecordRequest("query", "grpc_batch", "ok", 1);
            ServerDiagnostics.RecordExecute("non_query", "update", 1);
            ServerDiagnostics.RecordCommitDuration("ok", 1);
            ServerDiagnostics.AddRequestInFlight("grpc_batch", 1);
            using (ServerDiagnostics.MeasureExecute("non_query", "insert")) { }
            listener.RecordObservableInstruments();
        }

        Assert.That(captured, Is.Empty, "no measurements may be emitted when diagnostics are disabled");
    }

    [Test]
    public void MeasureExecuteRecordsOnDispose()
    {
        (MeterListener listener, List<Measurement> captured) = StartListener();
        using (listener)
        {
            ServerDiagnostics.Enabled = true;
            using (ServerDiagnostics.MeasureExecute(
                ServerDiagnostics.Tags.Operation.NonQuery, ServerDiagnostics.Tags.Statement.Update))
            {
                // simulate work
            }
        }

        var execute = captured.FirstOrDefault(m => m.Instrument == "camus.execute.duration");
        Assert.That(execute, Is.Not.Null);
        Assert.That(execute!.Tags["operation"], Is.EqualTo("non_query"));
        Assert.That(execute.Tags["statement"], Is.EqualTo("update"));
    }

    [Test]
    public void CommitAndTransactionMetricsCarryExpectedTags()
    {
        (MeterListener listener, List<Measurement> captured) = StartListener();
        using (listener)
        {
            ServerDiagnostics.Enabled = true;
            ServerDiagnostics.RecordCommitDuration(ServerDiagnostics.Tags.Outcome.Ok, 25.0);
            ServerDiagnostics.RecordTransaction(ServerDiagnostics.Tags.Operation.Commit, ServerDiagnostics.Tags.Outcome.Ok);
            ServerDiagnostics.AddActiveTransaction(ServerDiagnostics.Tags.TransactionMode.ReadWrite, 1);
            listener.RecordObservableInstruments();
        }

        Assert.That(captured.Any(m => m.Instrument == "camus.transaction.commit.duration" && m.Value == 25.0));
        Assert.That(captured.Any(m => m.Instrument == "camus.transaction.count" && m.Tags["operation"] == "commit"));
        Assert.That(captured.Any(m => m.Instrument == "camus.transaction.active" && m.Tags["transaction_mode"] == "read_write"));
    }

    [Test]
    public void TraceSpansFormAParentChildTree()
    {
        // Attach an ActivityListener directly (the spec's prescribed trace test): a root request span
        // with child execute/commit spans should nest via Activity.Current.
        List<Activity> started = new();
        using ActivityListener al = new()
        {
            ShouldListenTo = src => src.Name == ServerDiagnostics.MeterName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStarted = a => started.Add(a),
        };
        ActivitySource.AddActivityListener(al);

        using (Activity? request = ServerDiagnostics.StartSpan(ServerDiagnostics.Spans.Request))
        {
            using (ServerDiagnostics.StartSpan(ServerDiagnostics.Spans.Execute)) { }
            using (ServerDiagnostics.StartSpan(ServerDiagnostics.Spans.Commit)) { }
        }

        Activity? root = started.FirstOrDefault(a => a.OperationName == ServerDiagnostics.Spans.Request);
        Activity? execute = started.FirstOrDefault(a => a.OperationName == ServerDiagnostics.Spans.Execute);
        Assert.That(root, Is.Not.Null, "root request span must be created when a listener is active");
        Assert.That(execute, Is.Not.Null, "execute span must be created");
        Assert.That(execute!.Parent, Is.EqualTo(root), "child spans must nest under the request span");
    }

    [Test]
    public void StartSpanReturnsNullWithoutListener()
    {
        // No listener attached in this test → no activity should be created (unsampled = zero allocation).
        using Activity? span = ServerDiagnostics.StartSpan(ServerDiagnostics.Spans.Request);
        Assert.That(span, Is.Null);
    }

    [Test]
    public void BoundedTagVocabularyHasNoUnexpectedValues()
    {
        // The allowed values are a small, reviewed set — this fails if someone widens a vocabulary
        // without updating the test (a proxy guard against unbounded-cardinality tags).
        Assert.That(ServerDiagnostics.Tags.Operation.All,
            Is.EquivalentTo(new[] { "query", "non_query", "ddl", "begin", "commit", "rollback", "prepare", "close" }));
        Assert.That(ServerDiagnostics.Tags.Statement.All,
            Is.EquivalentTo(new[] { "select", "insert", "update", "delete", "other" }));
        Assert.That(ServerDiagnostics.Tags.Outcome.All,
            Is.EquivalentTo(new[] { "ok", "domain_error", "conflict", "canceled", "internal_error" }));
        Assert.That(ServerDiagnostics.Tags.Transport.All,
            Is.EquivalentTo(new[] { "grpc_unary", "grpc_batch", "http" }));
        Assert.That(ServerDiagnostics.Tags.TransactionMode.All,
            Is.EquivalentTo(new[] { "read_only", "read_write" }));
        Assert.That(ServerDiagnostics.Tags.Scan.All,
            Is.EquivalentTo(new[] { "point", "primary_range", "index_range", "full" }));
        Assert.That(ServerDiagnostics.Tags.Stage.All,
            Is.EquivalentTo(new[] { "scanned", "returned" }));
    }

    [Test]
    public void ParseAndScanMetricsRecordWhenEnabled()
    {
        (MeterListener listener, List<Measurement> captured) = StartListener();
        using (listener)
        {
            ServerDiagnostics.Enabled = true;
            ServerDiagnostics.RecordParse(cacheHit: false, milliseconds: 3.0);
            ServerDiagnostics.RecordParse(cacheHit: true, milliseconds: 0);
            ServerDiagnostics.RecordQueryRows(ServerDiagnostics.Tags.Scan.Full, ServerDiagnostics.Tags.Stage.Scanned, 100);
            ServerDiagnostics.RecordScanDuration(ServerDiagnostics.Tags.Scan.Full, 7.5);
            ServerDiagnostics.RecordStagedMutations(4);
            listener.RecordObservableInstruments();
        }

        Assert.That(captured.Count(m => m.Instrument == "camus.sql.parse.cache"), Is.EqualTo(2));
        Assert.That(captured.Any(m => m.Instrument == "camus.sql.parse.cache" && m.Tags["result"] == "hit"));
        Assert.That(captured.Any(m => m.Instrument == "camus.sql.parse.cache" && m.Tags["result"] == "miss"));
        // Parse duration recorded only on the miss.
        Assert.That(captured.Count(m => m.Instrument == "camus.sql.parse.duration"), Is.EqualTo(1));
        Assert.That(captured.Any(m => m.Instrument == "camus.query.rows" && m.Tags["scan"] == "full" && m.Value == 100));
        Assert.That(captured.Any(m => m.Instrument == "camus.query.scan.duration" && m.Value == 7.5));
        Assert.That(captured.Any(m => m.Instrument == "camus.transaction.staged_mutations" && m.Value == 4));
    }
}
