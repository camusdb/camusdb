
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Text;

using CamusDB.Core.Diagnostics;

namespace CamusDB.Tests.Diagnostics;

/// <summary>
/// Names the Kahuna arm that refused a commit, for a test that failed with the engine's bare
/// "commit was aborted by the coordinator (conflict or deadline)".
///
/// <para>Kahuna's commit API answers only a response type: the reason behind an <c>Aborted</c> (a
/// decision-deadline rejection, an exhausted prepare retry loop, a lost lock or staged intent, a
/// moved staged base) is known to the coordinator alone, which counts it on a process-wide meter and,
/// for some arms, logs it. A CI runner keeps the counters and the log, but nothing tied them to the
/// failed test. This listens to those counters for the whole run, is read at the start and at the
/// end of each test, and the delta names the arm.</para>
///
/// <para>The listener enables only the attributing instruments, not the whole Kahuna meter: the
/// suite starts a node per test, an instrument tagged per node or per partition would add a row per
/// node for the life of the run, and the attributing instruments carry only a small fixed tag set.
/// The counters are still process-wide, so a test that runs in parallel with another fixture can see
/// that fixture's events in its delta. The report is attribution evidence, not an assertion.</para>
/// </summary>
internal static class KahunaAbortAttribution
{
    /// <summary>
    /// The counters and histograms that explain a refused commit. Each is a Kahuna instrument name;
    /// the tags (the outcome, the detection, the cause) stay on the row.
    /// </summary>
    private static readonly string[] Metrics =
    [
        "kahuna.durable_tx.late_commit_rejections",
        "kahuna.durable_tx.late_commit_rejected_transactions",
        "kahuna.durable_tx.late_commit_conclusions",
        "kahuna.durable_tx.prepare_retry_loops",
        "kahuna.durable_tx.helping_settled_nothing",
        "kahuna.durable_tx.decision_deadline_margin_ms",
        "kahuna.transactions.lost_lock_aborts",
        "kahuna.transactions.lost_staged_intent_aborts",
        "kahuna.kv.staged_revision_collisions",
        "kahuna.kv.staged_chain_break_aborts",
        "kahuna.kv.session_owned_intent_ceiling_expiries",
        // Not a refusal of a commit, but the usual cause of one: a proposal the partition's write
        // aggregator turned away (queue full, unflushed persistence backlog, oversized, stopping) is
        // what makes a prepare fail and a batched write answer transient for its whole budget.
        "kahuna.kv.write.rejections",
    ];

    /// <summary>A row's running state: how many measurements it took, and the last value it saw.</summary>
    internal readonly record struct Reading(long Count, double? Last);

    private sealed class Row
    {
        public long Count;
        public double Last;
    }

    private static readonly object Gate = new();

    private static readonly Dictionary<string, Row> Rows = new(StringComparer.Ordinal);

    // Kept for the life of the run: a listener that is collected stops delivering measurements.
    private static readonly MeterListener Listener = Start();

    private static MeterListener Start()
    {
        MeterListener listener = new()
        {
            InstrumentPublished = static (instrument, listener) =>
            {
                if (string.Equals(instrument.Meter.Name, EngineMetricsCollector.KahunaMeterName, StringComparison.Ordinal)
                    && IsAttributing(instrument.Name))
                    listener.EnableMeasurementEvents(instrument);
            },
        };

        listener.SetMeasurementEventCallback<long>(static (i, m, t, _) => Record(i, m, t));
        listener.SetMeasurementEventCallback<int>(static (i, m, t, _) => Record(i, m, t));
        listener.SetMeasurementEventCallback<double>(static (i, m, t, _) => Record(i, m, t));
        listener.Start();

        return listener;
    }

    private static void Record(Instrument instrument, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        string key = RowKey(instrument.Name, tags);

        lock (Gate)
        {
            if (!Rows.TryGetValue(key, out Row? row))
                Rows[key] = row = new Row();

            row.Count++;
            row.Last = value;
        }
    }

    /// <summary>
    /// Reads the current state of every attributing row, keyed by instrument name plus its tags.
    /// </summary>
    internal static Dictionary<string, Reading> Capture()
    {
        lock (Gate)
        {
            Dictionary<string, Reading> readings = new(Rows.Count, StringComparer.Ordinal);

            foreach ((string key, Row row) in Rows)
                readings[key] = new Reading(row.Count, row.Last);

            return readings;
        }
    }

    /// <summary>
    /// Describes every row that moved between <paramref name="before"/> and <paramref name="after"/>,
    /// one line per row, or null when nothing moved. A row reports how many measurements it took, and
    /// a histogram also its last sample. A row is named by its instrument and, in braces, its tags,
    /// so the outcome, the detection or the cause stays readable.
    /// </summary>
    internal static string? Describe(Dictionary<string, Reading> before, Dictionary<string, Reading> after)
    {
        StringBuilder? report = null;

        foreach ((string key, Reading now) in after)
        {
            long startCount = before.TryGetValue(key, out Reading start) ? start.Count : 0;
            long grew = now.Count - startCount;

            if (grew <= 0)
                continue;

            report ??= new StringBuilder("Kahuna commit refusal counters that moved during this test:");
            report.Append("\n  ").Append(key).Append(": +").Append(grew);

            if (now.Last is { } last && key.Contains("_ms", StringComparison.Ordinal))
                report.Append(" (last ").Append(last.ToString(CultureInfo.InvariantCulture)).Append(')');
        }

        return report?.ToString();
    }

    /// <summary>
    /// Whether an instrument explains a refused commit: one of the listed names, or any Kahuna
    /// instrument whose name says <c>abort</c>, so a refusal counted by a newer Kahuna still shows.
    /// </summary>
    private static bool IsAttributing(string metric)
    {
        foreach (string name in Metrics)
        {
            if (string.Equals(name, metric, StringComparison.Ordinal))
                return true;
        }

        return metric.Contains("abort", StringComparison.Ordinal);
    }

    /// <summary>The instrument name with its tags in braces, sorted by key so call-site order does not split a row.</summary>
    private static string RowKey(string metric, ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        if (tags.Length == 0)
            return metric;

        KeyValuePair<string, object?>[] sorted = tags.ToArray();
        Array.Sort(sorted, static (a, b) => string.CompareOrdinal(a.Key, b.Key));

        StringBuilder key = new(metric);
        key.Append('{');

        for (int i = 0; i < sorted.Length; i++)
        {
            if (i > 0)
                key.Append(',');

            key.Append(sorted[i].Key).Append('=').Append(Convert.ToString(sorted[i].Value, CultureInfo.InvariantCulture));
        }

        return key.Append('}').ToString();
    }
}
