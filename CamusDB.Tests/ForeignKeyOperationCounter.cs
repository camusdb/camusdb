/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;

using CamusDB.Core.Diagnostics;

namespace CamusDB.Tests;

/// <summary>
/// Counts the measurements of one <see cref="ServerDiagnostics"/> counter that the test which started it
/// records, summed by the value of one tag. The meter is process-wide, so a measurement is kept only
/// when it comes from the starting test's own async flow: work that other tests run at the same time
/// does not change the counts. Each instance marks the flow with its own <see cref="AsyncLocal{T}"/>, so
/// two counters can run in the same test. Turns <see cref="ServerDiagnostics.Enabled"/> on for its
/// lifetime, because the counters record only while diagnostics are enabled; dispose nested counters in
/// reverse order so the flag is restored correctly.
/// </summary>
internal abstract class FlowTaggedCounter : IDisposable
{
    private readonly AsyncLocal<bool> owningFlow = new();
    private readonly MeterListener listener = new();
    private readonly Dictionary<string, long> counts = new(StringComparer.Ordinal);
    private readonly bool wasEnabled;

    protected FlowTaggedCounter(string instrumentName, string tagKey)
    {
        wasEnabled = ServerDiagnostics.Enabled;
        ServerDiagnostics.Enabled = true;

        // Set in a synchronous call, so the value stays in the caller's execution context and flows
        // into every task the test starts after this point.
        owningFlow.Value = true;

        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == ServerDiagnostics.MeterName && instrument.Name == instrumentName)
                l.EnableMeasurementEvents(instrument);
        };

        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            if (!owningFlow.Value)
                return;

            string? tag = null;
            foreach (KeyValuePair<string, object?> pair in tags)
            {
                if (pair.Key == tagKey)
                    tag = pair.Value?.ToString();
            }

            if (tag is null)
                return;

            lock (counts)
                counts[tag] = counts.GetValueOrDefault(tag) + value;
        });

        listener.Start();
    }

    /// <summary>The count recorded for one tag value.</summary>
    public long Count(string tag)
    {
        lock (counts)
            return counts.GetValueOrDefault(tag);
    }

    /// <summary>The sum over every tag value.</summary>
    public long Total()
    {
        long total = 0;

        lock (counts)
        {
            foreach (long value in counts.Values)
                total += value;
        }

        return total;
    }

    public void Dispose()
    {
        listener.Dispose();
        ServerDiagnostics.Enabled = wasEnabled;
    }
}

/// <summary>
/// Counts the <c>camus.foreign_key.operations</c> measurements of the test that started it, by
/// <c>kind</c>; see <see cref="FlowTaggedCounter"/>.
/// </summary>
internal sealed class ForeignKeyOperationCounter : FlowTaggedCounter
{
    private ForeignKeyOperationCounter() : base("camus.foreign_key.operations", "kind")
    {
    }

    /// <summary>Starts counting. Call it from the test's own flow, and dispose it at the end.</summary>
    public static ForeignKeyOperationCounter Start() => new();
}

/// <summary>
/// Counts the <c>camus.kv.scan_entries</c> measurements of the test that started it, by <c>family</c>
/// (<c>row</c> or <c>index</c>): the entries the test's range scans read from storage. See
/// <see cref="FlowTaggedCounter"/>.
/// </summary>
internal sealed class KvScanEntryCounter : FlowTaggedCounter
{
    private KvScanEntryCounter() : base("camus.kv.scan_entries", "family")
    {
    }

    /// <summary>Starts counting. Call it from the test's own flow, and dispose it at the end.</summary>
    public static KvScanEntryCounter Start() => new();
}
