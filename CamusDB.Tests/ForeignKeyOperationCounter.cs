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
/// Counts the <c>camus.foreign_key.operations</c> measurements of the test that started it, by
/// <c>kind</c>. The meter is process-wide, so a measurement is kept only when it comes from the starting
/// test's own async flow: work that other tests run at the same time does not change the counts. Turns
/// <see cref="ServerDiagnostics.Enabled"/> on for its lifetime, because the counter records only while
/// diagnostics are enabled.
/// </summary>
internal sealed class ForeignKeyOperationCounter : IDisposable
{
    private static readonly AsyncLocal<object?> OwningFlow = new();

    private readonly MeterListener listener = new();
    private readonly Dictionary<string, long> counts = new(StringComparer.Ordinal);
    private readonly bool wasEnabled;

    private ForeignKeyOperationCounter()
    {
        wasEnabled = ServerDiagnostics.Enabled;
        ServerDiagnostics.Enabled = true;

        object flow = new();
        OwningFlow.Value = flow;

        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == ServerDiagnostics.MeterName && instrument.Name == "camus.foreign_key.operations")
                l.EnableMeasurementEvents(instrument);
        };

        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            if (!ReferenceEquals(OwningFlow.Value, flow))
                return;

            string? kind = null;
            foreach (KeyValuePair<string, object?> tag in tags)
            {
                if (tag.Key == "kind")
                    kind = tag.Value?.ToString();
            }

            if (kind is null)
                return;

            lock (counts)
                counts[kind] = counts.GetValueOrDefault(kind) + value;
        });

        listener.Start();
    }

    /// <summary>Starts counting. Call it from the test's own flow, and dispose it at the end.</summary>
    public static ForeignKeyOperationCounter Start() => new();

    /// <summary>The count recorded for <paramref name="kind"/>, for example <c>lock_acquired</c>.</summary>
    public long Count(string kind)
    {
        lock (counts)
            return counts.GetValueOrDefault(kind);
    }

    /// <summary>The sum over every kind.</summary>
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
