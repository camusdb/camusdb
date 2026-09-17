/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// The write-time rules <see cref="CompiledRowCodec"/> applies to each variable cell of one row: the
/// per-column <see cref="ColumnStorageStrategy"/>, the out-of-line threshold, and the compression
/// settings. It is built once per statement from the current <see cref="CamusDBOptions"/> snapshot
/// and the column list the row is encoded under, so a published configuration change governs the
/// next statement and never a statement already running.
///
/// <para>These rules decide only the physical form of newly written cells. A reader follows the
/// marks each stored cell carries, so no change here can alter what an existing row reads back.</para>
/// </summary>
public sealed class LargeValuePolicy
{
    private readonly ColumnStorageStrategy[] strategies;
    private readonly int[] minCandidateBytes;

    /// <summary>Stored size at or above which a value may move out of line. &lt;= 0 keeps every value inline.</summary>
    public int ThresholdBytes { get; }

    /// <summary>Whether the writer may compress a value. Reads never consult it.</summary>
    public bool CompressionEnabled { get; }

    /// <summary>Minimum saving, in percent, for a compressed form to be kept.</summary>
    public int MinSavingPercent { get; }

    /// <summary>A policy that stores every cell raw. Used where no configuration applies.</summary>
    public static LargeValuePolicy Inline { get; } = new([], 0, false, 0);

    private LargeValuePolicy(IReadOnlyList<TableColumnSchema> columns, int thresholdBytes, bool compressionEnabled, int minSavingPercent)
    {
        ThresholdBytes = thresholdBytes;
        CompressionEnabled = compressionEnabled;
        MinSavingPercent = minSavingPercent;

        strategies = new ColumnStorageStrategy[columns.Count];
        minCandidateBytes = new int[columns.Count];

        for (int i = 0; i < columns.Count; i++)
        {
            ColumnStorageStrategy strategy = columns[i].EffectiveStorage;
            strategies[i] = strategy;

            int candidate = 0;
            if (compressionEnabled && strategy is ColumnStorageStrategy.Extended or ColumnStorageStrategy.Main)
                candidate = LargeValueCompression.MinCompressibleBytes;

            if (thresholdBytes > 0 && strategy is ColumnStorageStrategy.Extended or ColumnStorageStrategy.External)
                candidate = candidate == 0 ? thresholdBytes : Math.Min(candidate, thresholdBytes);

            minCandidateBytes[i] = candidate;
        }
    }

    /// <summary>
    /// Builds the policy for rows encoded under <paramref name="columns"/> — the column list of the
    /// schema version the writer encodes with, in stored ordinal order.
    /// </summary>
    public static LargeValuePolicy For(IReadOnlyList<TableColumnSchema> columns, CamusDBOptions options) =>
        new(columns, options.LargeValueThresholdBytes, options.LargeValueCompressionEnabled, options.LargeValueCompressionMinSavingPercent);

    /// <summary>Test seam: a policy with explicit settings.</summary>
    internal static LargeValuePolicy Create(IReadOnlyList<TableColumnSchema> columns, int thresholdBytes, bool compressionEnabled, int minSavingPercent) =>
        new(columns, thresholdBytes, compressionEnabled, minSavingPercent);

    internal ColumnStorageStrategy StrategyOf(int storedOrdinal) =>
        storedOrdinal < strategies.Length ? strategies[storedOrdinal] : ColumnStorageStrategy.Plain;

    /// <summary>
    /// Smallest value size, in bytes, that this policy could store in any form other than raw for the
    /// column at <paramref name="storedOrdinal"/>; 0 when the column always stays raw.
    /// </summary>
    internal int MinCandidateBytes(int storedOrdinal) =>
        storedOrdinal < minCandidateBytes.Length ? minCandidateBytes[storedOrdinal] : 0;
}

/// <summary>One out-of-line value to write: the cell's variable ordinal and the enveloped stored bytes.</summary>
public readonly record struct LargeValueWrite(int VariableOrdinal, byte[] StorageValue);

/// <summary>
/// The result of encoding one row under a <see cref="LargeValuePolicy"/>: the row's Kahuna storage
/// value, and the out-of-line values that must be written in the same transaction, or null when the
/// row has none.
/// </summary>
public readonly record struct EncodedRow(byte[] StorageValue, IReadOnlyList<LargeValueWrite>? OutOfLine);
