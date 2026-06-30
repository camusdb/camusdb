/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.IO;
using System.Runtime.CompilerServices;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;

/// <summary>
/// A re-enumerable, spill-capable buffer of <see cref="ColumnValue"/> values used by
/// <c>InSubqueryExecutor</c> to accumulate an uncorrelated IN/NOT IN subquery's value set
/// without pinning it in a <c>List&lt;ColumnValue&gt;</c>.
///
/// <para>
/// <b>Memory phase:</b> values are accumulated in a <see cref="List{T}"/> until the buffer
/// reaches <see cref="CamusDBConfig.SpillEffectiveThreshold"/>. When the threshold is hit,
/// every buffered value is flushed to a spill file (encoded as a single-column
/// <see cref="QueryResultRow"/> via <see cref="SpillRowCodec"/>) and subsequent values are
/// written directly to disk; the in-memory list is released.
/// </para>
///
/// <para>
/// <b>Flag-off / under-threshold:</b> when <see cref="CamusDBConfig.SpillEnabled"/> is
/// <c>false</c>, or when the subquery ends without reaching the threshold, all values remain
/// in memory — byte-identical to a plain <c>List&lt;ColumnValue&gt;</c>.
/// </para>
///
/// <para>
/// <b>Re-enumerability:</b> <see cref="EnumerateAsync"/> may be called multiple times. In
/// the in-memory case it iterates the list; in the spill case it opens a new
/// <see cref="SpillRunReader"/> each time.
/// </para>
///
/// <para>
/// <b>Ownership and cleanup:</b> the caller must call <see cref="SealAsync"/> after all
/// values have been added, then dispose via <see cref="DisposeAsync"/> (or <c>await using</c>)
/// when the list is no longer needed. Disposing deletes any spill file.
/// </para>
/// </summary>
internal sealed class SpillableValueList : IAsyncDisposable
{
    // Column name used when encoding a ColumnValue as a single-column QueryResultRow for SpillRowCodec.
    private const string SpillColumn = "v";

    private readonly List<ColumnValue> _memoryValues = new();
    private SpillScope? _scope;
    private string? _spillPath;
    private FileStream? _spillWriter;
    private bool _overflowed;

    /// <summary>Total value count (memory + spill).</summary>
    public int Count { get; private set; }

    /// <summary><c>true</c> when values have overflowed to a spill file.</summary>
    public bool IsSpilled => _overflowed;

    /// <summary>
    /// Appends a value to the list. When <see cref="CamusDBConfig.SpillEnabled"/> is
    /// <c>true</c> and the buffer reaches <see cref="CamusDBConfig.SpillEffectiveThreshold"/>,
    /// all buffered values are flushed to a spill file and subsequent values are written
    /// directly to disk. Must not be called after <see cref="SealAsync"/>.
    /// </summary>
    public async Task AddAsync(ColumnValue value, CancellationToken ct = default)
    {
        if (!_overflowed)
        {
            _memoryValues.Add(value);
            Count++;

            if (CamusDBConfig.SpillEnabled && _memoryValues.Count >= CamusDBConfig.SpillEffectiveThreshold)
                await OverflowToSpillAsync(ct).ConfigureAwait(false);
        }
        else
        {
            SpillRowCodec.EncodeToStream(_spillWriter!, ToRow(value));
            Count++;
        }
    }

    /// <summary>
    /// Flushes and closes the spill writer. Must be called after all values have been added
    /// and before the first call to <see cref="EnumerateAsync"/>.
    /// </summary>
    public async Task SealAsync(CancellationToken ct = default)
    {
        if (_spillWriter is not null)
        {
            await _spillWriter.FlushAsync(ct).ConfigureAwait(false);
            _spillWriter.Close();
            _spillWriter = null;
        }
    }

    /// <summary>
    /// Enumerates all values in insertion order. When the list has not overflowed, values are
    /// yielded from the in-memory buffer. When it has overflowed, a new
    /// <see cref="SpillRunReader"/> is opened each time so this method is re-enumerable.
    /// <see cref="SealAsync"/> must have been called before the first enumeration.
    /// </summary>
    public async IAsyncEnumerable<ColumnValue> EnumerateAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (!_overflowed)
        {
            foreach (ColumnValue v in _memoryValues)
                yield return v;
            yield break;
        }

        SpillRunReader? reader = await SpillRunReader.OpenAsync(_spillPath!, ct).ConfigureAwait(false);
        if (reader is null)
            yield break;

        try
        {
            yield return reader.Current.Row[SpillColumn];
            while (await reader.AdvanceAsync(ct).ConfigureAwait(false))
                yield return reader.Current.Row[SpillColumn];
        }
        finally
        {
            await reader.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (_spillWriter is not null)
        {
            try { await _spillWriter.DisposeAsync().ConfigureAwait(false); }
            catch { /* ensure scope is still disposed */ }
            _spillWriter = null;
        }

        if (_scope is not null)
        {
            await _scope.DisposeAsync().ConfigureAwait(false);
            _scope = null;
        }
    }

    private async Task OverflowToSpillAsync(CancellationToken ct)
    {
        _scope = SpillFileManager.CreateScope(CamusDBConfig.DataDirectory);
        _spillPath = _scope.OpenWriter(out _spillWriter);

        foreach (ColumnValue v in _memoryValues)
            SpillRowCodec.EncodeToStream(_spillWriter, ToRow(v));

        await _spillWriter.FlushAsync(ct).ConfigureAwait(false);
        _memoryValues.Clear();
        _overflowed = true;
    }

    private static QueryResultRow ToRow(ColumnValue value) =>
        new(default, new Dictionary<string, ColumnValue> { { SpillColumn, value } });
}
