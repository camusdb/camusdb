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
/// A re-enumerable, spill-capable buffer of <see cref="QueryResultRow"/> values.
/// Used by derived-table materialization and DELETE/UPDATE row collection to avoid unbounded
/// in-memory buffering when the matched row set is large.
///
/// <para>
/// <b>Memory phase:</b> rows are accumulated in a <see cref="List{T}"/> until the buffer
/// reaches <see cref="CamusDBConfig.SpillEffectiveThreshold"/>. When the threshold is hit,
/// every buffered row is flushed to a single spill file (via <see cref="SpillRowCodec"/>)
/// and subsequent rows are written directly to that file; the in-memory list is cleared.
/// </para>
///
/// <para>
/// <b>Flag-off / under-threshold:</b> when <see cref="CamusDBConfig.SpillEnabled"/> is
/// <c>false</c>, or when the input ends without the threshold being hit, all rows stay in
/// memory and no spill file is ever created — byte-identical to a plain
/// <c>List&lt;QueryResultRow&gt;</c>.
/// </para>
///
/// <para>
/// <b>Re-enumerability:</b> <see cref="EnumerateAsync"/> may be called multiple times. In
/// the in-memory case it iterates the list; in the spill case it opens a new
/// <see cref="SpillRunReader"/> each time. Both paths are safe to re-enter sequentially
/// (not concurrently).
/// </para>
///
/// <para>
/// <b>RowId preservation:</b> <see cref="QueryResultRow.RowId"/> is encoded by
/// <see cref="SpillRowCodec"/> and decoded intact, so DML callers (RowDeleter, RowUpdater)
/// that buffer match sets via this class always recover the original row id.
/// </para>
///
/// <para>
/// <b>Ownership and cleanup:</b> the caller must call <see cref="SealAsync"/> once after all
/// rows have been added, then dispose via <see cref="DisposeAsync"/> (or <c>await using</c>)
/// when the list is no longer needed. Disposing deletes the spill file.
/// </para>
/// </summary>
internal sealed class SpillableRowList : IAsyncDisposable
{
    private readonly List<QueryResultRow> _memoryRows = [];

    private SpillScope? _scope;

    private string? _spillPath;

    private FileStream? _spillWriter;
    
    private bool _overflowed;

    /// <summary>Total row count (memory + spill).</summary>
    public int Count { get; private set; }

    /// <summary>
    /// Appends a row to the list. When <see cref="CamusDBConfig.SpillEnabled"/> is
    /// <c>true</c> and the buffer reaches <see cref="CamusDBConfig.SpillEffectiveThreshold"/>,
    /// all buffered rows are flushed to a spill file and subsequent rows are written
    /// directly to disk. Must not be called after <see cref="SealAsync"/>.
    /// </summary>
    public async Task AddAsync(QueryResultRow row, CancellationToken ct = default)
    {
        if (!_overflowed)
        {
            _memoryRows.Add(row);
            Count++;

            if (CamusDBConfig.SpillEnabled && _memoryRows.Count >= CamusDBConfig.SpillEffectiveThreshold)
                await OverflowToSpillAsync(ct).ConfigureAwait(false);
        }
        else
        {
            SpillRowCodec.EncodeToStream(_spillWriter!, row);
            Count++;
        }
    }

    /// <summary>
    /// Flushes and closes the spill writer. Must be called after all rows have been added
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
    /// Enumerates all rows in insertion order, preserving <see cref="QueryResultRow.RowId"/>.
    /// When the list has not overflowed, rows are yielded from the in-memory buffer. When it
    /// has overflowed, a new <see cref="SpillRunReader"/> is opened each time so this method
    /// is re-enumerable. <see cref="SealAsync"/> must have been called before the first
    /// enumeration.
    /// </summary>
    public async IAsyncEnumerable<QueryResultRow> EnumerateAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (!_overflowed)
        {
            foreach (QueryResultRow row in _memoryRows)
                yield return row;
            yield break;
        }

        SpillRunReader? reader = await SpillRunReader.OpenAsync(_spillPath!, ct).ConfigureAwait(false);
        if (reader is null)
            yield break;

        try
        {
            yield return reader.Current;
            while (await reader.AdvanceAsync(ct).ConfigureAwait(false))
                yield return reader.Current;
        }
        finally
        {
            await reader.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Flushes all in-memory rows to a new spill file, clears the memory buffer, and marks
    /// the list as overflowed so subsequent rows go straight to disk.
    /// </summary>
    private async Task OverflowToSpillAsync(CancellationToken ct)
    {
        _scope = SpillFileManager.CreateScope(CamusDBConfig.DataDirectory);
        _spillPath = _scope.OpenWriter(out _spillWriter);

        foreach (QueryResultRow r in _memoryRows)
            SpillRowCodec.EncodeToStream(_spillWriter, r);

        await _spillWriter.FlushAsync(ct).ConfigureAwait(false);
        _memoryRows.Clear();
        _overflowed = true;
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (_spillWriter is not null)
        {
            try { await _spillWriter.DisposeAsync().ConfigureAwait(false); }
            catch { /* swallow — ensure scope is still disposed */ }
            _spillWriter = null;
        }

        if (_scope is not null)
        {
            await _scope.DisposeAsync().ConfigureAwait(false);
            _scope = null;
        }
    }
}
