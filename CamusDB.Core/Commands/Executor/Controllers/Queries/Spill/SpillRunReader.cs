/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;

/// <summary>
/// Sequential reader for a single sorted spill run written by <see cref="SpillRowCodec.EncodeToStream"/>.
///
/// Usage pattern:
/// <code>
/// SpillRunReader? r = await SpillRunReader.OpenAsync(path, ct);
/// while (r != null &amp;&amp; !r.IsExhausted)
/// {
///     QueryResultRow row = r.Current;
///     // …
///     await r.AdvanceAsync(ct);
/// }
/// if (r != null) await r.DisposeAsync();
/// </code>
///
/// The reader opens the file with <see cref="FileShare.Read"/> so the startup sweep can still
/// inspect the directory while it is open.
/// </summary>
internal sealed class SpillRunReader : IAsyncDisposable
{
    private readonly FileStream _stream;

    private readonly byte[] _lenBuf = new byte[4];

    // One growable pooled buffer reused across records: each payload is read into the active
    // prefix and decoded synchronously before the next AdvanceAsync overwrites it. Grown (and the
    // old buffer returned) only when a record is larger than the current capacity, so steady-state
    // reading allocates no per-record array. Returned to the pool on dispose. Safe to reuse because
    // SpillRowCodec copies every string/byte value out into owned ColumnValue storage on decode, so
    // a decoded row never references this buffer.
    private byte[] _payloadBuffer = [];

    private QueryResultRow _current;

    private bool _exhausted;

    // When non-null the run was written in value-only format (no per-row column names);
    // AdvanceAsync reconstructs each row as a QueryRow using this layout.
    private readonly RowLayout? _layout;

    private SpillRunReader(FileStream stream, RowLayout? layout = null)
    {
        _stream = stream;
        _layout = layout;
    }

    /// <summary>Whether all records have been consumed from the underlying file.</summary>
    public bool IsExhausted => _exhausted;

    /// <summary>
    /// The most recently decoded row. Valid only when <see cref="IsExhausted"/> is
    /// <c>false</c>. Undefined after the reader is disposed.
    /// </summary>
    public QueryResultRow Current => _current;

    /// <summary>
    /// Opens a spill file and positions the reader at the first record by calling
    /// <see cref="AdvanceAsync"/> once. Returns <c>null</c> when the file is empty.
    /// The caller owns the returned reader and must dispose it.
    /// <para>
    /// When <paramref name="layout"/> is non-null the file was written in value-only format
    /// (no per-row column names); <see cref="AdvanceAsync"/> decodes each record using the
    /// supplied layout and returns a <see cref="QueryRow"/>. When null the file is schema-less
    /// (the legacy format where each record embeds column names).
    /// </para>
    /// Throws <see cref="CamusDBException"/> with
    /// <see cref="CamusDBErrorCodes.SpillStorageUnavailable"/> if the file cannot be opened,
    /// matching the fail-loud contract of <see cref="SpillScope.OpenReader"/>.
    /// </summary>
    internal static async ValueTask<SpillRunReader?> OpenAsync(string path, RowLayout? layout = null, CancellationToken ct = default)
    {
        FileStream fs;
        try
        {
            fs = new(path, FileMode.Open, FileAccess.Read, FileShare.Read, 65536, useAsync: true);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.SpillStorageUnavailable,
                $"Cannot open spill run file '{path}': {ex.Message}");
        }

        SpillRunReader reader = new(fs, layout);
        try
        {
            if (!await reader.AdvanceAsync(ct).ConfigureAwait(false))
            {
                await reader.DisposeAsync().ConfigureAwait(false);
                return null;
            }
        }
        catch
        {
            // AdvanceAsync failed (e.g. truncated/corrupt file → CADB0507). Close the stream we
            // just opened before rethrowing so the failed-open path does not leak the handle.
            await reader.DisposeAsync().ConfigureAwait(false);
            throw;
        }
        return reader;
    }

    /// <summary>
    /// Reads the next framed record from the file, updating <see cref="Current"/>.
    /// Returns <c>true</c> if a record was read, <c>false</c> when the file is exhausted.
    /// Throws <see cref="CamusDBException"/> with
    /// <see cref="CamusDBErrorCodes.SpillStorageUnavailable"/> if an I/O failure occurs
    /// mid-read, matching the fail-loud contract of the write path.
    /// </summary>
    public async ValueTask<bool> AdvanceAsync(CancellationToken ct = default)
    {
        try
        {
            // ReadAtLeastAsync returns 0 on clean EOF and <4 only on genuine truncation;
            // a bare ReadAsync could return 1–3 on a partial read even mid-file.
            int bytesRead = await _stream.ReadAtLeastAsync(_lenBuf, minimumBytes: 4,
                throwOnEndOfStream: false, ct).ConfigureAwait(false);
            if (bytesRead == 0)
            {
                _exhausted = true;
                return false;
            }
            if (bytesRead < 4)
                throw new InvalidDataException("SpillRunReader: truncated frame-length header");

            int payloadLen = BinaryPrimitives.ReadInt32LittleEndian(_lenBuf);
            if (payloadLen < 0 || payloadLen > CamusDBConfig.SpillMaxFrameBytes)
                throw new InvalidDataException(
                    $"SpillRunReader: invalid frame length {payloadLen} (max {CamusDBConfig.SpillMaxFrameBytes})");

            EnsurePayloadCapacity(payloadLen);
            await _stream.ReadExactlyAsync(_payloadBuffer.AsMemory(0, payloadLen), ct).ConfigureAwait(false);

            // Decode from the active prefix; SpillRowCodec copies values out so nothing keeps a
            // reference into _payloadBuffer once decode returns and the next Advance may reuse it.
            ReadOnlySpan<byte> payload = _payloadBuffer.AsSpan(0, payloadLen);
            if (_layout is not null)
            {
                QueryRow qr = SpillRowCodec.DecodeValueOnlyPayload(payload, _layout);
                _current = new QueryResultRow(qr.RowId, qr);
            }
            else
            {
                _current = SpillRowCodec.DecodePayload(payload);
            }
            return true;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or InvalidDataException)
        {
            // IOException/UnauthorizedAccessException: storage-level read failure.
            // InvalidDataException: a truncated frame-length header (above) or a corrupt payload
            // rejected by SpillRowCodec — both mean the spill run is unreadable. All map to the
            // same fail-loud CADB0507 so callers never see a raw framing/codec exception.
            throw new CamusDBException(
                CamusDBErrorCodes.SpillStorageUnavailable,
                $"Spill run file read failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Grows the reusable payload buffer to hold at least <paramref name="required"/> bytes, renting a
    /// larger array from the pool and returning the previous one. A no-op while the current buffer
    /// already fits, which is the steady state once the buffer has grown to the run's widest record.
    /// </summary>
    private void EnsurePayloadCapacity(int required)
    {
        if (_payloadBuffer.Length >= required)
            return;

        if (_payloadBuffer.Length > 0)
            ArrayPool<byte>.Shared.Return(_payloadBuffer);
        _payloadBuffer = ArrayPool<byte>.Shared.Rent(required);
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (_payloadBuffer.Length > 0)
        {
            ArrayPool<byte>.Shared.Return(_payloadBuffer);
            _payloadBuffer = [];
        }
        await _stream.DisposeAsync().ConfigureAwait(false);
    }
}
