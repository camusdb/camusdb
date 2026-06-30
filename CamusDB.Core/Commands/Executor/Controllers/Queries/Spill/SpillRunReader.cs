/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers.Binary;
using System.IO;
using CamusDB.Core.CommandsExecutor.Models;

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
    private QueryResultRow _current;
    private bool _exhausted;

    private SpillRunReader(FileStream stream) => _stream = stream;

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
    /// Throws <see cref="CamusDBException"/> with
    /// <see cref="CamusDBErrorCodes.SpillStorageUnavailable"/> if the file cannot be opened,
    /// matching the fail-loud contract of <see cref="SpillScope.OpenReader"/>.
    /// </summary>
    internal static async ValueTask<SpillRunReader?> OpenAsync(string path, CancellationToken ct = default)
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

        SpillRunReader reader = new(fs);
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
            byte[] payload = new byte[payloadLen];
            await _stream.ReadExactlyAsync(payload, ct).ConfigureAwait(false);

            _current = SpillRowCodec.DecodePayload(payload);
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

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => _stream.DisposeAsync();
}
