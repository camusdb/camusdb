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
    /// </summary>
    internal static async ValueTask<SpillRunReader?> OpenAsync(string path, CancellationToken ct = default)
    {
        FileStream fs = new(path, FileMode.Open, FileAccess.Read, FileShare.Read, 65536, useAsync: true);
        SpillRunReader reader = new(fs);
        if (!await reader.AdvanceAsync(ct).ConfigureAwait(false))
        {
            await reader.DisposeAsync().ConfigureAwait(false);
            return null;
        }
        return reader;
    }

    /// <summary>
    /// Reads the next framed record from the file, updating <see cref="Current"/>.
    /// Returns <c>true</c> if a record was read, <c>false</c> when the file is exhausted.
    /// </summary>
    public async ValueTask<bool> AdvanceAsync(CancellationToken ct = default)
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

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => _stream.DisposeAsync();
}
