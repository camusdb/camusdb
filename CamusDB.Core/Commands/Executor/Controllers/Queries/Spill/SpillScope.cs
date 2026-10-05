/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.IO;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;

/// <summary>
/// Lifetime scope for a single query's spill files. Allocated by
/// <see cref="SpillFileManager.CreateScope"/> and disposed (via <c>await using</c> or a
/// <c>try/finally</c>) on query completion, cancellation, or exception.
///
/// On dispose all open file handles are closed and the entire scope directory — including
/// every file created via <see cref="OpenWriter"/> — is deleted recursively.
///
/// <para><b>Byte limits:</b> each write through a <see cref="SpillWriteStream"/> reserves its bytes
/// in the node's <see cref="SpillDiskBudget"/> first, and dispose gives back the whole total of the
/// scope. A reservation that would pass a limit throws, so the statement fails, and its owner's
/// <c>finally</c> disposes the scope and deletes its files. The limits are read from the options of
/// the statement when the scope is created, so a runtime change applies from the next scope.</para>
/// </summary>
public sealed class SpillScope : IAsyncDisposable
{
    private readonly string _scopeDir;
    private readonly SpillDiskBudget _budget;
    private readonly long _maxTotalBytes;
    private readonly long _minFreeDiskBytes;
    // Not thread-safe: file numbering, handle tracking and the reserved total assume a single
    // writer at a time. Per-query execution is sequential today; revisit if intra-query
    // parallelism is added. The budget itself is shared and thread-safe.
    private readonly List<Stream> _openHandles = new();
    private int _fileCount;
    private long _reservedBytes;
    private bool _disposed;

    internal SpillScope(string scopeDir, SpillDiskBudget budget, long maxTotalBytes, long minFreeDiskBytes)
    {
        _scopeDir = scopeDir;
        _budget = budget;
        _maxTotalBytes = maxTotalBytes;
        _minFreeDiskBytes = minFreeDiskBytes;
    }

    /// <summary>Bytes the files of this scope hold, as reserved by its writers.</summary>
    public long ReservedBytes => _reservedBytes;

    /// <summary>
    /// Reserves <paramref name="bytes"/> for a write by one of this scope's writers, or throws
    /// <see cref="CamusDBErrorCodes.SpillLimitExceeded"/> or
    /// <see cref="CamusDBErrorCodes.InsufficientDiskSpace"/> and reserves nothing.
    /// </summary>
    internal void Reserve(int bytes)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        _budget.Reserve(bytes, _maxTotalBytes, _minFreeDiskBytes);
        _reservedBytes += bytes;
    }

    /// <summary>
    /// Creates a new spill file in this scope's directory, opens it for sequential write,
    /// and returns its absolute path. The caller writes to it and then passes the path back
    /// to <see cref="OpenReader"/> for the merge phase.
    ///
    /// Throws <see cref="CamusDBException"/> with
    /// <see cref="CamusDBErrorCodes.SpillStorageUnavailable"/> if the file cannot be created.
    /// </summary>
    public string OpenWriter(out SpillWriteStream stream)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        string path = Path.Combine(_scopeDir, $"{_fileCount++:D6}.spill");
        try
        {
            FileStream file = new FileStream(
                path,
                FileMode.Create,
                FileAccess.Write,
                FileShare.None,
                bufferSize: 65536,
                useAsync: true);
            stream = new SpillWriteStream(file, this);
            _openHandles.Add(stream);
            return path;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.SpillStorageUnavailable,
                $"Cannot create spill file '{path}': {ex.Message}");
        }
    }

    /// <summary>
    /// Opens an existing spill file (previously written via <see cref="OpenWriter"/>) for
    /// sequential read. The returned stream is tracked and closed on dispose.
    ///
    /// Throws <see cref="CamusDBException"/> with
    /// <see cref="CamusDBErrorCodes.SpillStorageUnavailable"/> if the file cannot be opened.
    /// </summary>
    public FileStream OpenReader(string filePath)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        try
        {
            FileStream stream = new FileStream(
                filePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                bufferSize: 65536,
                useAsync: true);
            _openHandles.Add(stream);
            return stream;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.SpillStorageUnavailable,
                $"Cannot open spill file '{filePath}': {ex.Message}");
        }
    }

    /// <summary>The directory where all spill files for this scope are stored.</summary>
    public string ScopeDirectory => _scopeDir;

    /// <summary>
    /// Closes all open spill-file handles, deletes the scope directory recursively, and gives back
    /// the bytes this scope reserved.
    /// Safe to call from a <c>finally</c> block — exceptions during file deletion are
    /// swallowed so the original exception (if any) propagates unobstructed.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;
        _disposed = true;

        foreach (Stream handle in _openHandles)
        {
            try { await handle.DisposeAsync().ConfigureAwait(false); }
            catch { /* swallow */ }
        }
        _openHandles.Clear();

        try
        {
            if (Directory.Exists(_scopeDir))
                Directory.Delete(_scopeDir, recursive: true);
        }
        catch { /* swallow */ }

        // Given back even when the delete failed: an undeletable file is a permission problem, and
        // holding its bytes forever would make every later spill on this node fail. The startup
        // sweep removes such a directory.
        _budget.Release(_reservedBytes);
        _reservedBytes = 0;
    }
}
