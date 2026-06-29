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
/// </summary>
public sealed class SpillScope : IAsyncDisposable
{
    private readonly string _scopeDir;
    private readonly List<FileStream> _openHandles = new();
    private bool _disposed;

    internal SpillScope(string scopeDir)
    {
        _scopeDir = scopeDir;
    }

    /// <summary>
    /// Creates a new spill file in this scope's directory, opens it for sequential write,
    /// and returns its absolute path. The caller writes to it and then passes the path back
    /// to <see cref="OpenReader"/> for the merge phase.
    ///
    /// Throws <see cref="CamusDBException"/> with
    /// <see cref="CamusDBErrorCodes.SpillStorageUnavailable"/> if the file cannot be created.
    /// </summary>
    public string OpenWriter(out FileStream stream)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        string path = Path.Combine(_scopeDir, $"{_openHandles.Count:D6}.spill");
        try
        {
            stream = new FileStream(
                path,
                FileMode.Create,
                FileAccess.Write,
                FileShare.None,
                bufferSize: 65536,
                useAsync: true);
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
    /// Closes all open spill-file handles and deletes the scope directory recursively.
    /// Safe to call from a <c>finally</c> block — exceptions during file deletion are
    /// swallowed so the original exception (if any) propagates unobstructed.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;
        _disposed = true;

        foreach (FileStream handle in _openHandles)
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
    }
}
