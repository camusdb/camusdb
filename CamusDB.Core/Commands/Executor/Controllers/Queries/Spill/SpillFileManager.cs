/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.IO;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;

/// <summary>
/// Factory and startup-sweep helper for spill-to-disk temp files.
///
/// All spill files live under <c>{DataDir}/tmp/spill/{InstanceId}/{scopeId}/</c>.
///
/// <b>Liveness detection</b>: each process holds an exclusive
/// <c>{DataDir}/tmp/spill/{InstanceId}/.lock</c> file for its entire lifetime via
/// <see cref="AcquireInstanceLock"/>. At startup, <see cref="RunStartupSweep"/> enumerates
/// every subdirectory under the spill root and, for each, tries to open its <c>.lock</c>
/// file exclusively. Success means the previous owner process is dead and the directory is
/// orphaned; failure (file locked) means a live concurrent instance owns it. This correctly
/// reclaims dirs left by crashed processes while never touching a concurrent live instance's
/// files, regardless of how many processes share the same <paramref name="DataDirectory"/>.
///
/// <b>Call order at node boot:</b>
/// <code>
/// SpillFileManager.AcquireInstanceLock(CamusDBConfig.DataDirectory);
/// SpillFileManager.RunStartupSweep(CamusDBConfig.DataDirectory);
/// </code>
/// <b>Call at graceful shutdown:</b>
/// <code>
/// SpillFileManager.ReleaseInstanceLock();
/// </code>
///
/// <b>Per-query usage pattern:</b>
/// <code>
/// await using SpillScope scope = SpillFileManager.CreateScope(dataDir);
/// string path = scope.OpenWriter(out FileStream writer);
/// // … write rows …
/// await writer.FlushAsync();
/// writer.Close();
/// FileStream reader = scope.OpenReader(path);
/// // … read rows …
/// // scope disposed on exit → scope directory deleted
/// </code>
/// </summary>
public static class SpillFileManager
{
    private const string LockFileName = ".lock";

    /// <summary>
    /// Unique identifier for this process's spill directory. A fresh random value is
    /// generated once per process start so restarts never collide with a sibling process.
    /// Tests may set this before calling <see cref="AcquireInstanceLock"/> to get a
    /// deterministic directory path.
    /// </summary>
    public static string InstanceId { get; set; } = Guid.NewGuid().ToString("N");

    private static FileStream? _instanceLock;

    // ──────────────────────────────────────────────────────────────────────────
    // Process-lifetime lock
    // ──────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Creates this instance's subdirectory under the spill root and opens
    /// <c>{instanceDir}/.lock</c> with <see cref="FileShare.None"/>.
    /// The OS releases this handle automatically if the process crashes, making the
    /// directory reclaimable by <see cref="RunStartupSweep"/> on the next boot.
    ///
    /// Idempotent: calling more than once on the same process is safe (second call is a no-op).
    /// Best-effort: a failure to acquire the lock is swallowed — spill still works, the
    /// directory just will not be swept at the next boot if the process crashes.
    /// </summary>
    public static void AcquireInstanceLock(string dataDirectory)
    {
        if (_instanceLock is not null)
            return;

        try
        {
            string instanceDir = BuildInstanceDir(dataDirectory);
            Directory.CreateDirectory(instanceDir);
            string lockPath = Path.Combine(instanceDir, LockFileName);
            _instanceLock = new FileStream(
                lockPath,
                FileMode.OpenOrCreate,
                FileAccess.ReadWrite,
                FileShare.None);
        }
        catch
        {
            // Best-effort: lock failure must not prevent the node from starting.
        }
    }

    /// <summary>
    /// Releases the process-lifetime lock file. Called during graceful shutdown.
    /// After this call, <see cref="RunStartupSweep"/> on another instance will be able to
    /// reclaim this instance's directory.
    /// </summary>
    public static void ReleaseInstanceLock()
    {
        _instanceLock?.Dispose();
        _instanceLock = null;
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Startup sweep
    // ──────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Called once at node boot (after <see cref="AcquireInstanceLock"/>). Enumerates every
    /// subdirectory under <c>{dataDirectory}/tmp/spill/</c> and deletes those whose
    /// <c>.lock</c> file can be opened exclusively — meaning the previous owner process has
    /// exited or crashed. Directories whose <c>.lock</c> is still held (live concurrent
    /// instance) are skipped. This instance's own directory is always skipped.
    /// Failures during deletion are silently swallowed so they never block the boot.
    /// </summary>
    public static void RunStartupSweep(string dataDirectory)
    {
        string spillRoot = Path.Combine(dataDirectory, "tmp", "spill");
        if (!Directory.Exists(spillRoot))
            return;

        string myDir = BuildInstanceDir(dataDirectory);

        foreach (string dir in Directory.GetDirectories(spillRoot))
        {
            // Never touch our own directory.
            if (string.Equals(
                    Path.GetFullPath(dir),
                    Path.GetFullPath(myDir),
                    StringComparison.OrdinalIgnoreCase))
                continue;

            bool isDead;
            string lockPath = Path.Combine(dir, LockFileName);

            if (!File.Exists(lockPath))
            {
                // No lock file: orphaned dir from a crash before the lock was created,
                // or from an older version of the code.
                isDead = true;
            }
            else
            {
                try
                {
                    // Try to grab the lock exclusively.  If the owning process is still
                    // alive, this throws IOException and we leave the directory alone.
                    using FileStream probe = new(
                        lockPath,
                        FileMode.Open,
                        FileAccess.ReadWrite,
                        FileShare.None);
                    isDead = true;
                }
                catch (IOException)
                {
                    // Lock is still held by a live process.
                    isDead = false;
                }
                catch
                {
                    // Unexpected error (permissions, etc.) — leave the dir alone.
                    isDead = false;
                }
            }

            if (isDead)
            {
                try { Directory.Delete(dir, recursive: true); }
                catch { /* best-effort */ }
            }
        }
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Scope allocation
    // ──────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Allocates a new per-query <see cref="SpillScope"/> under
    /// <c>{dataDirectory}/tmp/spill/{InstanceId}/{newScopeId}/</c> and creates the
    /// scope directory.
    ///
    /// Throws <see cref="CamusDBException"/> (<see cref="CamusDBErrorCodes.SpillStorageUnavailable"/>)
    /// if the directory cannot be created (disk full, bad path, permission denied, etc.).
    ///
    /// Callers should check <see cref="CamusDBConfig.SpillEnabled"/> before calling this
    /// method; the scope itself does not enforce the flag.
    /// </summary>
    public static SpillScope CreateScope(string dataDirectory)
    {
        string scopeDir = BuildScopeDir(dataDirectory, Guid.NewGuid().ToString("N"));

        try
        {
            Directory.CreateDirectory(scopeDir);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.SpillStorageUnavailable,
                $"Cannot create spill scope directory '{scopeDir}': {ex.Message}");
        }

        return new SpillScope(scopeDir);
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Path helpers
    // ──────────────────────────────────────────────────────────────────────────

    private static string BuildInstanceDir(string dataDirectory) =>
        Path.Combine(dataDirectory, "tmp", "spill", InstanceId);

    private static string BuildScopeDir(string dataDirectory, string scopeId) =>
        Path.Combine(dataDirectory, "tmp", "spill", InstanceId, scopeId);
}
