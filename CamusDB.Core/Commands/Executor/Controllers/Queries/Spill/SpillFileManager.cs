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
/// The two-level directory scheme lets the startup sweep delete this instance's
/// leftover directory without touching any other concurrent instance's files.
///
/// <b>Usage pattern:</b>
/// <code>
/// await using SpillScope scope = SpillFileManager.CreateScope(dataDir);
/// string path = scope.OpenWriter(out FileStream writer);
/// // … write rows …
/// await writer.FlushAsync();
/// writer.Close();
/// FileStream reader = scope.OpenReader(path);
/// // … read rows …
/// // scope disposed on exit → directory deleted
/// </code>
/// </summary>
public static class SpillFileManager
{
    /// <summary>
    /// Identifies this process's spill directory. A fresh random value is generated per
    /// process start so restarts never collide with a sibling process. Tests may override
    /// this to a deterministic value before calling <see cref="RunStartupSweep"/>.
    /// </summary>
    public static string InstanceId { get; set; } = Guid.NewGuid().ToString("N");

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
    // Startup sweep
    // ──────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Called once at node boot. Deletes the entire
    /// <c>{dataDirectory}/tmp/spill/{InstanceId}/</c> directory (and everything beneath
    /// it) if it exists, removing leftover scope files from a previous crash of this
    /// instance. Directories belonging to any other instance ID are never touched, so
    /// concurrent processes sharing the same <paramref name="dataDirectory"/> are safe.
    /// </summary>
    public static void RunStartupSweep(string dataDirectory)
    {
        string instanceDir = BuildInstanceDir(dataDirectory);

        try
        {
            if (Directory.Exists(instanceDir))
                Directory.Delete(instanceDir, recursive: true);
        }
        catch
        {
            // Best-effort: a sweep failure should not prevent the node from starting.
        }
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Path helpers
    // ──────────────────────────────────────────────────────────────────────────

    private static string BuildInstanceDir(string dataDirectory) =>
        Path.Combine(dataDirectory, "tmp", "spill", InstanceId);

    private static string BuildScopeDir(string dataDirectory, string scopeId) =>
        Path.Combine(dataDirectory, "tmp", "spill", InstanceId, scopeId);
}
