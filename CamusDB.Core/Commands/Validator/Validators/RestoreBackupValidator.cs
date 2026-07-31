/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsValidator.Validators;

/// <summary>
/// Validates a <see cref="RestoreBackupTicket"/>. Guards what a bad restore would otherwise get wrong:
/// (1) a real leaf id and an absolute target data root; (2) that the target is <b>not</b> the live
/// node's data directory or its kv/wal subdirs (restore is offline and must never write over a serving
/// node's storage); (3) that the target is empty or non-existent, so a restore never overwrites an
/// existing data root; (4) a non-negative <c>targetTimeMs</c>.
///
/// <para>The recoverability of a specific <c>targetTimeMs</c> is <b>not</b> checked here. Kahuna
/// validates the target against the selected chain's exact recoverable HLC coverage and rejects an
/// out-of-coverage point with <c>KahunaBackupOutcome.TargetOutsideCoverage</c> (mapped to
/// <see cref="CamusDBErrorCodes.RestorePointOutOfWindow"/>). A wall-clock window guard here would be
/// both redundant and wrong — it would reject a valid archived backup merely because wall time advanced
/// past the live retention window. Authorization and the backup-configured gate are enforced in the
/// executor.</para>
/// </summary>
internal sealed class RestoreBackupValidator : ValidatorBase
{
    public void Validate(RestoreBackupTicket ticket)
    {
        if (ticket.LeafBackupId == Guid.Empty)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "A restore requires a non-empty 'leafBackupId'");

        if (string.IsNullOrWhiteSpace(ticket.TargetDir))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "A restore requires a non-empty 'targetDir'");

        if (!Path.IsPathRooted(ticket.TargetDir))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"'targetDir' must be an absolute path, got '{ticket.TargetDir}'");

        // Restore is non-destructive to the live node: writing into the serving node's storage tree is
        // unsupported and would corrupt it. Reject the live data directory and its kv/wal children.
        string target = NormalizeDir(ticket.TargetDir);
        string dataDir = NormalizeDir(CamusDBConfig.DataDirectory);
        if (target == dataDir
            || target == NormalizeDir(Path.Combine(CamusDBConfig.DataDirectory, "kv"))
            || target == NormalizeDir(Path.Combine(CamusDBConfig.DataDirectory, "wal")))
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "'targetDir' must not be the live data directory (or its kv/wal subdirectory); " +
                "restore into a fresh directory, then restart a node pointed at it");
        }

        // Never overwrite an existing data root: require the target to be absent or empty.
        if (Directory.Exists(ticket.TargetDir) && Directory.EnumerateFileSystemEntries(ticket.TargetDir).Any())
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"'targetDir' must be empty or non-existent, but '{ticket.TargetDir}' already contains entries; " +
                "restore into a fresh directory");

        // 0 = chain max. A specific point's recoverability is enforced by Kahuna against the chain's
        // exact HLC coverage (TargetOutsideCoverage), not by a wall-clock heuristic here.
        if (ticket.TargetTimeMs < 0)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"'targetTimeMs' must be >= 0 (0 = latest recoverable point), got {ticket.TargetTimeMs}");
    }

    private static string NormalizeDir(string path)
        => Path.TrimEndingDirectorySeparator(Path.GetFullPath(path));
}
