/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

/// <summary>
/// Ticket for an offline restore: rebuild the data of the chain ending at <see cref="LeafBackupId"/>
/// into the target CamusDB <b>data root</b> <see cref="TargetDir"/>, replaying WAL up to
/// <see cref="TargetTimeMs"/>. A privileged server operation — the executor requires a superuser
/// <see cref="Principal"/> when authentication is on.
///
/// <para><see cref="TargetDir"/> is a CamusDB data directory, not Kahuna's inner storage path: the
/// restore lays out <c>{TargetDir}/kv</c> (the restored storage) and <c>{TargetDir}/wal</c>, so a fresh
/// node booted with <c>data_dir = TargetDir</c> reads the restored image directly. It must be an
/// absolute path, must not be the running node's data directory (nor its kv/wal subdirs), and must be
/// empty or non-existent so the restore never overwrites existing data — the validator enforces this.</para>
///
/// <para><see cref="TargetTimeMs"/> is Unix epoch milliseconds; <c>0</c> means "chain max" (latest
/// point the chain reconstructs). A non-zero value is rejected by the validator when it is in the
/// future or older than the configured PITR window.</para>
/// </summary>
public readonly struct RestoreBackupTicket
{
    public Guid LeafBackupId { get; }

    public string TargetDir { get; }

    public long TargetTimeMs { get; }

    /// <summary>The authenticated caller. Null when authentication is disabled; otherwise must be a superuser.</summary>
    public Principal? Principal { get; }

    public RestoreBackupTicket(Guid leafBackupId, string targetDir, long targetTimeMs, Principal? principal = null)
    {
        LeafBackupId = leafBackupId;
        TargetDir = targetDir;
        TargetTimeMs = targetTimeMs;
        Principal = principal;
    }
}
