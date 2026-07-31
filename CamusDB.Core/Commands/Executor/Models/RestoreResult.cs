/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// Result of an offline restore. <see cref="DataRoot"/> is a ready-to-boot CamusDB data directory: the
/// restore lays out the restored storage under <c>{DataRoot}/kv</c> (Kahuna writes the RocksDB
/// checkpoint into its <c>{revision}</c> subdirectory there) and creates an empty <c>{DataRoot}/wal</c>,
/// so a fresh server started with <c>data_dir = DataRoot</c> boots directly on the restored image with
/// no manual file moves — which is why we return the data root rather than Kahuna's inner checkpoint
/// path (that path is not a valid <c>data_dir</c>, since a node resolves storage as
/// <c>{data_dir}/kv/{revision}</c>).
/// </summary>
public sealed record RestoreResult(
    string DataRoot,
    int PartitionsRestored,
    long EntriesApplied,
    long LastAppliedPhysicalMs,
    long MinRecoverablePhysicalMs,
    long MaxRecoverablePhysicalMs,
    IReadOnlyList<BackupInfo> Chain);
