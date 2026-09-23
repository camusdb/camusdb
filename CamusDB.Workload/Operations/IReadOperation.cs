/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Workload.Operations;

/// <summary>
/// The read half of the mixed workload. <see cref="ReadOperation"/> is the primary-key point read of the
/// seeded dataset; <see cref="AppendOperation"/> runs a read-only list-append transaction instead, so
/// the reads of an Elle run land in its history too. Both report <see cref="Workload.OperationKind.Read"/>.
/// </summary>
public interface IReadOperation
{
    Task<OperationResult> ExecuteAsync(long rowIndex, CancellationToken ct);
}
