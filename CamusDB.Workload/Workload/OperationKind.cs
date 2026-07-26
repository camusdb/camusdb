/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Workload.Workload;

/// <summary>The two operation shapes in the mixed workload. Kept bounded so it can double as a metric tag.</summary>
public enum OperationKind
{
    /// <summary>Read-only primary-key point lookup.</summary>
    Read,

    /// <summary>Optimistic read/write transaction: read a row then update it, commit.</summary>
    Write,
}
