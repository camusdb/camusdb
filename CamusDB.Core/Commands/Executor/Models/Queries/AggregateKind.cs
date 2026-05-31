
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>Supported aggregate functions in logical projections.</summary>
public enum AggregateKind
{
    Count,
    Sum,
    Average,
    Min,
    Max,
    Distinct,
}
