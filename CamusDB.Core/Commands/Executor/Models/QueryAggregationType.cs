
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models;

public enum QueryAggregationType
{
    None,
    Count,
    Sum,
    Average,
    Min,
    Max,
    Distinct,

    /// <summary>
    /// An aggregate of a C# extension (<c>AggregateFunctionCatalog</c>). It has no fast path: the
    /// aggregator runs it through the general workspace with one state object per group.
    /// </summary>
    Extension
}
