/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// The one seam every join operator needs back into the plan tree.
///
/// <para>A join operator reads its child rows from another plan node, and that child can be any
/// node kind — another join, a sort, or a leaf scan. The operators are therefore mutually
/// recursive through the dispatch that <see cref="QueryJoinExecutor"/> owns. Each operator takes
/// this single-member interface instead of the executor itself, so it can recurse without seeing
/// the facade's fields or its other collaborators.</para>
/// </summary>
internal interface IJoinNodeExecutor
{
    /// <summary>
    /// Executes one physical plan node and streams its rows. The returned sequence is lazy: no
    /// storage read happens until the caller enumerates it.
    /// </summary>
    IAsyncEnumerable<QueryResultRow> ExecuteNode(PhysicalPlanNode node, QueryPlan plan);
}
