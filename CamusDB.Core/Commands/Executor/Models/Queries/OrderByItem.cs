
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>
/// One sort key in an <c>ORDER BY</c> clause.
/// </summary>
/// <param name="Expression">Sort expression (typically a column identifier).</param>
/// <param name="Direction">Ascending or descending order.</param>
public sealed record OrderByItem(NodeAst Expression, OrderType Direction);
