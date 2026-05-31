
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

internal sealed record PreparedExistsSubquery(
    NodeAst? InnerWhere,
    BoundSelectQuery InnerBound,
    IReadOnlyList<BoundTableSource> OuterSources,
    IReadOnlyList<BoundDerivedTableSource> OuterDerivedSources);
