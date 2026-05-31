
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Materialized uncorrelated IN/NOT IN subquery column values (QP5.3 / QP5.3a).
/// </summary>
internal sealed record InSubqueryMaterialization(
    IReadOnlyList<ColumnValue> Values,
    bool ContainsNull,
    bool IsEmpty);
