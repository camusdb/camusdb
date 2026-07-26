
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// The access path a correlated EXISTS uses to test one outer row, chosen once at prepare time and
/// reused for every outer row of the query.
///
/// Without one, an existence check scans the whole inner table and applies the correlation predicate
/// afterwards, so a query costs O(outer candidates × total inner rows) and degrades with rows that
/// belong to entirely unrelated keys. This plan pins the inner index whose leading key columns are
/// all fixed by equality — either to a constant/parameter or to a column of the outer row — so the
/// check becomes a bounded seek over just the correlated key's entries.
///
/// The plan is a *template*: <see cref="PrefixBindings"/> are expressions, not values, because an
/// outer-row reference resolves to a different value for every outer row. The executor materializes
/// the seek key per row (see <see cref="ExistsSubqueryExecutor"/>).
///
/// Null when no index qualifies; the executor then falls back to the full inner scan, which stays
/// the correctness baseline. Nothing here changes which rows an EXISTS matches — the full inner
/// predicate is still evaluated on every row the seek returns.
/// </summary>
internal sealed record CorrelatedExistsSeekPlan(
    TableIndexSchema Index,
    ColumnType[] KeyTypes,
    bool Unique,
    IReadOnlyList<CorrelatedExistsSeekBinding> PrefixBindings);

/// <summary>
/// One index key column pinned by an equality in the inner WHERE, as the expression that produces
/// its value plus the column type the index stores.
///
/// <see cref="Expression"/> is evaluated against the *outer* row only, so it is restricted at plan
/// time to shapes that cannot reference an inner column: a literal, a parameter placeholder, or an
/// identifier qualified with an outer alias. <see cref="ExpectedType"/> is the inner column's
/// declared type; a value of any other type cannot be encoded into this index's key, so the executor
/// falls back to the scan rather than seeking with a key it would encode differently.
/// </summary>
internal sealed record CorrelatedExistsSeekBinding(
    NodeAst Expression,
    ColumnType ExpectedType);
