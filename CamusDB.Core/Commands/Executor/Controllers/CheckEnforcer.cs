
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Enforces all CHECK constraints on a fully-built row dictionary before the row is written.
/// Each constraint is evaluated via <see cref="CheckEvaluator"/> using SQL three-valued logic:
/// a check is violated only when it evaluates to <c>false</c>; <c>NULL</c>/unknown passes.
/// <para>
/// Called from <c>RowInserter.Validate</c> and <c>RowUpdater.FlushUpdateChunk</c> after defaults
/// have been applied and NOT NULL / length checks have passed, but before <c>RowEncoder.Encode</c>.
/// </para>
/// </summary>
internal static class CheckEnforcer
{
    /// <summary>
    /// Evaluates every CHECK constraint on <paramref name="table"/> against <paramref name="row"/>.
    /// Throws <see cref="CamusDBException"/> with <see cref="CamusDBErrorCodes.CheckConstraintViolation"/>
    /// if any constraint evaluates to <c>false</c>. The exception message names the violated constraint.
    /// </summary>
    public static void EnforceOnRow(TableDescriptor table, IReadOnlyDictionary<string, ColumnValue> row)
    {
        List<CheckConstraintSchema>? checks = table.Schema.CheckConstraints;

        if (checks is null || checks.Count == 0)
            return;

        foreach (CheckConstraintSchema check in checks)
        {
            if (check.ParsedCondition is null)
                continue; // defensive: malformed entry — skip rather than crash

            bool? result = CheckEvaluator.Evaluate(check.ParsedCondition, row);

            if (result == false)
                throw new CamusDBException(
                    CamusDBErrorCodes.CheckConstraintViolation,
                    $"new row violates check constraint \"{check.Name}\"");
        }
    }
}
