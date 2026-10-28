/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>
/// The operator of a <see cref="SetOperationSource"/>. Each one has a DISTINCT form (the default,
/// which removes duplicate rows from the result) and an ALL form, carried separately in
/// <see cref="SetOperationSource.All"/>. Two rows are the same row when their values are equal
/// position by position, with NULL equal to NULL, as for DISTINCT and GROUP BY.
/// </summary>
public enum SetOperationKind
{
    /// <summary>The rows of both operands. <c>UNION ALL</c> returns every row of both.</summary>
    Union,

    /// <summary>
    /// The rows that are in both operands. <c>INTERSECT ALL</c> returns a row min(m, n) times, where m
    /// and n are its counts in the left and the right operand.
    /// </summary>
    Intersect,

    /// <summary>
    /// The rows of the left operand that are not in the right one. <c>EXCEPT ALL</c> returns a row
    /// max(m − n, 0) times.
    /// </summary>
    Except,
}
