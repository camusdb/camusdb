
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>
/// A resolved aggregate projection (e.g. <c>COUNT(*)</c>, <c>SUM(col)</c>).
/// </summary>
/// <param name="Kind">Aggregate function.</param>
/// <param name="Argument">
/// Operand expression, or <see langword="null"/> for <c>COUNT(*)</c>.
/// </param>
/// <param name="OutputName">Optional alias for the aggregate result column.</param>
public sealed record AggregateCall(
    AggregateKind Kind,
    NodeAst? Argument,
    string? OutputName);
