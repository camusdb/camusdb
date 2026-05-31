
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>
/// A subquery in the <c>FROM</c> clause (<c>FROM (SELECT ...) alias</c>).
/// </summary>
/// <param name="Query">Inner select whose projected columns define the derived schema.</param>
/// <param name="Alias">Required alias for the derived source.</param>
public sealed record DerivedTableSource(
    SelectQuery Query,
    string Alias) : QuerySource;
