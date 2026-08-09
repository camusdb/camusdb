
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Cache;

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>
/// A subquery in the <c>FROM</c> clause (<c>FROM (SELECT ...) alias</c>).
/// </summary>
/// <param name="Query">Inner select whose projected columns define the derived schema.</param>
/// <param name="Alias">Required alias for the derived source.</param>
/// <param name="CacheHint">
/// A <c>{cache=name}</c> hint written on the source this derived table stands for. Only view
/// expansion sets it: a view reference may carry a hint, and expansion turns that reference into a
/// derived table, so without somewhere to put the hint it would have to be either discarded silently
/// or refused. It is carried so the response can report honestly what became of it.
/// </param>
/// <param name="OwnerName">
/// The name of the principal a view's body runs as, when this derived table is an expanded view.
/// Null for a derived table the user wrote themselves, which runs as the caller.
/// </param>
/// <param name="OwnerId">
/// The owner's immutable id, verified alongside the name so a dropped-and-recreated username cannot
/// inherit the original owner's access.
/// </param>
public sealed record DerivedTableSource(
    SelectQuery Query,
    string Alias,
    CacheHintOptions? CacheHint = null,
    string? OwnerName = null,
    string? OwnerId = null) : QuerySource;
