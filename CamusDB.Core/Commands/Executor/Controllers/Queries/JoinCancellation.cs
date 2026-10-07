
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Resolves the one cancellation token a join operator must observe.
///
/// <para>A join sees two tokens: the ticket token, which cancels the whole statement, and the
/// token an <c>await foreach</c> consumer supplied through <c>WithCancellation</c>, which cancels
/// just this enumeration. A leaf scan observes both on every row it reads, so a join that is
/// still reading storage stops promptly. A join that has finished reading — a Grace hash join
/// draining its spill files, a materialized merge join padding its buffered left rows — reads
/// nothing more and must check the token itself, or an abandoned request keeps allocating rows
/// and holding its spill scope until the padding loop ends.</para>
///
/// <para><see cref="Link"/> returns null when there is nothing to link, so the common case (the
/// consumer passed no token, or the ticket token itself) allocates nothing; the caller then uses
/// <see cref="Effective"/> to pick the token to pass down and to poll.</para>
/// </summary>
internal static class JoinCancellation
{
    /// <summary>
    /// Links <paramref name="enumeratorToken"/> with the ticket token, or returns null when the
    /// enumerator token cannot be cancelled or is the ticket token already. The caller owns the
    /// returned source and must dispose it when the enumeration ends.
    /// </summary>
    internal static CancellationTokenSource? Link(QueryPlan plan, CancellationToken enumeratorToken)
    {
        CancellationToken ticketToken = plan.Ticket.CancellationToken;

        if (!enumeratorToken.CanBeCanceled || enumeratorToken == ticketToken)
            return null;

        return CancellationTokenSource.CreateLinkedTokenSource(ticketToken, enumeratorToken);
    }

    /// <summary>The token to observe: the linked one when <see cref="Link"/> made one, else the ticket token.</summary>
    internal static CancellationToken Effective(QueryPlan plan, CancellationTokenSource? linked) =>
        linked?.Token ?? plan.Ticket.CancellationToken;
}
