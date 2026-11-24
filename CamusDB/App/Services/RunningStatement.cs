/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Transactions;

namespace CamusDB.App.Services;

/// <summary>
/// A statement that runs on a tracked transaction. While it is not disposed, the idle reaper leaves the
/// transaction alone. <see cref="Dispose"/> ends the statement and starts the idle time of the
/// transaction.
///
/// <para>Hold it with <c>using</c> for the whole statement. For a statement that returns rows, that
/// includes the read of its cursor and the write of its rows to the client, because a slow client keeps
/// the cursor open. A request that its client dropped ends its cursor through the request token, and so
/// ends the statement too.</para>
///
/// <para>The default value holds nothing, and its <see cref="Dispose"/> does nothing. Do not copy a
/// value and dispose both copies: each dispose ends one statement.</para>
/// </summary>
public readonly struct RunningStatement : IDisposable
{
    private readonly ActiveTransaction? entry;

    internal RunningStatement(ActiveTransaction entry) => this.entry = entry;

    /// <summary>The transaction the statement runs on. Not available on the default value.</summary>
    public KvTransaction Transaction =>
        entry?.Transaction ?? throw new InvalidOperationException("No statement is running");

    public void Dispose() => entry?.LeaveStatement();
}
