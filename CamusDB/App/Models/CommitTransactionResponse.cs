
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kommander.Time;

namespace CamusDB.App.Models;

public sealed class CommitTransactionResponse
{
    public string Status { get; set; }

    public string? Code { get; set; }

    public string? Message { get; set; }

    /// <summary>
    /// The commit timestamp of the transaction, as a causal token (<c>n</c>, <c>l</c>, <c>c</c>; all
    /// three are needed). Send it as <c>causalToken</c> on a later <c>/start-transaction</c> or
    /// autocommit query, so that read sees this commit even on a node whose clock is behind. Null when
    /// the commit has no timestamp, for example a transaction that never opened a session.
    /// </summary>
    public HLCTimestamp? CausalToken { get; set; }

    public CommitTransactionResponse(string status)
    {
        Status = status;
    }

    public CommitTransactionResponse(string status, string code, string message)
    {
        Status = status;
        Code = code;
        Message = message;
    }
}
