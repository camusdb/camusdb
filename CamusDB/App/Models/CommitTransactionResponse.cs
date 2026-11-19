
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
    /// The causal token of the commit (<c>n</c>, <c>l</c>, <c>c</c>; all three are needed): a reading
    /// of the server clock after the commit, at or above <see cref="CommitTimestamp"/>. Send it as
    /// <c>causalToken</c> on a later <c>/start-transaction</c> or autocommit query, so that read sees
    /// this commit even on a node whose clock is behind. Null when the server has no clock reading to
    /// give.
    /// </summary>
    public HLCTimestamp? CausalToken { get; set; }

    /// <summary>
    /// The Kahuna commit timestamp (<c>n</c>, <c>l</c>, <c>c</c>): the HLC timestamp that every write
    /// of the transaction carries. Two transactions that write the same row commit in this order. Null
    /// when the transaction wrote nothing.
    /// </summary>
    public HLCTimestamp? CommitTimestamp { get; set; }

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
