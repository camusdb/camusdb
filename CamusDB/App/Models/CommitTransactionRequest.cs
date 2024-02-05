
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.App.Models;

public sealed class CommitTransactionRequest
{
    public long TxnIdPT { get; set; }

    public uint TxnIdCounter { get; set; }

    public string? DatabaseName { get; set; }
}

