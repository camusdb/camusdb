
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.App.Models;

public sealed class StartTransactionResponse
{
    public long TxnIdPT { get; set; }

    public uint TxnIdCounter { get; set; }

    public string Status { get; set; }

    public string? Code { get; set; }

    public string? Message { get; set; }

    public StartTransactionResponse(string status, long txnIdPT, uint txnIdCounter)
    {
        Status = status;
        TxnIdPT = txnIdPT;
        TxnIdCounter = txnIdCounter;
    }

    public StartTransactionResponse(string status, string code, string message)
    {
        Status = status;
        Code = code;
        Message = message;
    }    
}
