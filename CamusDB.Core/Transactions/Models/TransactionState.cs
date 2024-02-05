
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.Time;

namespace CamusDB.Core.Transactions.Models;

public sealed class TransactionState
{
	public HLCTimestamp TxnId { get; }

	public TransactionState(HLCTimestamp txnId)
	{
		TxnId = txnId;
	}
}
