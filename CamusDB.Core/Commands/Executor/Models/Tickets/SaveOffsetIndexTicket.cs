
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Util.Time;
using CamusDB.Core.Util.Trees.Experimental;

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct SaveOffsetIndexTicket
{	
	public BPlusTree<ObjectIdValue, ObjectIdValue> Index { get; }

	public HLCTimestamp TxnId { get; }

	public ObjectIdValue Key { get; }

	public ObjectIdValue Value { get; }    

    public SaveOffsetIndexTicket(
        BPlusTree<ObjectIdValue, ObjectIdValue> index,
        HLCTimestamp txnId,
        ObjectIdValue key,
        ObjectIdValue value
	)
	{		
		Index = index;
		TxnId = txnId;
		Key = key;
		Value = value;
	}
}

