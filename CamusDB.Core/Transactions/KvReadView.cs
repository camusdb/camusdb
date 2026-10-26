/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace CamusDB.Core.Transactions;

/// <summary>
/// The read timestamp and the Kahuna read mode of one transactional read. The two travel together
/// because they are only correct as a pair: a statement timestamp read in the default mode does not
/// show the transaction's own staged writes. See <see cref="KvTransaction.QueryReadView"/> and
/// <see cref="KvTransaction.LatestReadView"/>.
/// </summary>
public readonly record struct KvReadView(HLCTimestamp Timestamp, KeyValueReadMode Mode);
