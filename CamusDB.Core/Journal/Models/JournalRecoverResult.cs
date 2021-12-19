
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Journal.Models;

public sealed class JournalRecoverResult
{
	public JournalGroupType Type { get; }

	public object State { get; }

	public object Step { get; }

	public JournalRecoverResult(JournalGroupType type, object state, object step)
	{
		Type = type;
		State = state;
		Step = step;
	}
}

