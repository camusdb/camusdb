
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Util.Trees;

public sealed class BTreeMvccEntry<TValue>
{
    public BTreeCommitState CommitState { get; set; }

    public TValue? Value { get; set; }

    public BTreeMvccEntry(BTreeCommitState commitState, TValue? value)
	{
        CommitState = commitState;
        Value = value;
	}
}
