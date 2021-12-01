
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Support;

public class Guard
{
    private const int NOTCALLED = 0;

    private const int CALLED = 1;

    private int _state = NOTCALLED;
        
    public bool CheckAndSetFirstCall
    {
        get { return Interlocked.Exchange(ref _state, CALLED) == NOTCALLED; }
    }
}
