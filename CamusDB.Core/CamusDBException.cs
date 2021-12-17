
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core;

public class CamusDBException : Exception
{
    public string Code { get; }

    public CamusDBException(string code, string? msg) : base(msg)
    {
        Code = code;
    }
}
