
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

    /// <summary>
    /// Keeps the exception that caused this one, for example an exception thrown by extension code,
    /// so a log shows the original stack trace. The message an API caller sees is still
    /// <paramref name="msg"/> alone.
    /// </summary>
    public CamusDBException(string code, string? msg, Exception? inner) : base(msg, inner)
    {
        Code = code;
    }
}
