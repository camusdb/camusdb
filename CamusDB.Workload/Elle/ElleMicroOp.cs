/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using System.Text;

namespace CamusDB.Workload.Elle;

/// <summary>
/// One step of an Elle list-append transaction: either append a unique value to the end of a list, or
/// read the whole list. The invocation carries no read result; the completion of a committed
/// transaction carries what each read actually returned.
/// </summary>
public readonly record struct ElleMicroOp(bool IsAppend, long Key, long Value, IReadOnlyList<long>? Read)
{
    public static ElleMicroOp Append(long key, long value) => new(true, key, value, null);

    public static ElleMicroOp ReadOf(long key) => new(false, key, 0, null);

    /// <summary>This read with its observed list. A missing list is recorded as <c>nil</c>, which Elle
    /// treats as the empty list.</summary>
    public ElleMicroOp WithRead(IReadOnlyList<long>? observed) => this with { Read = observed };

    /// <summary>
    /// Appends the EDN form: <c>[:append 3 17]</c>, <c>[:r 3 nil]</c> or <c>[:r 3 [1 2 17]]</c>.
    /// </summary>
    public void WriteEdn(StringBuilder sb)
    {
        if (IsAppend)
        {
            sb.Append("[:append ").Append(Key.ToString(CultureInfo.InvariantCulture)).Append(' ')
              .Append(Value.ToString(CultureInfo.InvariantCulture)).Append(']');
            return;
        }

        sb.Append("[:r ").Append(Key.ToString(CultureInfo.InvariantCulture)).Append(' ');
        if (Read is null)
        {
            sb.Append("nil]");
            return;
        }

        sb.Append('[');
        for (int i = 0; i < Read.Count; i++)
        {
            if (i > 0)
                sb.Append(' ');
            sb.Append(Read[i].ToString(CultureInfo.InvariantCulture));
        }
        sb.Append("]]");
    }
}
