/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using System.Text;

namespace CamusDB.Workload.Elle;

/// <summary>The four step shapes Elle's two checkers read. The list-append checker takes
/// <see cref="Append"/> and <see cref="ListRead"/>; the rw-register checker takes <see cref="Write"/>
/// and <see cref="RegisterRead"/>. A history mixes the two pairs never.</summary>
public enum ElleMicroOpKind
{
    /// <summary><c>[:append k v]</c>: add a unique value to the end of list <c>k</c>.</summary>
    Append,

    /// <summary><c>[:r k [..]]</c>: read the whole of list <c>k</c>; <c>nil</c> when the list is absent.</summary>
    ListRead,

    /// <summary><c>[:w k v]</c>: blindly set register <c>k</c> to a unique value.</summary>
    Write,

    /// <summary><c>[:r k v]</c>: read register <c>k</c>; <c>nil</c> when no write reached it.</summary>
    RegisterRead,
}

/// <summary>
/// One step of an Elle transaction. The invocation carries no read result; the completion of a
/// committed transaction carries what each read actually returned.
///
/// <para>Two models share this type. In list-append a read returns the whole list, so one read fixes
/// a prefix of the key's version order. In rw-register a write replaces the value, so a read returns
/// one value and Elle infers version orders only from the initial <c>nil</c>, from the order of the
/// steps inside one transaction, and from the write a read observed. Both models rely on every
/// written value being unique across the run.</para>
/// </summary>
public readonly record struct ElleMicroOp(
    ElleMicroOpKind Kind, long Key, long Value, IReadOnlyList<long>? Read, long? RegisterValue)
{
    public static ElleMicroOp Append(long key, long value) => new(ElleMicroOpKind.Append, key, value, null, null);

    public static ElleMicroOp ReadOf(long key) => new(ElleMicroOpKind.ListRead, key, 0, null, null);

    public static ElleMicroOp Write(long key, long value) => new(ElleMicroOpKind.Write, key, value, null, null);

    public static ElleMicroOp RegisterReadOf(long key) => new(ElleMicroOpKind.RegisterRead, key, 0, null, null);

    public bool IsAppend => Kind == ElleMicroOpKind.Append;

    public bool IsWrite => Kind == ElleMicroOpKind.Write;

    /// <summary>An append or a register write: a step that changes the key.</summary>
    public bool IsMutation => Kind is ElleMicroOpKind.Append or ElleMicroOpKind.Write;

    public bool IsRead => Kind is ElleMicroOpKind.ListRead or ElleMicroOpKind.RegisterRead;

    /// <summary>This list read with its observed list. A missing list is recorded as <c>nil</c>, which
    /// Elle treats as the empty list.</summary>
    public ElleMicroOp WithRead(IReadOnlyList<long>? observed) => this with { Read = observed };

    /// <summary>This register read with its observed value. A missing row is recorded as <c>nil</c>,
    /// the initial state Elle assumes for every register.</summary>
    public ElleMicroOp WithRegisterRead(long? observed) => this with { RegisterValue = observed };

    /// <summary>
    /// Appends the EDN form: <c>[:append 3 17]</c>, <c>[:r 3 nil]</c>, <c>[:r 3 [1 2 17]]</c>,
    /// <c>[:w 3 17]</c> or <c>[:r 3 17]</c>.
    /// </summary>
    public void WriteEdn(StringBuilder sb)
    {
        switch (Kind)
        {
            case ElleMicroOpKind.Append:
                sb.Append("[:append ").Append(Key.ToString(CultureInfo.InvariantCulture)).Append(' ')
                  .Append(Value.ToString(CultureInfo.InvariantCulture)).Append(']');
                return;

            case ElleMicroOpKind.Write:
                sb.Append("[:w ").Append(Key.ToString(CultureInfo.InvariantCulture)).Append(' ')
                  .Append(Value.ToString(CultureInfo.InvariantCulture)).Append(']');
                return;

            case ElleMicroOpKind.RegisterRead:
                sb.Append("[:r ").Append(Key.ToString(CultureInfo.InvariantCulture)).Append(' ');
                if (RegisterValue is long value)
                    sb.Append(value.ToString(CultureInfo.InvariantCulture)).Append(']');
                else
                    sb.Append("nil]");
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
