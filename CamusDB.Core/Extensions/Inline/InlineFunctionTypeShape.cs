/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

#if !CAMUSDB_BROWSER

using Microsoft.CodeAnalysis;

namespace CamusDB.Core.Extensions.Inline;

/// <summary>
/// Measures the real types of a body, after the first compile, against the limits that keep a call
/// inside its stack.
///
/// <para><b>Why types have limits at all.</b> A value tuple is a struct, and a struct lives on the
/// stack. <c>var t1 = (n, n); var t2 = (t1, t1);</c> and thirteen more lines make a local of half a
/// megabyte; the method that holds it overflows the stack of a pool thread when it is entered, and a
/// stack overflow ends the process. No budget check can stop that: the frame is taken before the
/// first statement runs. A type that nests thousands of levels (<c>var t2 = (n, t1);</c>, one level
/// for each line) does the same in the framework's own <c>Equals</c>, <c>CompareTo</c> and
/// <c>ToString</c>, which recurse once for each level.</para>
///
/// <para><b>The limits.</b></para>
/// <list type="bullet">
/// <item><see cref="MaxNames"/> type names in one type, and <see cref="MaxDepth"/> levels of nesting.
/// Real code stays under twenty names and five levels.</item>
/// <item><see cref="MaxValueBytes"/> bytes for one value of a value type.</item>
/// <item><see cref="MaxFrameBytes"/> bytes for the values that one method works with: the body, each
/// lambda and each local function count alone, because each has its own stack frame. The count is an
/// estimate of the frame: every local, and every operation that makes a new value larger than a
/// register (a call, an operator, a tuple, a conversion).</item>

/// </list>
///
/// <para><b>Why these numbers are enough.</b> The budget check at the start of each lambda, local
/// function and loop pass asks the runtime for sufficient stack, which is 128 KB or more on a 64-bit
/// process, and the guard around a call asks the same before it enters the body. A frame of the body
/// is a small multiple of <see cref="MaxFrameBytes"/>, so it fits in what the last check left, and
/// the next check runs before the next frame is taken.</para>
///
/// <para>A walk stops as soon as it passes a limit, so its cost does not depend on the size of the
/// type it measures.</para>
/// </summary>
internal sealed class InlineFunctionTypeShape
{
    /// <summary>The most type names one type may hold: <c>Dictionary&lt;string, List&lt;long&gt;&gt;</c> holds 4.</summary>
    public const int MaxNames = 256;

    /// <summary>The deepest nesting of a type: <c>List&lt;long[]&gt;</c> is 3 levels.</summary>
    public const int MaxDepth = 32;

    /// <summary>The largest value of a value type, in bytes.</summary>
    public const int MaxValueBytes = 1024;

    /// <summary>The most bytes of values that the body, one lambda or one local function works with.</summary>
    public const int MaxFrameBytes = 32 * 1024;

    /// <summary>What a type measures. A number over its limit is not exact: the walk stopped there.</summary>
    /// <param name="Names">The type names in the type.</param>
    /// <param name="Depth">The levels of nesting.</param>
    /// <param name="Bytes">The size of one value: the fields of a struct, or one reference.</param>
    internal readonly record struct Shape(int Names, int Depth, int Bytes)
    {
        public bool IsWithinLimits => Names <= MaxNames && Depth <= MaxDepth && Bytes <= MaxValueBytes;
    }

    // The compiler gives the same symbol object for the same local or expression type most of the
    // time, and comparing two types by structure costs as much as measuring one.
    private readonly Dictionary<ITypeSymbol, Shape> measured = new(ReferenceEqualityComparer.Instance);

    /// <summary>The shape of <paramref name="type"/>.</summary>
    public Shape Of(ITypeSymbol type)
    {
        if (measured.TryGetValue(type, out Shape shape))
            return shape;

        int names = 0;
        int depth = CountNames(type, level: 1, ref names);

        // The layout of a type that already failed is not needed, and its walk would not be short.
        int bytes = names <= MaxNames && depth <= MaxDepth ? Layout(type, level: 0).Size : 0;

        shape = new Shape(names, depth, bytes);
        measured[type] = shape;
        return shape;
    }

    /// <summary>Counts the names of a type into <paramref name="names"/> and returns its depth; stops over a limit.</summary>
    private static int CountNames(ITypeSymbol type, int level, ref int names)
    {
        names++;

        if (names > MaxNames || level > MaxDepth)
            return level;

        int deepest = level;

        if (type is IArrayTypeSymbol array)
            return Math.Max(deepest, CountNames(array.ElementType, level + 1, ref names));

        for (INamedTypeSymbol? current = type as INamedTypeSymbol; current is not null; current = current.ContainingType)
        {
            foreach (ITypeSymbol argument in current.TypeArguments)
            {
                deepest = Math.Max(deepest, CountNames(argument, level + 1, ref names));

                if (names > MaxNames || deepest > MaxDepth)
                    return deepest;
            }
        }

        return deepest;
    }

    /// <summary>
    /// The size and the alignment of one value, with the fields of a struct in declaration order.
    /// The runtime can pack a struct tighter, never looser, so this is the size or more.
    /// </summary>
    private static (int Size, int Alignment) Layout(ITypeSymbol type, int level)
    {
        switch (type.SpecialType)
        {
            case SpecialType.System_Boolean or SpecialType.System_Byte or SpecialType.System_SByte:
                return (1, 1);

            case SpecialType.System_Char or SpecialType.System_Int16 or SpecialType.System_UInt16:
                return (2, 2);

            case SpecialType.System_Int32 or SpecialType.System_UInt32 or SpecialType.System_Single:
                return (4, 4);

            case SpecialType.System_Int64 or SpecialType.System_UInt64 or SpecialType.System_Double or
                SpecialType.System_IntPtr or SpecialType.System_UIntPtr or SpecialType.System_DateTime:
                return (8, 8);

            case SpecialType.System_Decimal:
                return (16, 8);
        }

        if (type is not INamedTypeSymbol { IsValueType: true } named)
            return (8, 8);

        if (named.EnumUnderlyingType is { } underlying)
            return Layout(underlying, level);

        // A struct nested this deep was refused for its depth before its layout was asked for.
        if (level > MaxDepth)
            return (MaxValueBytes + 1, 8);

        int offset = 0;
        int alignment = 1;

        if (named.IsTupleType)
        {
            // The fields of a tuple are its type arguments, in order: seven elements and, for a
            // longer tuple, one more tuple with the rest. The members of the tuple symbol are not
            // used: they also name each element of the rest a second time.
            foreach (ITypeSymbol element in (named.TupleUnderlyingType ?? named).TypeArguments)
            {
                if (!Place(Layout(element, level + 1), ref offset, ref alignment))
                    return (MaxValueBytes + 1, alignment);
            }
        }
        else
        {
            foreach (ISymbol member in named.GetMembers())
            {
                if (member is IFieldSymbol { IsStatic: false, IsConst: false } field &&
                    !Place(Layout(field.Type, level + 1), ref offset, ref alignment))
                    return (MaxValueBytes + 1, alignment);
            }
        }

        return (RoundUp(Math.Max(offset, 1), alignment), alignment);
    }

    /// <summary>Puts one field after the ones before it. False when the struct is then over the limit.</summary>
    private static bool Place((int Size, int Alignment) field, ref int offset, ref int alignment)
    {
        offset = RoundUp(offset, field.Alignment) + field.Size;
        alignment = Math.Max(alignment, field.Alignment);

        return offset <= MaxValueBytes;
    }

    private static int RoundUp(int value, int alignment) => (value + alignment - 1) / alignment * alignment;
}

#endif
