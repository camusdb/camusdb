/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;

namespace CamusDB.Core.CommandsExecutor.Models.Jsonb;

/// <summary>
/// Read operations on a <c>jsonb</c> document, straight on its CBOR bytes: navigation, the PostgreSQL
/// comparison order, a hash that agrees with it, containment (<c>@&gt;</c>) and key existence
/// (<c>?</c>). Nothing here builds a tree for a document.
///
/// <para>Methods take the CBOR item (<see cref="Item"/> strips the version byte of a cell). A slice
/// they return is itself a complete item, because every nested CBOR item is.</para>
///
/// <para><see cref="Compare"/> and <see cref="Contains"/> recurse once per nesting level. That is safe
/// because every document was checked against <see cref="JsonbLimits.MaxNestingDepth"/> when it was
/// parsed or built. The skip, the hash and the text writer are iterative.</para>
///
/// <para>Two PostgreSQL quirks are copied on purpose, because applications can see them: a top-level
/// scalar behaves as a one-element array for <c>-&gt; int</c>, <c>?</c> and <c>@&gt;</c> (PostgreSQL
/// stores it so), and an empty top-level array sorts below every top-level scalar, <c>null</c>
/// included.</para>
/// </summary>
internal static class JsonbDocument
{
    /// <summary>The CBOR item of a cell, after a check of the format version.</summary>
    public static ReadOnlySpan<byte> Item(byte[]? cell)
    {
        if (cell is null || cell.Length < 2)
            throw JsonbCbor.Corrupt("empty cell");

        if (cell[0] != JsonbLimits.FormatVersion)
            throw JsonbCbor.Corrupt($"unknown format version {cell[0]}");

        return cell.AsSpan(1);
    }

    public static JsonbKind Kind(ReadOnlySpan<byte> item) => JsonbCbor.KindAt(item, 0);

    /// <summary>The PostgreSQL <c>jsonb_typeof</c> name of a kind.</summary>
    public static string TypeName(JsonbKind kind) => kind switch
    {
        JsonbKind.Null => "null",
        JsonbKind.String => "string",
        JsonbKind.Number => "number",
        JsonbKind.Boolean => "boolean",
        JsonbKind.Array => "array",
        _ => "object",
    };

    /// <summary>The element count of an array, or -1 for any other kind.</summary>
    public static int ArrayLength(ReadOnlySpan<byte> item)
    {
        if (Kind(item) != JsonbKind.Array)
            return -1;

        int pos = 0;
        return JsonbCbor.ReadContainerHeader(item, ref pos, out _);
    }

    /// <summary>
    /// The value of <paramref name="key"/> in a top-level object. Keys are sorted shorter first, then
    /// by bytes, so the search stops at the first key that sorts after <paramref name="key"/>.
    /// </summary>
    public static bool TryGetField(ReadOnlySpan<byte> item, ReadOnlySpan<byte> key, out Range value)
    {
        value = default;
        if (Kind(item) != JsonbKind.Object)
            return false;

        int pos = 0;
        int count = JsonbCbor.ReadContainerHeader(item, ref pos, out _);
        for (int i = 0; i < count; i++)
        {
            ReadOnlySpan<byte> candidate = JsonbCbor.ReadText(item, ref pos);
            int valueEnd = JsonbCbor.SkipItem(item, pos);

            int order = CompareKeyOrder(candidate, key);
            if (order == 0)
            {
                value = pos..valueEnd;
                return true;
            }

            if (order > 0)
                return false;

            pos = valueEnd;
        }

        return false;
    }

    public static bool TryGetField(ReadOnlySpan<byte> item, string key, out Range value) =>
        TryGetField(item, Encoding.UTF8.GetBytes(key), out value);

    /// <summary>The order the canonical writer sorts keys in: shorter first, then by bytes.</summary>
    private static int CompareKeyOrder(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) =>
        a.Length != b.Length ? a.Length.CompareTo(b.Length) : a.SequenceCompareTo(b);

    /// <summary>
    /// Element <paramref name="index"/> of a top-level array; a negative index counts from the end.
    /// A top-level scalar acts as a one-element array, as in PostgreSQL (<c>'"a"' -&gt; 0</c> is
    /// <c>"a"</c>).
    /// </summary>
    public static bool TryGetElement(ReadOnlySpan<byte> item, long index, out Range value)
    {
        value = default;
        JsonbKind kind = Kind(item);
        if (kind == JsonbKind.Object)
            return false;

        if (kind != JsonbKind.Array)
        {
            if (index is 0 or -1)
            {
                value = 0..item.Length;
                return true;
            }
            return false;
        }

        int pos = 0;
        int count = JsonbCbor.ReadContainerHeader(item, ref pos, out _);
        if (index < 0)
            index += count;
        if (index < 0 || index >= count)
            return false;

        for (int i = 0; i < index; i++)
            pos = JsonbCbor.SkipItem(item, pos);

        value = pos..JsonbCbor.SkipItem(item, pos);
        return true;
    }

    /// <summary>
    /// The value at a path, as <c>#&gt;</c> reads it: each step is a key of an object or an integer
    /// index of an array (negative counts from the end). A step that does not match gives no value.
    /// </summary>
    public static bool TryGetPath(ReadOnlySpan<byte> item, IReadOnlyList<string> path, out Range value)
    {
        int start = 0;
        int end = item.Length;

        foreach (string step in path)
        {
            ReadOnlySpan<byte> current = item[start..end];
            Range inner;
            switch (Kind(current))
            {
                case JsonbKind.Object:
                    if (!TryGetField(current, step, out inner))
                    {
                        value = default;
                        return false;
                    }
                    break;

                case JsonbKind.Array:
                    if (!long.TryParse(step, System.Globalization.NumberStyles.AllowLeadingSign,
                            System.Globalization.CultureInfo.InvariantCulture, out long index) ||
                        !TryGetElement(current, index, out inner))
                    {
                        value = default;
                        return false;
                    }
                    break;

                default:
                    value = default;
                    return false;
            }

            (int offset, int length) = inner.GetOffsetAndLength(current.Length);
            start += offset;
            end = start + length;
        }

        value = start..end;
        return true;
    }

    // ---------------------------------------------------------------------------------------------
    // Comparison and hashing
    // ---------------------------------------------------------------------------------------------

    /// <summary>
    /// The PostgreSQL <c>jsonb</c> order: by kind (<c>Null &lt; String &lt; Number &lt; Boolean &lt;
    /// Array &lt; Object</c>), then an object with more pairs or an array with more elements is greater,
    /// then the items compare in stored order (an object key, then its value). Strings compare by code
    /// point (their UTF-8 bytes), numbers by value. An empty top-level array sorts below every top-level
    /// scalar.
    /// </summary>
    public static int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        JsonbKind kindA = Kind(a);
        JsonbKind kindB = Kind(b);

        // PostgreSQL stores a top-level scalar as a one-element array and compares element counts
        // first, so an empty top-level array sorts below every top-level scalar, null included.
        if (kindA != kindB)
        {
            if (kindA == JsonbKind.Array && kindB < JsonbKind.Array && ArrayLength(a) == 0)
                return -1;
            if (kindB == JsonbKind.Array && kindA < JsonbKind.Array && ArrayLength(b) == 0)
                return 1;
        }

        int posA = 0;
        int posB = 0;
        return CompareAt(a, ref posA, b, ref posB, 0);
    }

    private static int CompareAt(ReadOnlySpan<byte> a, ref int posA, ReadOnlySpan<byte> b, ref int posB, int depth)
    {
        if (depth > JsonbLimits.MaxNestingDepth)
            throw JsonbEncoder.TooDeep();

        JsonbKind kindA = JsonbCbor.KindAt(a, posA);
        JsonbKind kindB = JsonbCbor.KindAt(b, posB);
        if (kindA != kindB)
            return ((byte)kindA).CompareTo((byte)kindB);

        switch (kindA)
        {
            case JsonbKind.Null:
                JsonbCbor.ReadHeader(a, ref posA, out _, out _);
                JsonbCbor.ReadHeader(b, ref posB, out _, out _);
                return 0;

            case JsonbKind.Boolean:
                return JsonbCbor.ReadBoolean(a, ref posA).CompareTo(JsonbCbor.ReadBoolean(b, ref posB));

            case JsonbKind.String:
                return JsonbCbor.ReadText(a, ref posA).SequenceCompareTo(JsonbCbor.ReadText(b, ref posB));

            case JsonbKind.Number:
                return CompareNumbers(a, ref posA, b, ref posB);

            default:
            {
                int countA = JsonbCbor.ReadContainerHeader(a, ref posA, out bool isObject);
                int countB = JsonbCbor.ReadContainerHeader(b, ref posB, out _);
                if (countA != countB)
                {
                    posA = SkipItems(a, posA, isObject ? countA * 2 : countA);
                    posB = SkipItems(b, posB, isObject ? countB * 2 : countB);
                    return countA.CompareTo(countB);
                }

                int items = isObject ? countA * 2 : countA;
                int result = 0;
                for (int i = 0; i < items; i++)
                {
                    if (result != 0)
                    {
                        posA = JsonbCbor.SkipItem(a, posA);
                        posB = JsonbCbor.SkipItem(b, posB);
                        continue;
                    }

                    result = CompareAt(a, ref posA, b, ref posB, depth + 1);
                }

                return result;
            }
        }
    }

    private static int SkipItems(ReadOnlySpan<byte> data, int pos, int count)
    {
        for (int i = 0; i < count; i++)
            pos = JsonbCbor.SkipItem(data, pos);
        return pos;
    }

    private static int CompareNumbers(ReadOnlySpan<byte> a, ref int posA, ReadOnlySpan<byte> b, ref int posB)
    {
        if (JsonbCbor.TryReadSmallInteger(a, posA, out long smallA, out int endA) &&
            JsonbCbor.TryReadSmallInteger(b, posB, out long smallB, out int endB))
        {
            posA = endA;
            posB = endB;
            return smallA.CompareTo(smallB);
        }

        JsonbNumber numberA = JsonbCbor.ReadNumber(a, ref posA);
        JsonbNumber numberB = JsonbCbor.ReadNumber(b, ref posB);
        return JsonbNumber.Compare(numberA, numberB);
    }

    /// <summary>
    /// Equality under <see cref="Compare"/>. Equal bytes are equal values, so that check comes first;
    /// different bytes can still be equal values when only a number's scale differs (<c>1.0 = 1</c>).
    /// </summary>
    public static bool ValueEquals(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) =>
        a.SequenceEqual(b) || Compare(a, b) == 0;

    /// <summary>
    /// Adds a hash that agrees with <see cref="ValueEquals"/>: the structure, the key and string bytes,
    /// and each number by value (trailing fraction zeros stripped). One iterative pass in byte order.
    /// </summary>
    public static void AddHash(ref HashCode hash, ReadOnlySpan<byte> item)
    {
        int pos = 0;
        long pending = 1;
        while (pending > 0)
        {
            pending--;
            JsonbKind kind = JsonbCbor.KindAt(item, pos);
            hash.Add((byte)kind);

            switch (kind)
            {
                case JsonbKind.Array:
                case JsonbKind.Object:
                {
                    int count = JsonbCbor.ReadContainerHeader(item, ref pos, out bool isObject);
                    hash.Add(count);
                    pending += isObject ? 2L * count : count;
                    break;
                }

                case JsonbKind.String:
                    hash.AddBytes(JsonbCbor.ReadText(item, ref pos));
                    break;

                case JsonbKind.Boolean:
                    hash.Add(JsonbCbor.ReadBoolean(item, ref pos));
                    break;

                case JsonbKind.Number:
                    if (JsonbCbor.TryReadSmallInteger(item, pos, out long small, out int end))
                    {
                        JsonbNumber.AddIntegerHash(ref hash, small);
                        pos = end;
                    }
                    else
                    {
                        JsonbCbor.ReadNumber(item, ref pos).AddHash(ref hash);
                    }
                    break;

                default:
                    JsonbCbor.ReadHeader(item, ref pos, out _, out _);
                    break;
            }
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Containment and existence
    // ---------------------------------------------------------------------------------------------

    /// <summary>
    /// PostgreSQL <c>@&gt;</c>: <paramref name="container"/> contains <paramref name="contained"/>.
    /// <list type="bullet">
    /// <item>An object contains an object when every key of the right side is on the left side and
    /// its value is contained there. Two scalars must be equal; a scalar never matches a container.</item>
    /// <item>An array contains an array when every element of the right side is in the left side: a
    /// scalar equal to a scalar element, a container contained in some element of the same kind.
    /// Order and repeats do not matter.</item>
    /// <item>At the top level a scalar acts as a one-element array, so <c>'["a"]' @&gt; '"a"'</c> is
    /// true. A top-level scalar on the left never contains an array (<c>'"a"' @&gt; '["a"]'</c> is
    /// false), as in PostgreSQL.</item>
    /// </list>
    /// </summary>
    public static bool Contains(ReadOnlySpan<byte> container, ReadOnlySpan<byte> contained)
    {
        JsonbKind left = Kind(container);
        JsonbKind right = Kind(contained);

        if ((left == JsonbKind.Object) != (right == JsonbKind.Object))
            return false;

        if (left == JsonbKind.Object)
            return ObjectContains(container, 0, contained, 0, 0);

        bool leftScalar = left != JsonbKind.Array;
        bool rightScalar = right != JsonbKind.Array;

        if (leftScalar && !rightScalar)
            return false;

        if (leftScalar)
            return ScalarEquals(container, 0, contained, 0);

        if (rightScalar)
            return ArrayHasScalar(container, 0, contained, 0);

        return ArrayContains(container, 0, contained, 0, 0);
    }

    private static bool ValueContains(ReadOnlySpan<byte> a, int posA, ReadOnlySpan<byte> b, int posB, int depth)
    {
        if (depth > JsonbLimits.MaxNestingDepth)
            throw JsonbEncoder.TooDeep();

        JsonbKind kindA = JsonbCbor.KindAt(a, posA);
        JsonbKind kindB = JsonbCbor.KindAt(b, posB);
        if (kindA != kindB)
            return false;

        return kindA switch
        {
            JsonbKind.Object => ObjectContains(a, posA, b, posB, depth),
            JsonbKind.Array => ArrayContains(a, posA, b, posB, depth),
            _ => ScalarEquals(a, posA, b, posB),
        };
    }

    /// <summary>
    /// Object containment as one merge walk over the two key lists. Both objects are canonical, so
    /// their keys are unique and sorted in <see cref="CompareKeyOrder"/>. The walk reads each key of
    /// each side at most once, so the cost is linear in the two objects, not one search of the left
    /// object per key of the right object.
    /// </summary>
    private static bool ObjectContains(ReadOnlySpan<byte> a, int posA, ReadOnlySpan<byte> b, int posB, int depth)
    {
        int remainingA = JsonbCbor.ReadContainerHeader(a, ref posA, out _);
        int countB = JsonbCbor.ReadContainerHeader(b, ref posB, out _);

        // Keys are unique, so a right side with more keys always has a key the left side lacks.
        if (countB > remainingA)
            return false;

        for (int i = 0; i < countB; i++)
        {
            ReadOnlySpan<byte> key = JsonbCbor.ReadText(b, ref posB);

            // Move the left cursor to the first key that does not sort before this key.
            while (true)
            {
                if (remainingA == 0)
                    return false;

                ReadOnlySpan<byte> keyA = JsonbCbor.ReadText(a, ref posA);
                remainingA--;

                int order = CompareKeyOrder(keyA, key);
                if (order == 0)
                    break;

                if (order > 0)
                    return false;

                posA = JsonbCbor.SkipItem(a, posA);
            }

            if (!ValueContains(a, posA, b, posB, depth + 1))
                return false;

            posA = JsonbCbor.SkipItem(a, posA);
            posB = JsonbCbor.SkipItem(b, posB);
        }

        return true;
    }

    /// <summary>
    /// When both arrays are at least this wide, a scalar element of the right side is found through a
    /// hash index of the left side's scalar elements instead of one scan of the left side each.
    /// </summary>
    private const int ScalarIndexThreshold = 16;

    private static bool ArrayContains(ReadOnlySpan<byte> a, int posA, ReadOnlySpan<byte> b, int posB, int depth)
    {
        int countPos = posA;
        int countA = JsonbCbor.ReadContainerHeader(a, ref countPos, out _);

        int countB = JsonbCbor.ReadContainerHeader(b, ref posB, out _);
        Dictionary<int, List<int>>? scalarIndex = null;
        bool indexBuilt = false;

        for (int i = 0; i < countB; i++)
        {
            JsonbKind kindB = JsonbCbor.KindAt(b, posB);
            bool found;
            if (kindB is JsonbKind.Array or JsonbKind.Object)
            {
                found = ArrayHasContainer(a, posA, b, posB, depth);
            }
            else if (countA >= ScalarIndexThreshold && countB >= ScalarIndexThreshold)
            {
                if (!indexBuilt)
                {
                    scalarIndex = IndexScalars(a, posA);
                    indexBuilt = true;
                }

                found = IndexHasScalar(scalarIndex!, a, b, posB);
            }
            else
            {
                found = ArrayHasScalar(a, posA, b, posB);
            }

            if (!found)
                return false;

            posB = JsonbCbor.SkipItem(b, posB);
        }

        return true;
    }

    /// <summary>
    /// The positions of the scalar elements of the array at <paramref name="posA"/>, grouped by a hash
    /// that agrees with <see cref="ValueEquals"/> (so <c>1.0</c> and <c>1</c> fall in one group).
    /// </summary>
    private static Dictionary<int, List<int>> IndexScalars(ReadOnlySpan<byte> a, int posA)
    {
        int countA = JsonbCbor.ReadContainerHeader(a, ref posA, out _);
        Dictionary<int, List<int>> index = new(countA);
        for (int i = 0; i < countA; i++)
        {
            int end = JsonbCbor.SkipItem(a, posA);
            if (JsonbCbor.KindAt(a, posA) is not (JsonbKind.Array or JsonbKind.Object))
            {
                int hash = ScalarHash(a[posA..end]);
                if (!index.TryGetValue(hash, out List<int>? positions))
                    index[hash] = positions = new List<int>(1);
                positions.Add(posA);
            }

            posA = end;
        }

        return index;
    }

    private static bool IndexHasScalar(Dictionary<int, List<int>> index, ReadOnlySpan<byte> a, ReadOnlySpan<byte> b, int posB)
    {
        int endB = JsonbCbor.SkipItem(b, posB);
        if (!index.TryGetValue(ScalarHash(b[posB..endB]), out List<int>? positions))
            return false;

        foreach (int position in positions)
            if (ScalarEquals(a, position, b, posB))
                return true;

        return false;
    }

    private static int ScalarHash(ReadOnlySpan<byte> scalar)
    {
        HashCode hash = new();
        AddHash(ref hash, scalar);
        return hash.ToHashCode();
    }

    private static bool ArrayHasScalar(ReadOnlySpan<byte> a, int posA, ReadOnlySpan<byte> b, int posB)
    {
        int countA = JsonbCbor.ReadContainerHeader(a, ref posA, out _);
        for (int i = 0; i < countA; i++)
        {
            JsonbKind kindA = JsonbCbor.KindAt(a, posA);
            if (kindA is not (JsonbKind.Array or JsonbKind.Object) && ScalarEquals(a, posA, b, posB))
                return true;

            posA = JsonbCbor.SkipItem(a, posA);
        }

        return false;
    }

    private static bool ArrayHasContainer(ReadOnlySpan<byte> a, int posA, ReadOnlySpan<byte> b, int posB, int depth)
    {
        JsonbKind kindB = JsonbCbor.KindAt(b, posB);
        int countA = JsonbCbor.ReadContainerHeader(a, ref posA, out _);
        for (int i = 0; i < countA; i++)
        {
            if (JsonbCbor.KindAt(a, posA) == kindB && ValueContains(a, posA, b, posB, depth + 1))
                return true;

            posA = JsonbCbor.SkipItem(a, posA);
        }

        return false;
    }

    private static bool ScalarEquals(ReadOnlySpan<byte> a, int posA, ReadOnlySpan<byte> b, int posB)
    {
        JsonbKind kindA = JsonbCbor.KindAt(a, posA);
        if (kindA != JsonbCbor.KindAt(b, posB))
            return false;

        return CompareAt(a, ref posA, b, ref posB, 0) == 0;
    }

    /// <summary>
    /// PostgreSQL <c>?</c>: <paramref name="key"/> is a key of a top-level object, a string element of
    /// a top-level array, or the value of a top-level string.
    /// </summary>
    public static bool Exists(ReadOnlySpan<byte> item, ReadOnlySpan<byte> key)
    {
        switch (Kind(item))
        {
            case JsonbKind.Object:
                return TryGetField(item, key, out _);

            case JsonbKind.Array:
            {
                int pos = 0;
                int count = JsonbCbor.ReadContainerHeader(item, ref pos, out _);
                for (int i = 0; i < count; i++)
                {
                    if (JsonbCbor.KindAt(item, pos) == JsonbKind.String)
                    {
                        int cursor = pos;
                        if (JsonbCbor.ReadText(item, ref cursor).SequenceEqual(key))
                            return true;
                    }

                    pos = JsonbCbor.SkipItem(item, pos);
                }

                return false;
            }

            case JsonbKind.String:
            {
                int pos = 0;
                return JsonbCbor.ReadText(item, ref pos).SequenceEqual(key);
            }

            default:
                return false;
        }
    }

    /// <summary>The nesting depth of an item (a scalar is 0), measured iteratively.</summary>
    public static int Depth(ReadOnlySpan<byte> item)
    {
        // Each entry is the number of items still to read in one open container.
        List<long> open = [];
        int pos = 0;
        int max = 0;
        long pendingAtRoot = 1;

        while (true)
        {
            if (open.Count == 0)
            {
                if (pendingAtRoot == 0)
                    return max;
                pendingAtRoot--;
            }
            else
            {
                if (open[^1] == 0)
                {
                    open.RemoveAt(open.Count - 1);
                    continue;
                }
                open[^1]--;
            }

            JsonbKind kind = JsonbCbor.KindAt(item, pos);
            if (kind is JsonbKind.Array or JsonbKind.Object)
            {
                int count = JsonbCbor.ReadContainerHeader(item, ref pos, out bool isObject);
                open.Add(isObject ? 2L * count : count);
                if (open.Count > max)
                    max = open.Count;
            }
            else
            {
                pos = JsonbCbor.SkipItem(item, pos);
            }
        }
    }

    /// <summary>The UTF-8 bytes of a top-level string item.</summary>
    public static ReadOnlySpan<byte> StringBytes(ReadOnlySpan<byte> item)
    {
        int pos = 0;
        return JsonbCbor.ReadText(item, ref pos);
    }

    /// <summary>
    /// The text <c>-&gt;&gt;</c> and <c>#&gt;&gt;</c> give for a value: a string unquoted and
    /// unescaped, SQL NULL for a JSON null, and the JSON text for anything else.
    /// </summary>
    public static string? ToSqlText(ReadOnlySpan<byte> item) => Kind(item) switch
    {
        JsonbKind.Null => null,
        JsonbKind.String => Encoding.UTF8.GetString(StringBytes(item)),
        _ => JsonbText.ToText(item),
    };
}
