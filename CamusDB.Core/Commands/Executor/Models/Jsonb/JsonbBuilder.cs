/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Formats.Cbor;
using System.Globalization;
using System.Text;

namespace CamusDB.Core.CommandsExecutor.Models.Jsonb;

/// <summary>
/// Builds new <c>jsonb</c> documents from existing ones, with the PostgreSQL rules of <c>||</c>,
/// <c>-</c>, <c>#-</c>, <c>jsonb_set</c>, <c>jsonb_insert</c>, <c>jsonb_strip_nulls</c> and the
/// constructors.
///
/// <para>A change rebuilds only the containers on its path. Every other subtree is copied as its
/// encoded bytes (<see cref="CborWriter.WriteEncodedValue"/>), which is valid because every nested
/// item of a canonical document is itself a canonical item. The canonical writer sorts the keys of
/// each rebuilt object again, so the result is canonical too.</para>
///
/// <para>A result can nest deeper than its inputs (<c>1 || [[...]]</c> wraps), so
/// <see cref="Finish(CborWriter)"/> checks the depth limit before the cell leaves this class. The
/// rebuild recurses once per path step, which is bounded by the depth limit of the input.</para>
/// </summary>
internal static class JsonbBuilder
{
    /// <summary>The cell for what <paramref name="writer"/> holds, after the depth check.</summary>
    public static byte[] Finish(CborWriter writer)
    {
        byte[] cell = JsonbEncoder.ToCell(writer);
        if (JsonbDocument.Depth(cell.AsSpan(1)) > JsonbLimits.MaxNestingDepth)
            throw JsonbEncoder.TooDeep();
        return cell;
    }

    private static byte[] Finish(byte[] item)
    {
        if (JsonbDocument.Depth(item) > JsonbLimits.MaxNestingDepth)
            throw JsonbEncoder.TooDeep();
        return JsonbEncoder.ToCell(item);
    }

    private static CamusDBException Error(string message) => new(CamusDBErrorCodes.InvalidInput, message);

    // ---------------------------------------------------------------------------------------------
    // Constructors
    // ---------------------------------------------------------------------------------------------

    /// <summary>An object from key and item pairs; a repeated key keeps its last value.</summary>
    public static byte[] Object(IReadOnlyList<KeyValuePair<string, byte[]>> pairs)
    {
        Dictionary<string, int> last = new(StringComparer.Ordinal);
        for (int i = 0; i < pairs.Count; i++)
            last[JsonbEncoder.CheckString(pairs[i].Key)] = i;

        CborWriter writer = JsonbEncoder.NewWriter();
        writer.WriteStartMap(last.Count);
        for (int i = 0; i < pairs.Count; i++)
        {
            if (last[pairs[i].Key] != i)
                continue;

            writer.WriteTextString(pairs[i].Key);
            writer.WriteEncodedValue(pairs[i].Value);
        }
        writer.WriteEndMap();
        return Finish(writer);
    }

    /// <summary>An array of the given items, in order.</summary>
    public static byte[] Array(IReadOnlyList<byte[]> items)
    {
        CborWriter writer = JsonbEncoder.NewWriter();
        writer.WriteStartArray(items.Count);
        foreach (byte[] item in items)
            writer.WriteEncodedValue(item);
        writer.WriteEndArray();
        return Finish(writer);
    }

    /// <summary>The item of a JSON string.</summary>
    public static byte[] StringItem(string value)
    {
        CborWriter writer = JsonbEncoder.NewWriter();
        writer.WriteTextString(JsonbEncoder.CheckString(value));
        return writer.Encode();
    }

    /// <summary>The item of an exact number.</summary>
    public static byte[] NumberItem(in JsonbNumber number)
    {
        CborWriter writer = JsonbEncoder.NewWriter();
        JsonbEncoder.WriteNumber(writer, number);
        return writer.Encode();
    }

    public static byte[] BooleanItem(bool value) => [value ? (byte)0xF5 : (byte)0xF4];

    public static byte[] NullItem() => [0xF6];

    // ---------------------------------------------------------------------------------------------
    // Reading helpers
    // ---------------------------------------------------------------------------------------------

    private readonly record struct Child(int KeyStart, int KeyEnd, int Start, int End);

    /// <summary>The children of the container item at offset 0: for an object, each key and value.</summary>
    private static List<Child> Children(ReadOnlySpan<byte> item, out bool isObject)
    {
        int pos = 0;
        int count = JsonbCbor.ReadContainerHeader(item, ref pos, out isObject);
        List<Child> children = new(count);
        for (int i = 0; i < count; i++)
        {
            int keyStart = pos;
            int keyEnd = pos;
            if (isObject)
            {
                JsonbCbor.ReadText(item, ref pos);
                keyEnd = pos;
            }

            int end = JsonbCbor.SkipItem(item, pos);
            children.Add(new Child(keyStart, keyEnd, pos, end));
            pos = end;
        }

        return children;
    }

    private static string KeyOf(ReadOnlySpan<byte> item, Child child)
    {
        int pos = child.KeyStart;
        return Encoding.UTF8.GetString(JsonbCbor.ReadText(item, ref pos));
    }

    private static bool IsContainer(ReadOnlySpan<byte> item) =>
        JsonbDocument.Kind(item) is JsonbKind.Array or JsonbKind.Object;

    // ---------------------------------------------------------------------------------------------
    // ||
    // ---------------------------------------------------------------------------------------------

    /// <summary>
    /// PostgreSQL <c>jsonb || jsonb</c>: two objects merge (a key of the right side wins); any other
    /// pair concatenates as arrays, where a non-array operand counts as a one-element array.
    /// </summary>
    public static byte[] Concat(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right)
    {
        JsonbKind leftKind = JsonbDocument.Kind(left);
        JsonbKind rightKind = JsonbDocument.Kind(right);
        CborWriter writer = JsonbEncoder.NewWriter();

        if (leftKind == JsonbKind.Object && rightKind == JsonbKind.Object)
        {
            List<Child> leftChildren = Children(left, out _);
            List<Child> rightChildren = Children(right, out _);

            HashSet<string> rightKeys = new(StringComparer.Ordinal);
            foreach (Child child in rightChildren)
                rightKeys.Add(KeyOf(right, child));

            int count = rightChildren.Count;
            foreach (Child child in leftChildren)
                if (!rightKeys.Contains(KeyOf(left, child)))
                    count++;

            writer.WriteStartMap(count);
            foreach (Child child in leftChildren)
            {
                if (rightKeys.Contains(KeyOf(left, child)))
                    continue;
                writer.WriteEncodedValue(left[child.KeyStart..child.KeyEnd]);
                writer.WriteEncodedValue(left[child.Start..child.End]);
            }
            foreach (Child child in rightChildren)
            {
                writer.WriteEncodedValue(right[child.KeyStart..child.KeyEnd]);
                writer.WriteEncodedValue(right[child.Start..child.End]);
            }
            writer.WriteEndMap();
            return Finish(writer);
        }

        int leftCount = leftKind == JsonbKind.Array ? JsonbDocument.ArrayLength(left) : 1;
        int rightCount = rightKind == JsonbKind.Array ? JsonbDocument.ArrayLength(right) : 1;
        writer.WriteStartArray(leftCount + rightCount);
        WriteAsElements(writer, left, leftKind);
        WriteAsElements(writer, right, rightKind);
        writer.WriteEndArray();
        return Finish(writer);
    }

    private static void WriteAsElements(CborWriter writer, ReadOnlySpan<byte> item, JsonbKind kind)
    {
        if (kind != JsonbKind.Array)
        {
            writer.WriteEncodedValue(item);
            return;
        }

        foreach (Child child in Children(item, out _))
            writer.WriteEncodedValue(item[child.Start..child.End]);
    }

    // ---------------------------------------------------------------------------------------------
    // - and #-
    // ---------------------------------------------------------------------------------------------

    /// <summary>
    /// PostgreSQL <c>jsonb - text</c> and <c>jsonb - text[]</c>: removes the keys from an object, or
    /// the string elements equal to a key from an array. A scalar is an error.
    /// </summary>
    public static byte[] DeleteKeys(ReadOnlySpan<byte> item, IReadOnlyCollection<string> keys)
    {
        JsonbKind kind = JsonbDocument.Kind(item);
        if (kind is not (JsonbKind.Array or JsonbKind.Object))
            throw Error("cannot delete from scalar");

        HashSet<string> remove = new(keys, StringComparer.Ordinal);
        List<Child> children = Children(item, out bool isObject);
        List<Child> kept = new(children.Count);
        foreach (Child child in children)
        {
            ReadOnlySpan<byte> value = item[child.Start..child.End];
            bool matches = isObject
                ? remove.Contains(KeyOf(item, child))
                : JsonbDocument.Kind(value) == JsonbKind.String && remove.Contains(Encoding.UTF8.GetString(JsonbDocument.StringBytes(value)));
            if (!matches)
                kept.Add(child);
        }

        return Rewrite(item, isObject, kept);
    }

    /// <summary>
    /// PostgreSQL <c>jsonb - integer</c>: removes one array element; a negative index counts from the
    /// end, and an index out of range changes nothing. An object or a scalar is an error.
    /// </summary>
    public static byte[] DeleteIndex(ReadOnlySpan<byte> item, long index)
    {
        JsonbKind kind = JsonbDocument.Kind(item);
        if (kind == JsonbKind.Object)
            throw Error("cannot delete from object using integer index");
        if (kind != JsonbKind.Array)
            throw Error("cannot delete from scalar");

        List<Child> children = Children(item, out _);
        if (index < 0)
            index += children.Count;
        if (index < 0 || index >= children.Count)
            return JsonbEncoder.ToCell(item);

        children.RemoveAt((int)index);
        return Rewrite(item, false, children);
    }

    /// <summary>
    /// PostgreSQL <c>#-</c>: removes the element at the path. A path that does not lead to an element
    /// changes nothing; a top-level scalar is an error.
    /// </summary>
    public static byte[] DeletePath(ReadOnlySpan<byte> item, IReadOnlyList<string> path)
    {
        if (!IsContainer(item))
            throw Error("cannot delete path in scalar");

        if (path.Count == 0)
            return JsonbEncoder.ToCell(item);

        byte[]? changed = DeleteAt(item, path, 0);
        return changed is null ? JsonbEncoder.ToCell(item) : Finish(changed);
    }

    /// <summary>The rebuilt item, or null when the path leads nowhere (nothing changes).</summary>
    private static byte[]? DeleteAt(ReadOnlySpan<byte> item, IReadOnlyList<string> path, int level)
    {
        // One frame per path step into the document. A valid document nests no deeper than the
        // limit, but the guard does not rely on that: a damaged cell must not overflow the stack.
        if (level > JsonbLimits.MaxNestingDepth)
            throw JsonbEncoder.TooDeep();

        List<Child> children = Children(item, out bool isObject);
        int found = FindStep(item, children, isObject, path[level], level, out _);
        if (found < 0)
            return null;

        if (level == path.Count - 1)
        {
            children.RemoveAt(found);
            return RewriteItem(item, isObject, children);
        }

        ReadOnlySpan<byte> child = item[children[found].Start..children[found].End];
        if (!IsContainer(child))
            return null;

        byte[]? inner = DeleteAt(child, path, level + 1);
        return inner is null ? null : ReplaceChild(item, isObject, children, found, inner);
    }

    // ---------------------------------------------------------------------------------------------
    // jsonb_set / jsonb_insert
    // ---------------------------------------------------------------------------------------------

    /// <summary>
    /// PostgreSQL <c>jsonb_set</c>: replaces the element at the path with <paramref name="value"/>,
    /// or adds it when the last step is missing and <paramref name="createMissing"/> is true (an
    /// array index past either end prepends or appends). A missing intermediate step changes
    /// nothing; a top-level scalar is an error.
    /// </summary>
    public static byte[] Set(ReadOnlySpan<byte> item, IReadOnlyList<string> path, ReadOnlySpan<byte> value, bool createMissing)
    {
        if (!IsContainer(item))
            throw Error("cannot set path in scalar");

        if (path.Count == 0)
            return JsonbEncoder.ToCell(item);

        byte[]? changed = SetAt(item, path, 0, value, createMissing ? Mode.SetCreate : Mode.Set);
        return changed is null ? JsonbEncoder.ToCell(item) : Finish(changed);
    }

    /// <summary>
    /// PostgreSQL <c>jsonb_insert</c>: inserts <paramref name="value"/> into an array before the
    /// element at the path (after it when <paramref name="insertAfter"/> is true), or adds a new key
    /// to an object. An existing key is an error.
    /// </summary>
    public static byte[] Insert(ReadOnlySpan<byte> item, IReadOnlyList<string> path, ReadOnlySpan<byte> value, bool insertAfter)
    {
        if (!IsContainer(item))
            throw Error("cannot set path in scalar");

        if (path.Count == 0)
            return JsonbEncoder.ToCell(item);

        byte[]? changed = SetAt(item, path, 0, value, insertAfter ? Mode.InsertAfter : Mode.InsertBefore);
        return changed is null ? JsonbEncoder.ToCell(item) : Finish(changed);
    }

    private enum Mode { Set, SetCreate, InsertBefore, InsertAfter }

    private static byte[]? SetAt(ReadOnlySpan<byte> item, IReadOnlyList<string> path, int level, ReadOnlySpan<byte> value, Mode mode)
    {
        // The same independent recursion bound as DeleteAt.
        if (level > JsonbLimits.MaxNestingDepth)
            throw JsonbEncoder.TooDeep();

        List<Child> children = Children(item, out bool isObject);
        bool last = level == path.Count - 1;
        int found = FindStep(item, children, isObject, path[level], level, out long index);

        if (!last)
        {
            if (found < 0)
                return null;

            ReadOnlySpan<byte> child = item[children[found].Start..children[found].End];
            if (!IsContainer(child))
                return null;

            byte[]? inner = SetAt(child, path, level + 1, value, mode);
            return inner is null ? null : ReplaceChild(item, isObject, children, found, inner);
        }

        if (isObject)
        {
            if (found >= 0)
            {
                if (mode is Mode.InsertBefore or Mode.InsertAfter)
                    throw Error("cannot replace existing key");
                return ReplaceChild(item, true, children, found, value.ToArray());
            }

            if (mode == Mode.Set)
                return null;

            return AddKey(item, children, path[level], value);
        }

        // An array at the last step.
        if (mode is Mode.Set or Mode.SetCreate)
        {
            if (found >= 0)
                return ReplaceChild(item, false, children, found, value.ToArray());
            if (mode == Mode.Set)
                return null;
            return InsertElement(item, children, index < 0 ? 0 : children.Count, value);
        }

        int position;
        if (found >= 0)
            position = mode == Mode.InsertAfter ? found + 1 : found;
        else
            position = index < 0 ? 0 : children.Count;

        return InsertElement(item, children, position, value);
    }

    /// <summary>
    /// Finds path step <paramref name="step"/> among the children: a key of an object, or an integer
    /// index of an array (negative counts from the end). Returns -1 when absent; for an array,
    /// <paramref name="index"/> tells which end an out-of-range index points past.
    /// </summary>
    private static int FindStep(ReadOnlySpan<byte> item, List<Child> children, bool isObject, string step, int level, out long index)
    {
        index = 0;
        if (isObject)
        {
            for (int i = 0; i < children.Count; i++)
                if (string.Equals(KeyOf(item, children[i]), step, StringComparison.Ordinal))
                    return i;
            return -1;
        }

        if (!long.TryParse(step.Trim(), NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out index))
            throw Error($"path element at position {level + 1} is not an integer: \"{step}\"");

        long resolved = index < 0 ? index + children.Count : index;
        return resolved >= 0 && resolved < children.Count ? (int)resolved : -1;
    }

    private static byte[] Rewrite(ReadOnlySpan<byte> item, bool isObject, List<Child> children) =>
        Finish(RewriteItem(item, isObject, children));

    private static byte[] RewriteItem(ReadOnlySpan<byte> item, bool isObject, List<Child> children)
    {
        CborWriter writer = JsonbEncoder.NewWriter();
        if (isObject)
            writer.WriteStartMap(children.Count);
        else
            writer.WriteStartArray(children.Count);

        foreach (Child child in children)
        {
            if (isObject)
                writer.WriteEncodedValue(item[child.KeyStart..child.KeyEnd]);
            writer.WriteEncodedValue(item[child.Start..child.End]);
        }

        if (isObject)
            writer.WriteEndMap();
        else
            writer.WriteEndArray();
        return writer.Encode();
    }

    private static byte[] ReplaceChild(ReadOnlySpan<byte> item, bool isObject, List<Child> children, int index, byte[] replacement)
    {
        CborWriter writer = JsonbEncoder.NewWriter();
        if (isObject)
            writer.WriteStartMap(children.Count);
        else
            writer.WriteStartArray(children.Count);

        for (int i = 0; i < children.Count; i++)
        {
            Child child = children[i];
            if (isObject)
                writer.WriteEncodedValue(item[child.KeyStart..child.KeyEnd]);
            if (i == index)
                writer.WriteEncodedValue(replacement);
            else
                writer.WriteEncodedValue(item[child.Start..child.End]);
        }

        if (isObject)
            writer.WriteEndMap();
        else
            writer.WriteEndArray();
        return writer.Encode();
    }

    private static byte[] AddKey(ReadOnlySpan<byte> item, List<Child> children, string key, ReadOnlySpan<byte> value)
    {
        CborWriter writer = JsonbEncoder.NewWriter();
        writer.WriteStartMap(children.Count + 1);
        foreach (Child child in children)
        {
            writer.WriteEncodedValue(item[child.KeyStart..child.KeyEnd]);
            writer.WriteEncodedValue(item[child.Start..child.End]);
        }
        writer.WriteTextString(JsonbEncoder.CheckString(key));
        writer.WriteEncodedValue(value);
        writer.WriteEndMap();
        return writer.Encode();
    }

    private static byte[] InsertElement(ReadOnlySpan<byte> item, List<Child> children, int position, ReadOnlySpan<byte> value)
    {
        CborWriter writer = JsonbEncoder.NewWriter();
        writer.WriteStartArray(children.Count + 1);
        for (int i = 0; i <= children.Count; i++)
        {
            if (i == position)
                writer.WriteEncodedValue(value);
            if (i < children.Count)
                writer.WriteEncodedValue(item[children[i].Start..children[i].End]);
        }
        writer.WriteEndArray();
        return writer.Encode();
    }

    // ---------------------------------------------------------------------------------------------
    // jsonb_strip_nulls
    // ---------------------------------------------------------------------------------------------

    /// <summary>
    /// PostgreSQL <c>jsonb_strip_nulls</c>: removes every object field whose value is JSON null, at
    /// every level. Null elements of arrays stay.
    /// </summary>
    public static byte[] StripNulls(ReadOnlySpan<byte> item) => Finish(StripNullsItem(item, 0));

    private static byte[] StripNullsItem(ReadOnlySpan<byte> item, int depth)
    {
        if (depth > JsonbLimits.MaxNestingDepth)
            throw JsonbEncoder.TooDeep();

        if (!IsContainer(item))
            return item.ToArray();

        List<Child> children = Children(item, out bool isObject);
        CborWriter writer = JsonbEncoder.NewWriter();

        if (isObject)
        {
            int count = 0;
            foreach (Child child in children)
                if (JsonbDocument.Kind(item[child.Start..child.End]) != JsonbKind.Null)
                    count++;

            writer.WriteStartMap(count);
            foreach (Child child in children)
            {
                ReadOnlySpan<byte> value = item[child.Start..child.End];
                if (JsonbDocument.Kind(value) == JsonbKind.Null)
                    continue;
                writer.WriteEncodedValue(item[child.KeyStart..child.KeyEnd]);
                writer.WriteEncodedValue(StripNullsItem(value, depth + 1));
            }
            writer.WriteEndMap();
        }
        else
        {
            writer.WriteStartArray(children.Count);
            foreach (Child child in children)
                writer.WriteEncodedValue(StripNullsItem(item[child.Start..child.End], depth + 1));
            writer.WriteEndArray();
        }

        return writer.Encode();
    }
}
