/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Numerics;
using System.Security.Cryptography;
using System.Text;

namespace CamusDB.Core.CommandsExecutor.Models.Jsonb;

/// <summary>
/// The entries of a GIN index over a jsonb column, and the entries a query needs.
///
/// <para><b>One entry per leaf.</b> An entry is the path from the document root to a leaf, then
/// <c>=</c>, then the leaf. A path step is <c>k&lt;hex&gt;;</c> for an object key (the key's UTF-8 bytes in
/// lower-case hex) or <c>[</c> for "an element of an array", with no position. A leaf is <c>n</c>
/// (null), <c>f</c> / <c>t</c>, <c>d&lt;number&gt;</c> (the number with trailing fraction zeros removed,
/// so 1.0 and 1 are one entry), <c>s&lt;hex&gt;</c> (a string), <c>o</c> (an empty object) or <c>a</c>
/// (an empty array). <c>{"a": {"b": 1}, "t": ["x"]}</c> has the entries <c>k61;k62;=d1</c> and
/// <c>k74;[=s78</c>. A key longer than 64 bytes, or a string or number longer than 128, is replaced by a
/// SHA-256 prefix (<c>K</c>, <c>S</c>, <c>D</c>), so an entry key stays short; a query computes the same
/// form. The alphabet is ASCII without <c>/</c>, so an entry is a valid index key segment, and
/// <see cref="PrefixUpperBound"/> (U+007F) sorts after every character of it.</para>
///
/// <para><b>Queries get a superset.</b> The entries lose array positions and structure, so a row whose
/// entries all match can still fail the operator (<c>[{"a":1},{"b":2}]</c> has every entry of
/// <c>[{"a":1,"b":2}]</c>). The planner always keeps the predicate as a filter, which checks each
/// candidate row. What a query asks for must therefore never miss a matching row; where an exact
/// entry cannot be named (an empty container on the right side of <c>@&gt;</c>), it asks for a path
/// prefix instead, which is a larger superset.</para>
/// </summary>
internal static class JsonbGinEntries
{
    /// <summary>A character after every character an entry uses: the end of a prefix range.</summary>
    public const char PrefixUpperBound = '\u007F';

    private const int MaxKeyBytes = 64;

    private const int MaxLeafChars = 128;

    /// <summary>One thing a row's entries must hold: an exact entry, or any entry that starts with a prefix.</summary>
    public readonly record struct Requirement(string Text, bool IsPrefix);

    /// <summary>
    /// The entries a query needs: every group must hold, and a group holds when any of its
    /// requirements holds (an AND of ORs).
    /// </summary>
    public sealed record Query(IReadOnlyList<IReadOnlyList<Requirement>> Groups);

    // ---------------------------------------------------------------------------------------------
    // The entries of a stored document
    // ---------------------------------------------------------------------------------------------

    /// <summary>The distinct entries of the document <paramref name="item"/> (a CBOR item).</summary>
    public static HashSet<string> Entries(ReadOnlySpan<byte> item)
    {
        HashSet<string> entries = new(StringComparer.Ordinal);
        Stack<(int Pos, string Path)> pending = new();
        pending.Push((0, ""));

        while (pending.Count > 0)
        {
            (int pos, string path) = pending.Pop();
            JsonbKind kind = JsonbCbor.KindAt(item, pos);

            if (kind is JsonbKind.Array or JsonbKind.Object)
            {
                int cursor = pos;
                int count = JsonbCbor.ReadContainerHeader(item, ref cursor, out bool isObject);
                if (count == 0)
                {
                    entries.Add(path + (isObject ? "=o" : "=a"));
                    continue;
                }

                for (int i = 0; i < count; i++)
                {
                    string childPath = path;
                    if (isObject)
                        childPath += KeyStep(JsonbCbor.ReadText(item, ref cursor));
                    else
                        childPath += "[";

                    pending.Push((cursor, childPath));
                    cursor = JsonbCbor.SkipItem(item, cursor);
                }
                continue;
            }

            entries.Add(path + "=" + Leaf(item, pos));
        }

        return entries;
    }

    private static string KeyStep(ReadOnlySpan<byte> key) =>
        key.Length <= MaxKeyBytes ? "k" + Convert.ToHexStringLower(key) + ";" : "K" + Hash(key) + ";";

    private static string Hash(ReadOnlySpan<byte> bytes) =>
        Convert.ToHexStringLower(SHA256.HashData(bytes))[..32];

    /// <summary>The leaf form of the scalar item at <paramref name="pos"/>.</summary>
    private static string Leaf(ReadOnlySpan<byte> item, int pos)
    {
        switch (JsonbCbor.KindAt(item, pos))
        {
            case JsonbKind.Null:
                return "n";

            case JsonbKind.Boolean:
            {
                int cursor = pos;
                return JsonbCbor.ReadBoolean(item, ref cursor) ? "t" : "f";
            }

            case JsonbKind.String:
            {
                int cursor = pos;
                return StringLeaf(JsonbCbor.ReadText(item, ref cursor));
            }

            default:
            {
                int cursor = pos;
                return NumberLeaf(JsonbCbor.ReadNumber(item, ref cursor));
            }
        }
    }

    private static string StringLeaf(ReadOnlySpan<byte> utf8) =>
        utf8.Length * 2 <= MaxLeafChars ? "s" + Convert.ToHexStringLower(utf8) : "S" + Hash(utf8);

    private static string NumberLeaf(JsonbNumber number)
    {
        BigInteger mantissa = number.Mantissa;
        int scale = number.Scale;
        while (scale > 0 && !mantissa.IsZero)
        {
            BigInteger quotient = BigInteger.DivRem(mantissa, 10, out BigInteger remainder);
            if (!remainder.IsZero)
                break;
            mantissa = quotient;
            scale--;
        }

        if (mantissa.IsZero)
            scale = 0;

        string text = new JsonbNumber(mantissa, scale).ToString();
        return text.Length <= MaxLeafChars ? "d" + text : "D" + Hash(Encoding.ASCII.GetBytes(text));
    }

    // ---------------------------------------------------------------------------------------------
    // What a query needs
    // ---------------------------------------------------------------------------------------------

    /// <summary>
    /// The entries a row needs for <c>col @&gt; <paramref name="item"/></c>, or null when the index
    /// cannot narrow the scan (the right side is a top-level scalar, <c>{}</c> or <c>[]</c>).
    /// </summary>
    public static Query? Contains(ReadOnlySpan<byte> item)
    {
        JsonbKind kind = JsonbCbor.KindAt(item, 0);
        if (kind is not (JsonbKind.Array or JsonbKind.Object))
            return null;

        List<IReadOnlyList<Requirement>> groups = [];
        Stack<(int Pos, string Path)> pending = new();
        pending.Push((0, ""));

        while (pending.Count > 0)
        {
            (int pos, string path) = pending.Pop();
            JsonbKind childKind = JsonbCbor.KindAt(item, pos);

            if (childKind is JsonbKind.Array or JsonbKind.Object)
            {
                int cursor = pos;
                int count = JsonbCbor.ReadContainerHeader(item, ref cursor, out bool isObject);
                if (count == 0)
                {
                    // Any document with something at this path can contain an empty container
                    // here, so the requirement is the path itself. At the root it narrows nothing.
                    if (path.Length > 0)
                        groups.Add([new Requirement(path, IsPrefix: true)]);
                    continue;
                }

                for (int i = 0; i < count; i++)
                {
                    string childPath = path;
                    if (isObject)
                        childPath += KeyStep(JsonbCbor.ReadText(item, ref cursor));
                    else
                        childPath += "[";

                    pending.Push((cursor, childPath));
                    cursor = JsonbCbor.SkipItem(item, cursor);
                }
                continue;
            }

            groups.Add([new Requirement(path + "=" + Leaf(item, pos), IsPrefix: false)]);
        }

        return groups.Count == 0 ? null : new Query(groups);
    }

    /// <summary>
    /// The requirements of <c>col ? key</c>: the key at the top level of an object, or the string as an
    /// element of a top-level array or as a top-level string.
    /// </summary>
    public static IReadOnlyList<Requirement> Exists(string key)
    {
        byte[] utf8 = Encoding.UTF8.GetBytes(key);
        string leaf = StringLeaf(utf8);
        return
        [
            new Requirement(KeyStep(utf8), IsPrefix: true),
            new Requirement("[=" + leaf, IsPrefix: false),
            new Requirement("=" + leaf, IsPrefix: false),
        ];
    }

    /// <summary><c>col ?| keys</c> (any) or <c>col ?&amp; keys</c> (all); null for an empty key list.</summary>
    public static Query? ExistsMany(IReadOnlyList<string> keys, bool all)
    {
        if (keys.Count == 0)
            return null;

        if (all)
        {
            List<IReadOnlyList<Requirement>> groups = new(keys.Count);
            foreach (string key in keys)
                groups.Add(Exists(key));
            return new Query(groups);
        }

        List<Requirement> any = [];
        foreach (string key in keys)
            any.AddRange(Exists(key));
        return new Query([any]);
    }

    /// <summary>
    /// The requirements of <c>col -&gt;&gt; 'key' = text</c>: the value at the key is a string with that
    /// text, or a number or boolean whose text it is. Null when the text could be a container's text,
    /// which the entries cannot name.
    /// </summary>
    public static Query? FieldTextEquals(string key, string text)
    {
        if (text.Length > 0 && text[0] is '{' or '[')
            return null;

        string path = KeyStep(Encoding.UTF8.GetBytes(key));
        List<Requirement> any = [new Requirement(path + "=" + StringLeaf(Encoding.UTF8.GetBytes(text)), IsPrefix: false)];

        if (text is "true" or "false")
            any.Add(new Requirement(path + "=" + (text == "true" ? "t" : "f"), IsPrefix: false));

        try
        {
            JsonbNumber number = JsonbNumber.ParseJsonText(Encoding.ASCII.GetBytes(text));
            any.Add(new Requirement(path + "=" + NumberLeaf(number), IsPrefix: false));
        }
        catch (CamusDBException)
        {
            // Not the text of a number.
        }
        catch (ArgumentException)
        {
            // Not ASCII, so not the text of a number.
        }

        return new Query([any]);
    }

    /// <summary>
    /// The requirements of <c>col -&gt; 'key' = document</c>: the same as <c>col @&gt; {key: document}</c>,
    /// which equality implies.
    /// </summary>
    public static Query? FieldEquals(string key, ReadOnlySpan<byte> item)
    {
        Query? inner = Contains(item);
        string path = KeyStep(Encoding.UTF8.GetBytes(key));

        if (inner is null)
        {
            // A scalar or an empty container: the value at the key is exactly that.
            JsonbKind kind = JsonbCbor.KindAt(item, 0);
            if (kind is JsonbKind.Array or JsonbKind.Object)
                return new Query([[new Requirement(path, IsPrefix: true)]]);

            return new Query([[new Requirement(path + "=" + Leaf(item, 0), IsPrefix: false)]]);
        }

        List<IReadOnlyList<Requirement>> groups = new(inner.Groups.Count);
        foreach (IReadOnlyList<Requirement> group in inner.Groups)
        {
            List<Requirement> prefixed = new(group.Count);
            foreach (Requirement requirement in group)
                prefixed.Add(requirement with { Text = path + requirement.Text });
            groups.Add(prefixed);
        }
        return new Query(groups);
    }
}
