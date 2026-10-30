/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.SQLParser;

/// <summary>
/// Builds and checks the nodes of a window call, <c>fn(args) OVER (PARTITION BY … ORDER BY … frame)</c>
/// (<see cref="NodeType.ExprWindowCall"/>, <see cref="NodeType.WindowSpec"/>,
/// <see cref="NodeType.WindowFrame"/>, <see cref="NodeType.WindowFrameBound"/>).
///
/// <para><b>Why the words are checked here.</b> Only <c>OVER</c> is a reserved word. PARTITION, ROWS,
/// RANGE, GROUPS, UNBOUNDED, PRECEDING, FOLLOWING, CURRENT, ROW and EXCLUDE reach the grammar as plain
/// identifiers, so columns and tables with those names keep working. The grammar therefore accepts any
/// word in those positions, and each builder must refuse every word but the one it expects.</para>
///
/// <para><b>Two kinds of error.</b> A wrong word is a syntax error (<see cref="CamusDBErrorCodes.SqlSyntaxError"/>).
/// A form that is valid SQL but that CamusDB does not run — a <c>GROUPS</c> frame, an <c>EXCLUDE</c>
/// clause, <c>RANGE</c> with an offset, a named window — is <see cref="CamusDBErrorCodes.InvalidInput"/>
/// with a message that names the form, so the caller learns what to change instead of reading an
/// unexpected-token message.</para>
///
/// <para>The frame bounds are checked against each other here as well, with the PostgreSQL rules: the
/// start may not be <c>UNBOUNDED FOLLOWING</c>, the end may not be <c>UNBOUNDED PRECEDING</c>, and the
/// start may not come after the end in the order
/// <c>UNBOUNDED PRECEDING &lt; n PRECEDING &lt; CURRENT ROW &lt; n FOLLOWING &lt; UNBOUNDED FOLLOWING</c>.
/// Two offsets of one direction are not compared: <c>ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING</c> is a
/// valid, empty frame, as in PostgreSQL.</para>
/// </summary>
internal static class WindowAst
{
    public const string UnboundedPreceding = "unbounded preceding";
    public const string Preceding = "preceding";
    public const string CurrentRow = "current row";
    public const string Following = "following";
    public const string UnboundedFollowing = "unbounded following";

    public const string Rows = "rows";
    public const string Range = "range";

    private static readonly NodeAst EmptySpecNode = new(NodeType.WindowSpec, null, null, null, null, null, null, null, null);

    /// <summary>Builds the window call over <paramref name="functionCall"/>; a null spec is <c>OVER ()</c>.</summary>
    public static NodeAst Call(NodeAst functionCall, NodeAst? spec) =>
        new(NodeType.ExprWindowCall, functionCall, spec ?? EmptySpecNode, null, null, null, null, null, null);

    public static NodeAst Spec(NodeAst? partition, NodeAst? order, NodeAst? frame) =>
        new(NodeType.WindowSpec, partition, order, frame, null, null, null, null, null);

    /// <summary>Checks the word before <c>BY</c> and returns the partition list.</summary>
    public static NodeAst Partition(string? word, NodeAst list)
    {
        if (!string.Equals(word, "partition", StringComparison.OrdinalIgnoreCase))
            throw new CamusDBException(
                CamusDBErrorCodes.SqlSyntaxError,
                $"Expected PARTITION BY in a window specification, got '{word} BY'");

        return list;
    }

    /// <summary>
    /// Builds a frame. <paramref name="end"/> is null for the one-bound form
    /// (<c>ROWS 2 PRECEDING</c>), whose end is the current row.
    /// </summary>
    public static NodeAst Frame(string? unit, NodeAst start, NodeAst? end)
    {
        string lowered = unit?.ToLowerInvariant() ?? "";

        if (lowered == "groups")
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "GROUPS frames are not supported in a window specification. Use ROWS or RANGE");

        if (lowered is not (Rows or Range))
            throw new CamusDBException(
                CamusDBErrorCodes.SqlSyntaxError,
                $"Expected ROWS or RANGE to start a window frame, got '{unit}'");

        if (lowered == Range && (HasOffset(start) || (end is not null && HasOffset(end))))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "RANGE with an offset (n PRECEDING or n FOLLOWING) is not supported. Use ROWS with an offset, "
                + "or RANGE with UNBOUNDED PRECEDING, CURRENT ROW or UNBOUNDED FOLLOWING");

        if (lowered == Rows)
        {
            RequireRowsOffset(start);

            if (end is not null)
                RequireRowsOffset(end);
        }

        if (start.yytext == UnboundedFollowing)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "A window frame cannot start at UNBOUNDED FOLLOWING");

        if (end is not null && end.yytext == UnboundedPreceding)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "A window frame cannot end at UNBOUNDED PRECEDING");

        // The one-bound form ends at the current row, so a start after it is the same error.
        int endRank = end is null ? Rank(CurrentRow) : Rank(end.yytext);

        if (Rank(start.yytext) > endRank)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                end is null
                    ? $"A window frame that starts at {Describe(start)} must name its end with BETWEEN … AND …"
                    : $"A window frame cannot start at {Describe(start)} and end at {Describe(end)}");

        return new NodeAst(NodeType.WindowFrame, start, end, null, null, null, null, null, lowered);
    }

    /// <summary>Builds <c>UNBOUNDED PRECEDING</c>, <c>UNBOUNDED FOLLOWING</c> or <c>CURRENT ROW</c>.</summary>
    public static NodeAst Bound(string? first, string? second)
    {
        string text = string.Concat(first?.ToLowerInvariant(), " ", second?.ToLowerInvariant());

        if (text is not (UnboundedPreceding or UnboundedFollowing or CurrentRow))
            throw new CamusDBException(
                CamusDBErrorCodes.SqlSyntaxError,
                $"Expected UNBOUNDED PRECEDING, UNBOUNDED FOLLOWING, CURRENT ROW, n PRECEDING or n FOLLOWING "
                + $"in a window frame, got '{first} {second}'");

        return new NodeAst(NodeType.WindowFrameBound, null, null, null, null, null, null, null, text);
    }

    /// <summary>Builds <c>n PRECEDING</c> or <c>n FOLLOWING</c>.</summary>
    public static NodeAst OffsetBound(NodeAst offset, string? direction)
    {
        string lowered = direction?.ToLowerInvariant() ?? "";

        if (lowered is not (Preceding or Following))
            throw new CamusDBException(
                CamusDBErrorCodes.SqlSyntaxError,
                $"Expected PRECEDING or FOLLOWING after a window frame offset, got '{direction}'");

        return new NodeAst(NodeType.WindowFrameBound, offset, null, null, null, null, null, null, lowered);
    }

    /// <summary>Refuses <c>EXCLUDE …</c> after a frame by name, and any other word there as a syntax error.</summary>
    public static NodeAst RefuseExclude(string? word)
    {
        if (string.Equals(word, "exclude", StringComparison.OrdinalIgnoreCase))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "EXCLUDE is not supported in a window frame");

        throw new CamusDBException(
            CamusDBErrorCodes.SqlSyntaxError,
            $"Unexpected '{word}' after a window frame");
    }

    /// <summary>Refuses <c>OVER name</c>, which needs a WINDOW clause.</summary>
    public static NodeAst RefuseNamedWindow(NodeAst name) =>
        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            $"Named windows (OVER {name.yytext}) and the WINDOW clause are not supported. Write the window "
            + "specification in parentheses after OVER");

    private static bool HasOffset(NodeAst bound) => bound.yytext is Preceding or Following;

    /// <summary>
    /// A ROWS offset counts rows, so it must be a non-negative integer. A placeholder is accepted here
    /// and its value is checked when the window runs.
    /// </summary>
    private static void RequireRowsOffset(NodeAst bound)
    {
        if (!HasOffset(bound) || bound.leftAst is not { } offset)
            return;

        if (offset.nodeType == NodeType.Placeholder)
            return;

        if (offset.nodeType != NodeType.Integer
            || !long.TryParse(offset.yytext, System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out long value)
            || value < 0)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"A ROWS frame offset must be a non-negative integer, got '{offset.yytext}'");
        }
    }

    private static int Rank(string? bound) => bound switch
    {
        UnboundedPreceding => 0,
        Preceding => 1,
        CurrentRow => 2,
        Following => 3,
        _ => 4,
    };

    private static string Describe(NodeAst bound) => bound.yytext switch
    {
        Preceding => "n PRECEDING",
        Following => "n FOLLOWING",
        { } text => text.ToUpperInvariant(),
        _ => "?",
    };
}
