
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.SQLParser;

/// <summary>
/// The one owner of the words a <see cref="NodeType.Join"/> node stores in its <c>yytext</c> to say
/// which join form the user wrote.
///
/// <para>The grammar writes the word through <see cref="Write"/>, and every consumer reads it through
/// <see cref="Read"/>; no other code compares the strings. A null <c>yytext</c> reads as
/// <see cref="Kind.Inner"/>, so a join node built by code that predates the outer-join forms (an old
/// stored view body, a wire-decoded fragment, a test fixture) still means what it meant then.</para>
///
/// <para>The words are deliberately not the <c>JoinKind</c> enum of the query model: a right join and
/// a cross join exist only here, in the parse tree, and are rewritten into a left outer join or an
/// inner join when the logical query is created.</para>
/// </summary>
public static class JoinAstKind
{
    /// <summary>The join form as written, before any rewrite.</summary>
    public enum Kind
    {
        /// <summary><c>JOIN</c> or <c>INNER JOIN</c>.</summary>
        Inner,

        /// <summary><c>LEFT JOIN</c> or <c>LEFT OUTER JOIN</c>: every left row survives.</summary>
        Left,

        /// <summary><c>RIGHT JOIN</c> or <c>RIGHT OUTER JOIN</c>: every right row survives.</summary>
        Right,

        /// <summary><c>CROSS JOIN</c>: every pair, with no <c>ON</c> clause.</summary>
        Cross,
    }

    private const string InnerWord = "inner";
    private const string LeftWord = "left";
    private const string RightWord = "right";
    private const string CrossWord = "cross";

    /// <summary>Returns the word a join node of the given kind stores in its <c>yytext</c>.</summary>
    public static string Write(Kind kind) => kind switch
    {
        Kind.Inner => InnerWord,
        Kind.Left => LeftWord,
        Kind.Right => RightWord,
        Kind.Cross => CrossWord,
        _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, $"Unknown join kind {kind}"),
    };

    private const string FullWord = "full";

    private const string FullJoinMessage = "FULL [OUTER] JOIN is not supported; use LEFT JOIN or RIGHT JOIN";

    /// <summary>
    /// Refuses <c>FULL JOIN</c>. FULL is a plain identifier (the foreign-key <c>MATCH FULL</c> clause
    /// needs it), so <c>a FULL JOIN b</c> lexes as an inner join of a table aliased <c>full</c>
    /// and would silently drop the unmatched rows. The grammar calls this only for the bare
    /// <c>JOIN</c> keyword, with <paramref name="bareAlias"/> set to the alias of the table before
    /// it when that alias was written bare and unquoted, and null otherwise. So <c>a FULL JOIN b</c>
    /// is refused, while <c>a AS full JOIN b</c>, <c>a `full` JOIN b</c> and <c>a full INNER JOIN b</c>
    /// are the aliases they look like.
    /// </summary>
    public static void RejectFullJoin(string? bareAlias)
    {
        if (string.Equals(bareAlias, FullWord, StringComparison.OrdinalIgnoreCase))
            throw new CamusDBException(CamusDBErrorCodes.FeatureNotSupported, FullJoinMessage);
    }

    /// <summary>
    /// Called for <c>OUTER JOIN</c> with no side keyword before it, which is never valid: when the
    /// bare alias before it is <c>full</c> the user wrote <c>FULL OUTER JOIN</c>, refused as a
    /// missing feature; otherwise the statement is a syntax error that names the two accepted forms.
    /// </summary>
    public static void RejectOuterJoinWithoutSide(string? bareAlias)
    {
        RejectFullJoin(bareAlias);

        throw new CamusDBException(
            CamusDBErrorCodes.SqlSyntaxError,
            "OUTER JOIN must be written as LEFT OUTER JOIN or RIGHT OUTER JOIN");
    }

    /// <summary>
    /// Reads the join form of a <see cref="NodeType.Join"/> node. A null <c>yytext</c> is an inner
    /// join; any other unknown word is an internal error, because only <see cref="Write"/> produces
    /// the stored word.
    /// </summary>
    public static Kind Read(NodeAst joinNode)
    {
        if (joinNode.nodeType != NodeType.Join)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, $"Expected a join node, got {joinNode.nodeType}");

        return joinNode.yytext switch
        {
            null or InnerWord => Kind.Inner,
            LeftWord => Kind.Left,
            RightWord => Kind.Right,
            CrossWord => Kind.Cross,
            _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, $"Unknown join kind word '{joinNode.yytext}'"),
        };
    }
}
