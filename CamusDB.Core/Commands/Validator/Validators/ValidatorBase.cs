
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsValidator.Validators;

internal abstract class ValidatorBase
{
    /// <summary>
    /// Validates a user-facing identifier (database, table, column, or index name):
    /// must be non-empty, within <see cref="CamusDB.Core.CamusDBConfig.MaxIdentifierLength"/>,
    /// and composed only of alphanumeric characters and underscores.
    /// </summary>
    protected static void ValidateIdentifier(string name, string kind)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"{kind} name is required");

        int maxLen = CamusDB.Core.CamusDBConfig.MaxIdentifierLength;
        if (maxLen > 0 && name.Length > maxLen)
            throw new CamusDBException(
                CamusDBErrorCodes.SchemaLimitExceeded,
                $"{kind} name '{name}' is too long ({name.Length} > {maxLen})");

        if (!HasValidCharacters(name))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"{kind} name has invalid characters");
    }

    /// <summary>
    /// Enforces <see cref="CamusDB.Core.CamusDBConfig.MaxCommentLength"/> on one comment.
    ///
    /// <para>Shared rather than inlined because comments arrive through several unrelated tickets —
    /// <c>COMMENT ON</c>, inline <c>CREATE TABLE</c> (table, column, and index positions), and
    /// <c>ALTER TABLE … ADD COLUMN</c> — and they all end up in the same replicated schema-log entry
    /// and KV checkpoint. A bound enforced on only one of those entry points does not bound anything;
    /// every comment-bearing field on every comment-bearing ticket must call this.</para>
    ///
    /// <para><paramref name="comment"/> may be null (no comment, or a removal); only a present value
    /// is measured.</para>
    /// </summary>
    protected static void ValidateCommentLength(string? comment, string subject)
    {
        if (comment is null)
            return;

        int maxLen = CamusDB.Core.CamusDBConfig.MaxCommentLength;
        if (comment.Length > maxLen)
            throw new CamusDBException(
                CamusDBErrorCodes.CommentTooLong,
                $"{subject} comment is {comment.Length} characters, exceeding the maximum of {maxLen}");
    }

    /// <summary>
    /// Rejects comment text that cannot be re-emitted as a SQL string literal and read back unchanged.
    ///
    /// <para>Quote characters are <b>not</b> the problem — the emitter doubles <c>'</c>, which the
    /// lexer and <c>UnquoteStringLiteral</c> undo exactly, so a value containing quotes (including a
    /// deliberate <c>'); DROP TABLE …</c> payload) round-trips as inert text. The problem is that this
    /// dialect has no escape <em>decoding</em>: the lexer treats backslash-plus-any-character as one
    /// unit and hands it through verbatim, and its string body excludes raw control characters
    /// outright. So two shapes have no representation at all:</para>
    /// <list type="bullet">
    ///   <item>a backslash that would end up adjacent to a quote — the backslash swallows the quote
    ///     and the literal never closes, spilling the rest of the statement into the parser as
    ///     top-level tokens;</item>
    ///   <item>a raw control character (newline, tab, NUL …) — no production accepts one.</item>
    /// </list>
    ///
    /// <para>Storing such a value would make <c>SHOW CREATE TABLE</c> emit DDL that does not parse,
    /// breaking the round-trip guarantee and putting attacker-influenced text outside the quotes.
    /// Refusing it up front is the containment: the stored set is exactly the emittable set.</para>
    ///
    /// <para>The same limitation applies to string <c>DEFAULT</c> values, which predate this check and
    /// are not covered by it — see the notes in the COMMENT ON spec.</para>
    /// </summary>
    protected static void ValidateCommentIsRepresentable(string? comment, string subject)
    {
        if (string.IsNullOrEmpty(comment))
            return;

        for (int i = 0; i < comment.Length; i++)
        {
            char ch = comment[i];

            if (char.IsControl(ch))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"{subject} comment contains a control character (U+{(int)ch:X4}) at position {i}, " +
                    "which cannot be represented in a SQL string literal");

            if (ch != '\\')
                continue;

            if (i == comment.Length - 1)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"{subject} comment ends with a backslash, which would escape the closing quote " +
                    "when the comment is rendered back as SQL");

            char next = comment[i + 1];
            if (next is '\'' or '"')
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"{subject} comment contains a backslash immediately before a quote at position {i}, " +
                    "which would escape that quote when the comment is rendered back as SQL");
        }
    }

    protected static bool HasValidCharacters(string name)
    {
        for (int i = 0; i < name.Length; i++)
        {
            char ch = name[i];

            if (ch >= 'a' && ch <= 'z')
                continue;

            if (ch >= 'A' && ch <= 'Z')
                continue;

            if (ch >= '0' && ch <= '9')
                continue;

            if (ch == '_')
                continue;

            return false;
        }

        return true;
    }

    protected static bool IsReservedName(string name)
    {
        return name == "_id";            
    }
}

