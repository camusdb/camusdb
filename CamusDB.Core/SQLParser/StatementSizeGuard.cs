/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;

namespace CamusDB.Core.SQLParser;

/// <summary>
/// Refuses SQL text longer than <see cref="CamusDBOptions.MaxStatementBytes"/> before anything parses
/// it, with <see cref="CamusDBErrorCodes.StatementTooLarge"/>.
///
/// <para><b>Why before the parse.</b> The lexer and the shift-reduce parser cost time and memory in
/// proportion to the text, and a statement that is new text always misses the parse cache. Without
/// this check the only bound was the request body limit, so each request could make the node parse
/// tens of megabytes. Every transport entry that parses inline text calls this first:
/// <see cref="CommandsExecutor.CommandExecutor.ParseSql"/>, the SQL ticket validator, and the
/// prepare path.</para>
///
/// <para>The unit is UTF-8 bytes, the size on the wire. The count is exact only near the limit: one
/// UTF-16 character is one to three UTF-8 bytes, so text of at most a third of the limit in
/// characters passes, and text longer than the limit in characters fails, with no count.</para>
/// </summary>
internal static class StatementSizeGuard
{
    /// <summary>Throws when <paramref name="sql"/> is longer than <paramref name="maxBytes"/>; a limit of zero or less disables the check.</summary>
    /// <exception cref="CamusDBException"><see cref="CamusDBErrorCodes.StatementTooLarge"/>.</exception>
    internal static void Check(string? sql, int maxBytes)
    {
        if (maxBytes <= 0 || sql is null)
            return;

        // Every UTF-16 character encodes to at most three UTF-8 bytes (a surrogate pair is two
        // characters and four bytes), so text this short is under the limit without a count.
        if ((long)sql.Length * 3 <= maxBytes)
            return;

        // Every character encodes to at least one byte, so text this long is over the limit.
        long bytes = sql.Length > maxBytes ? sql.Length : Encoding.UTF8.GetByteCount(sql);

        if (bytes > maxBytes)
            throw new CamusDBException(
                CamusDBErrorCodes.StatementTooLarge,
                $"The SQL statement is longer than {maxBytes} bytes, the largest this node parses " +
                "(max_statement_bytes). Bind large values as parameters, or split the statement.");
    }
}
