/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.SQLParser;

/// <summary>
/// Splits a SQL script into its statements at each top-level <c>;</c>. The parser takes one
/// statement per call, so a host that accepts a pasted script (the browser playground, a sample
/// data set) has to cut it first.
///
/// <para>A <c>;</c> ends a statement only outside the constructs where the lexer
/// (<c>SQLParser.Language.analyzer.lex</c>) reads it as text. These rules copy the lexer's, and must
/// change with it:</para>
/// <list type="bullet">
/// <item><c>'…'</c> and <c>"…"</c> strings, where a doubled quote is a literal quote and a backslash
/// is an ordinary character;</item>
/// <item><c>E'…'</c> and <c>E"…"</c> strings, where a backslash also escapes the next character, so
/// <c>E'\''</c> is one quote;</item>
/// <item><c>`…`</c> quoted identifiers;</item>
/// <item><c>-- …</c> comments to the end of the line, and <c>/* … */</c> comments, which do not nest.</item>
/// </list>
///
/// <para>The <c>E</c> prefix counts only when it starts a token: in <c>name'x'</c> the quote follows
/// an identifier, so the backslash rule does not apply. A piece that holds only whitespace and
/// comments is dropped, so a trailing <c>;</c> or a closing comment gives no empty statement.
/// Unterminated quotes or comments are not an error here: the rest of the script becomes the last
/// statement, and the parser reports the error with its own message.</para>
/// </summary>
public static class SqlScriptSplitter
{
    private enum State
    {
        Code,
        Quoted,
        LineComment,
        BlockComment,
    }

    /// <summary>
    /// Returns the statements of <paramref name="script"/> in order, each trimmed and without its
    /// terminating <c>;</c>.
    /// </summary>
    public static List<string> Split(string script)
    {
        ArgumentNullException.ThrowIfNull(script);

        List<string> statements = [];
        State state = State.Code;
        char quote = '\0';
        bool backslashEscapes = false;
        int start = 0;
        bool hasCode = false;

        for (int i = 0; i < script.Length; i++)
        {
            char c = script[i];

            switch (state)
            {
                case State.Code:
                    if (c == ';')
                    {
                        AddStatement(statements, script, start, i, hasCode);
                        start = i + 1;
                        hasCode = false;
                    }
                    else if (c == '-' && Next(script, i) == '-')
                    {
                        state = State.LineComment;
                        i++;
                    }
                    else if (c == '/' && Next(script, i) == '*')
                    {
                        state = State.BlockComment;
                        i++;
                    }
                    else if (c is '\'' or '"' or '`')
                    {
                        state = State.Quoted;
                        quote = c;
                        backslashEscapes = c != '`' && StartsEscapeString(script, i);
                        hasCode = true;
                    }
                    else if (!char.IsWhiteSpace(c))
                    {
                        hasCode = true;
                    }
                    break;

                case State.Quoted:
                    if (backslashEscapes && c == '\\')
                    {
                        i++;
                    }
                    else if (c == quote)
                    {
                        if (quote != '`' && Next(script, i) == quote)
                            i++;
                        else
                            state = State.Code;
                    }
                    break;

                case State.LineComment:
                    if (c == '\n')
                        state = State.Code;
                    break;

                case State.BlockComment:
                    if (c == '*' && Next(script, i) == '/')
                    {
                        state = State.Code;
                        i++;
                    }
                    break;
            }
        }

        AddStatement(statements, script, start, script.Length, hasCode);
        return statements;
    }

    private static char Next(string script, int i) => i + 1 < script.Length ? script[i + 1] : '\0';

    /// <summary>
    /// True when the quote at <paramref name="quoteIndex"/> opens an <c>E'…'</c> or <c>E"…"</c> string:
    /// it follows an <c>E</c> that is not itself the end of a longer identifier.
    /// </summary>
    private static bool StartsEscapeString(string script, int quoteIndex)
    {
        if (quoteIndex < 1 || script[quoteIndex - 1] is not ('E' or 'e'))
            return false;

        if (quoteIndex < 2)
            return true;

        char before = script[quoteIndex - 2];
        return !(char.IsAsciiLetterOrDigit(before) || before == '_');
    }

    private static void AddStatement(List<string> statements, string script, int start, int end, bool hasCode)
    {
        if (!hasCode)
            return;

        statements.Add(script[start..end].Trim());
    }
}
