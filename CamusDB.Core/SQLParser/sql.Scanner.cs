
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.SQLParser;

/// <summary>
/// Scanner for the SQL Parser
/// </summary>
internal partial class sqlScanner
{
	public string? YYError { get; set; }    

	/// <summary>
	/// Intercepts the yyerror method. Uses first-error-wins: if <see cref="YYError"/> is already
	/// set (e.g. by <see cref="yywrap"/> detecting an unterminated block comment), the subsequent
	/// parser "Syntax error, unexpected EOF" message is not allowed to overwrite it.
	/// </summary>
	public override void yyerror(string format, params object[] args)
	{
		base.yyerror(format, args);

        string message = args.Length > 0 ? string.Format(format, args) : format;
        string error = $"(line {yyline}, col {yycol + 1}) {message}";
        if (string.IsNullOrEmpty(YYError))
            YYError = error;
	}

	/// <summary>
	/// Removes the grouping underscores from a numeric literal, so <c>200_000</c> reaches the parser
	/// as <c>200000</c>. The lexer rules admit an underscore only between two digits, so removing
	/// every one cannot join or split a number. Stripping here, before the token text is stored,
	/// means every consumer of a numeric token — literal evaluation, <c>LIMIT</c>, <c>string(N)</c>,
	/// the negative-literal rules — sees plain digits and needs no change.
	/// </summary>
	private static string StripDigitSeparators(string text) =>
		text.Contains('_') ? text.Replace("_", "") : text;

	/// <summary>
	/// The error for a number with a misplaced underscore (<c>200_</c>, <c>2__0</c>, <c>1_.5</c>).
	/// Thrown from the scanner action instead of left to the parser: otherwise the text lexes as a
	/// number followed by an identifier, and the parser reports an unrelated unexpected token.
	/// </summary>
	private CamusDBException InvalidNumericLiteral(string text) =>
		new(CamusDBErrorCodes.SqlSyntaxError,
			$"(line {yyline}, col {yycol + 1}) Invalid numeric literal '{text}': an underscore must sit between two digits");

	/// <summary>
	/// Called by the scanner on EOF. Raises an error if EOF arrives inside a block comment
	/// (i.e. the scanner is still in BLOCKCOMMENT state), which means the opening <c>/*</c>
	/// was never closed. Always returns true to signal end-of-input.
	/// </summary>
	protected override bool yywrap()
	{
		if (currentScOrd == BLOCKCOMMENT)
			yyerror("unterminated block comment");
		return true;
	}
}
