
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

	/// <summary>The opening quote of the dollar-quoted text the scanner is inside, such as <c>$$</c> or <c>$fn$</c>.</summary>
	private string? dollarQuote;

	/// <summary>The raw text read so far between the dollar quotes.</summary>
	private System.Text.StringBuilder? dollarText;

	/// <summary>Starts a dollar-quoted text: remembers the quote that must close it.</summary>
	private void BeginDollarQuote(string quote)
	{
		dollarQuote = quote;
		dollarText = new System.Text.StringBuilder();
	}

	/// <summary>True when <paramref name="quote"/> is the quote that opened the text. Tags compare exactly: <c>$A$</c> does not close <c>$a$</c>.</summary>
	private bool EndsDollarQuote(string quote) => string.Equals(quote, dollarQuote, StringComparison.Ordinal);

	private void AppendDollarQuote(string text) => dollarText!.Append(text);

	/// <summary>Returns the text between the quotes, exactly as written, and leaves the state clean for the next token.</summary>
	private string TakeDollarQuote()
	{
		string text = dollarText!.ToString();
		dollarQuote = null;
		dollarText = null;
		return text;
	}

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
	/// The error for a character that no token starts with. Without it the scanner drops the character
	/// and reads on, so the statement runs as if the character were not there: <c>a \ - b</c> computes
	/// <c>a - b</c>, and <c>$1</c> reads as the literal <c>1</c>. A quote reaches this rule when its
	/// literal does not close, or when the literal holds a raw control character, which the string rules
	/// do not admit. A character outside printable ASCII is named by its code point, because it may not
	/// be visible in the statement or in the error.
	/// </summary>
	private CamusDBException UnexpectedCharacter(string text)
	{
		string where = $"(line {yyline}, col {yycol + 1}) Syntax error";

		if (text is "'" or "\"" or "`")
			return new(CamusDBErrorCodes.SqlSyntaxError,
				$"{where}, quoted text starting with {text} does not close, or holds a raw control character such as a tab or a line break");

		string shown = text.Length == 1 && text[0] >= ' ' && text[0] <= '~'
			? $"'{text}'"
			: $"U+{char.ConvertToUtf32(text, 0):X4}";

		return new(CamusDBErrorCodes.SqlSyntaxError, $"{where}, unexpected character {shown}");
	}

	/// <summary>
	/// Called by the scanner on EOF. Raises an error if EOF arrives inside a block comment
	/// (i.e. the scanner is still in BLOCKCOMMENT state), which means the opening <c>/*</c>
	/// was never closed. Always returns true to signal end-of-input.
	/// </summary>
	protected override bool yywrap()
	{
		if (currentScOrd == BLOCKCOMMENT)
			yyerror("unterminated block comment");
		else if (currentScOrd == DOLLARQUOTE)
			yyerror("dollar-quoted text starting with " + dollarQuote + " does not close");
		return true;
	}

}
