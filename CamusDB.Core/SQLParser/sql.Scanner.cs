
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
