
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
	/// Intercepts the yyerror method
	/// </summary>
	/// <param name="format"></param>
	/// <param name="args"></param>
	public override void yyerror(string format, params object[] args)
	{
		base.yyerror(format, args);

        YYError = string.Format(format, args);		
	}
}
