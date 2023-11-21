
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.SQLParser;

internal partial class sqlScanner
{
    void GetNumber()
    {            
        
    }

	public override void yyerror(string format, params object[] args)
	{
		base.yyerror(format, args);
		Console.WriteLine(format, args);
		Console.WriteLine();
	}
}