
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
