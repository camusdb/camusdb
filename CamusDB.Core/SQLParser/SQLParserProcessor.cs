
namespace CamusDB.Core.SQLParser;

public class SQLParserProcessor
{
	public static void Get()
	{
        sqlParser x = new();        

        NodeAst xu = x.Parse("SELECT xx, aa FROM yy");

        Console.WriteLine(xu.leftAst.nodeType);
    }
}

