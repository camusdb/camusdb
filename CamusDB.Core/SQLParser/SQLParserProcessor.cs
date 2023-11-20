
namespace CamusDB.Core.SQLParser;

public static class SQLParserProcessor
{
    public static NodeAst Parse(string sql)
    {
        sqlParser sqlParser = new();
        return sqlParser.Parse(sql);
    }
}

