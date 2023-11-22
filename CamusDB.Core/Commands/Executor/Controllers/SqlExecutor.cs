
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class SqlExecutor
{
    public SqlExecutor()
    {
    }

    public QueryTicket CreateQueryTicket(ExecuteSQLTicket ticket)
    {
        NodeAst ast = SQLParserProcessor.Parse(ticket.Sql);        

        switch (ast.nodeType)
        {
            case NodeType.Select:

                string tableName = ast.rightAst!.yytext!;

                List<QueryFilter>? filters = null;

                if (ast.extendedOne is not null)
                {
                    Console.WriteLine(ast.extendedOne.leftAst!.yytext!);
                    Console.WriteLine(ast.extendedOne.rightAst!.yytext!.Trim('"'));

                    filters = new() {
                        new(
                            ast.extendedOne.leftAst!.yytext!,
                            "=",
                            new ColumnValue(Catalogs.Models.ColumnType.String, ast.extendedOne.rightAst!.yytext!.Trim('"'))
                        )
                    };
                }

                /*if (ast.extendedTwo is not null)
                    Console.WriteLine(ast.extendedTwo.nodeType);
                else
                    Console.WriteLine("extendedTwo is null");*/

                return new(ticket.DatabaseName, tableName, null, filters, null);

            default:
                throw new Exception("Unknown ast stmt");
        }
    }    
}
