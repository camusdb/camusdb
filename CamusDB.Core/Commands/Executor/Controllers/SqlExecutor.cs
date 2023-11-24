
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

                return new(ticket.DatabaseName, tableName, null, null, ast.extendedOne, null);

            default:
                throw new Exception("Unknown ast stmt");
        }
    }    
}
