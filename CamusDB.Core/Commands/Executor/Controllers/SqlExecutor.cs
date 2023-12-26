
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Models;

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

                return new(ticket.DatabaseName, tableName, null, null, ast.extendedOne, GetQueryClause(ast));

            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Unknown ast stmt");
        }
    }

    private List<QueryOrderBy>? GetQueryClause(NodeAst ast)
    {
        if (ast.extendedTwo is null)
            return null;

        if (ast.extendedTwo.nodeType == NodeType.Identifier)
            return new() { new QueryOrderBy(ast.extendedTwo.yytext ?? "", QueryOrderByType.Ascending) };

        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid order by clause");
    }
}
