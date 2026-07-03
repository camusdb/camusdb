
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsValidator.Validators;

internal sealed class ExecuteSQLValidator : ValidatorBase
{
    public void Validate(ExecuteSQLTicket ticket)
    {
        if (string.IsNullOrWhiteSpace(ticket.Sql))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "SQL statement is required"
            );

        if (string.IsNullOrWhiteSpace(ticket.DatabaseName))
        {
            // Statements that operate at the server level (not tied to a specific database)
            // are valid without a DatabaseName. Parse to check before rejecting.
            NodeAst ast = SQLParserProcessor.Parse(ticket.Sql);
            bool serverLevel = ast.nodeType is
                NodeType.CreateDatabase or NodeType.CreateDatabaseIfNotExists or
                NodeType.CreateDatabaseBranch or NodeType.CreateDatabaseBranchIfNotExists or
                NodeType.DropDatabase or NodeType.DropDatabaseIfExists or
                NodeType.RenameDatabase or
                NodeType.ShowDatabases or
                NodeType.ShowBranches or
                NodeType.ShowAncestors;

            if (!serverLevel)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "Database name is required"
                );
        }
    }
}
