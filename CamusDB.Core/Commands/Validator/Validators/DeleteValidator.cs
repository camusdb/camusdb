
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsValidator.Validators;

internal sealed class DeleteValidator : ValidatorBase
{
    public DeleteValidator(CamusDBOptions options) : base(options) { }

    public void Validate(DeleteTicket ticket)
    {
        if (string.IsNullOrWhiteSpace(ticket.DatabaseName))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Database name is required"
            );

        if (string.IsNullOrWhiteSpace(ticket.TableName))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Table name is required"
            );

        if (ticket.Limit is { nodeType: NodeType.Integer } limitNode)
        {
            if (!long.TryParse(limitNode.yytext, out long limitValue) || limitValue < 0)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "LIMIT must be a non-negative integer"
                );
        }
    }
}
