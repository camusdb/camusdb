
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Controllers.DML;

namespace CamusDB.Core.CommandsValidator.Validators;

internal sealed class UpdateValidator : ValidatorBase
{
    public UpdateValidator(CamusDBOptions options) : base(options) { }

    public void Validate(UpdateTicket ticket)
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

        if (ticket.PlainValues is null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Values are required"
            );

        // A typed row update (HTTP /update, gRPC) arrives as a map that can hold "a" and "A" as two
        // keys. Both resolve to column a, so the map gives that column two values.
        AssignmentTargets.RequireUniqueIgnoreCase(ticket.PlainValues, ticket.PlainValues.Count);

        foreach (KeyValuePair<string, ColumnValue> columnValue in ticket.PlainValues)
        {
            switch (columnValue.Value.Type)
            {
                case ColumnType.Id: // @todo validate alphanumeric digits
                    if (!string.IsNullOrEmpty(columnValue.Value.StrValue) && columnValue.Value.StrValue.Length != 24)
                        throw new CamusDBException(
                            CamusDBErrorCodes.InvalidInput,
                            $"Invalid id value for field '{columnValue.Key}'"
                        );
                    break;
            }
        }

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
