
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Controllers.DML;

namespace CamusDB.Core.CommandsValidator.Validators;

internal sealed class InsertValidator : ValidatorBase
{
    public InsertValidator(CamusDBOptions options) : base(options) { }

    public void Validate(InsertTicket ticket)
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

        if (ticket.Values is null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Values are required"
            );

        // A typed row (HTTP /insert, gRPC) can hold "a" and "A" as two keys, and the insert resolves
        // both to column a. Refuse that here, for every table: the defaults pass also refuses it, but
        // it runs only for a table that has a column with a default.
        HashSet<string> seen = new(StringComparer.OrdinalIgnoreCase);

        foreach (Dictionary<string, ColumnValue> values in ticket.Values)
        {
            seen.Clear();

            foreach (KeyValuePair<string, ColumnValue> columnValue in values)
            {
                if (!seen.Add(columnValue.Key))
                    throw AssignmentTargets.RepeatedInsertColumn(columnValue.Key, "the inserted row");

                switch (columnValue.Value.Type)
                {
                    case ColumnType.Id: // @todo validate alphanumeric digits
                        if (!string.IsNullOrEmpty(columnValue.Value.StrValue) && columnValue.Value.StrValue.Length != 24)
                            throw new CamusDBException(
                                CamusDBErrorCodes.InvalidInput, $"Invalid id value for field '{columnValue.Key}'"
                            );
                        break;
                }
            }
        }
    }
}
