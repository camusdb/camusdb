
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.CommandsExecutor.Controllers.DML;

namespace CamusDB.Core.CommandsValidator.Validators;

/// <summary>
/// Validates the statement-level shape of an <c>INSERT … SELECT</c>. Only what is knowable before
/// execution is checked here: the arity of target columns against the source's output columns needs
/// the bound query, so it is enforced by the controller once the source has been bound.
/// </summary>
internal sealed class InsertSelectValidator : ValidatorBase
{
    public InsertSelectValidator(CamusDBOptions options) : base(options) { }

    public void Validate(InsertSelectTicket ticket)
    {
        if (string.IsNullOrWhiteSpace(ticket.DatabaseName))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Database name is required");

        if (string.IsNullOrWhiteSpace(ticket.TableName))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Table name is required");

        if (ticket.SourceSelect.nodeType != NodeType.Select)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "INSERT ... SELECT requires a SELECT source");

        if (ticket.TargetColumns is null)
            return;

        if (ticket.TargetColumns.Count == 0)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "The column list of an INSERT is empty");

        // A name resolves to its column case-insensitively, and two columns of one table never
        // differ only in case, so (a, A) name one column twice, as in the VALUES form.
        HashSet<string> seen = new(ticket.TargetColumns.Count, StringComparer.OrdinalIgnoreCase);
        foreach (string column in ticket.TargetColumns)
        {
            if (string.IsNullOrWhiteSpace(column))
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Column name is required");

            if (!seen.Add(column))
                throw AssignmentTargets.RepeatedInsertColumn(column, "INSERT column list");
        }
    }
}
