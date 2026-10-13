/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

/// <summary>
/// Attaches to a statement's parameters every value that its function calls read from the statement
/// rather than from the rows: the session snapshot (<see cref="SessionScalarFunctions"/>) and the
/// transaction and statement times (<see cref="ClockScalarFunctions"/>).
///
/// <para><b>Why one entry point.</b> Each kind is attached only when the statement calls a function
/// of that kind, which takes a walk of the whole AST. Most statements call neither kind, so one walk
/// first asks whether the statement calls a function of either kind, and only a statement that does
/// pays for the walk of each kind. A large multi-row INSERT then costs one walk, not two.</para>
///
/// <para>Called by the SQL entry points before any sub-ticket is built, and again wherever an AST
/// grows calls the entry point could not see (a view body expanded into the statement). Both
/// attaches are idempotent for the clock and repeatable for the session, so a second call is
/// safe.</para>
/// </summary>
internal static class StatementScopedValues
{
    public static ExecuteSQLTicket Attach(ExecuteSQLTicket ticket, NodeAst ast)
    {
        if (!ScalarFunctionEvaluator.ContainsStatementScopedFunction(ast))
            return ticket;

        ticket = SessionScalarFunctions.AttachSessionValues(ticket, ast);
        return ClockScalarFunctions.AttachStatementClock(ticket, ast);
    }
}
