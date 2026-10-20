/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using CamusDB.Extensions;

namespace CamusDB.Fixtures.Good;

/// <summary>
/// Test-only: <c>CALL fx_transfer(from, to, amount)</c> moves <c>amount</c> between two rows of
/// <c>accounts (name, balance)</c> with two UPDATEs, then reads the source balance and throws when
/// it went negative. The throw must roll both UPDATEs back.
/// </summary>
[CamusProcedure("fx_transfer", MinArguments = 3, MaxArguments = 3)]
public sealed class TransferProcedure : ICamusProcedure
{
    public async Task ExecuteAsync(IProcedureContext context)
    {
        string from = (string)context.Arguments[0]!;
        string to = (string)context.Arguments[1]!;
        long amount = (long)context.Arguments[2]!;

        await context.Sql.ExecuteAsync(
            "UPDATE accounts SET balance = balance - @amount WHERE name = @name",
            new Dictionary<string, object?> { ["amount"] = amount, ["name"] = from });

        await context.Sql.ExecuteAsync(
            "UPDATE accounts SET balance = balance + @amount WHERE name = @name",
            new Dictionary<string, object?> { ["amount"] = amount, ["name"] = to });

        IReadOnlyList<IRow> rows = await context.Sql.QueryAsync(
            "SELECT balance FROM accounts WHERE name = @name",
            new Dictionary<string, object?> { ["name"] = from });

        if (rows.Count == 1 && rows[0].GetInt64("balance") < 0)
            throw new InvalidOperationException("insufficient funds in " + from);
    }
}

/// <summary>Test-only: <c>CALL fx_swallow()</c> inserts a child row with no parent and swallows the
/// foreign-key error. The CALL must still fail and leave nothing behind.</summary>
[CamusProcedure("fx_swallow", MaxArguments = 0)]
public sealed class SwallowProcedure : ICamusProcedure
{
    public async Task ExecuteAsync(IProcedureContext context)
    {
        try
        {
            await context.Sql.ExecuteAsync("INSERT INTO child (id, name) VALUES (2, 'missing-parent')");
        }
        catch (Exception)
        {
            // A reasonable-looking local recovery; without savepoints it cannot be honored.
        }
    }
}

/// <summary>Test-only: a procedure with no arguments that does nothing.</summary>
[CamusProcedure("fx_noop", MaxArguments = 0)]
public sealed class NoopProcedure : ICamusProcedure
{
    public Task ExecuteAsync(IProcedureContext context) => Task.CompletedTask;
}
