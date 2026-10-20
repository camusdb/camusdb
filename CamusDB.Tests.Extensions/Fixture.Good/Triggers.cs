/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using CamusDB.Extensions;

namespace CamusDB.Fixtures.Good;

/// <summary>
/// Test-only: writes one row into <c>audit_log (op, timing, tbl, detail)</c> per event, through
/// the statement's transaction. <c>detail</c> is the <c>name</c> column of NEW (or OLD for a
/// DELETE), and the <c>wide</c> column length when the row carries one, so a test can see that
/// the trigger received the full row.
/// </summary>
public sealed class AuditTrigger : ICamusTrigger
{
    public Task FireAsync(ITriggerContext context)
    {
        IRow? row = (IRow?)context.New ?? context.Old;
        string detail = row?.GetString("name") ?? "";

        string? wide = row?.GetString("wide");
        if (wide is not null)
            detail += ":" + wide.Length;

        return context.Sql.ExecuteAsync(
            "INSERT INTO audit_log (op, timing, tbl, detail) VALUES (@op, @timing, @tbl, @detail)",
            new Dictionary<string, object?>
            {
                ["op"] = context.Operation.ToString(),
                ["timing"] = context.Timing.ToString(),
                ["tbl"] = context.TableName,
                ["detail"] = detail
            });
    }
}

/// <summary>Test-only: rejects a row whose <c>name</c> starts with "bad".</summary>
public sealed class GuardTrigger : ICamusTrigger
{
    public Task FireAsync(ITriggerContext context)
    {
        IRow? row = (IRow?)context.New ?? context.Old;
        string? name = row?.GetString("name");

        if (name is not null && name.StartsWith("bad", StringComparison.Ordinal))
            throw new InvalidOperationException("guard rejected " + name);

        return Task.CompletedTask;
    }
}

/// <summary>Test-only BEFORE trigger: upper-cases the <c>name</c> column of NEW.</summary>
public sealed class UppercaseTrigger : ICamusTrigger
{
    public Task FireAsync(ITriggerContext context)
    {
        IWritableRow? row = context.New;
        string? name = row?.GetString("name");

        if (row is not null && name is not null)
            row.Set("name", name.ToUpperInvariant());

        return Task.CompletedTask;
    }
}

/// <summary>Test-only BEFORE trigger: sets <c>name</c> to NULL, so a NOT NULL column must refuse
/// the row.</summary>
public sealed class NullNameTrigger : ICamusTrigger
{
    public Task FireAsync(ITriggerContext context)
    {
        context.New?.SetNull("name");
        return Task.CompletedTask;
    }
}

/// <summary>Test-only: inserts a row into its own table on each insert, so it nests until the
/// server's depth limit stops it.</summary>
public sealed class RecursiveTrigger : ICamusTrigger
{
    public Task FireAsync(ITriggerContext context)
    {
        return context.Sql.ExecuteAsync(
            "INSERT INTO " + context.TableName + " (name) VALUES ('again')");
    }
}

/// <summary>Test-only: counts firings per table name. <see cref="Functions.TriggerCount"/> reads
/// the count through SQL.</summary>
public sealed class CountingTrigger : ICamusTrigger
{
    private static readonly ConcurrentDictionary<string, long> counts = new(StringComparer.Ordinal);

    public static long Count(string table) => counts.TryGetValue(table, out long n) ? n : 0;

    public Task FireAsync(ITriggerContext context)
    {
        counts.AddOrUpdate(context.TableName, 1, static (_, n) => n + 1);
        return Task.CompletedTask;
    }
}

/// <summary>
/// Test-only AFTER trigger: counts the rows of its own table through SQL and records the count in
/// <c>audit_log.detail</c>. When AFTER triggers fire after the whole statement, every firing of one
/// multi-row statement records the same, final count.
/// </summary>
public sealed class CountRowsTrigger : ICamusTrigger
{
    public async Task FireAsync(ITriggerContext context)
    {
        IReadOnlyList<IRow> rows = await context.Sql.QueryAsync("SELECT count(*) AS n FROM " + context.TableName);
        long count = rows[0].GetInt64("n") ?? -1;

        await context.Sql.ExecuteAsync(
            "INSERT INTO audit_log (op, timing, tbl, detail) VALUES (@op, @timing, @tbl, @detail)",
            new Dictionary<string, object?>
            {
                ["op"] = context.Operation.ToString(),
                ["timing"] = context.Timing.ToString(),
                ["tbl"] = context.TableName,
                ["detail"] = count.ToString(System.Globalization.CultureInfo.InvariantCulture)
            });
    }
}

/// <summary>Test-only BEFORE UPDATE trigger: writes into the bytes it reads from NEW, without a setter.
/// The server hands out copies, so the stored row must not change.</summary>
public sealed class MutateBytesTrigger : ICamusTrigger
{
    public Task FireAsync(ITriggerContext context)
    {
        byte[]? payload = context.New?.GetBytes("payload");
        if (payload is { Length: > 0 })
            payload[0] = 2;

        byte[]? old = context.Old?.GetBytes("payload");
        if (old is { Length: > 0 })
            old[0] = 3;

        return Task.CompletedTask;
    }
}

/// <summary>Test-only BEFORE trigger: on the row with id 1, updates the row with id 2 of the same
/// table, which the outer statement already read in the same chunk.</summary>
public sealed class UpdateNextRowTrigger : ICamusTrigger
{
    public Task FireAsync(ITriggerContext context)
    {
        IRow? row = (IRow?)context.New ?? context.Old;
        return row?.GetInt64("id") == 1
            ? context.Sql.ExecuteAsync("UPDATE " + context.TableName + " SET name = 'nested' WHERE id = 2")
            : Task.CompletedTask;
    }
}

/// <summary>Test-only BEFORE DELETE trigger: on the row with id 1, deletes the row with id 2 of the same
/// table, which the outer statement already read in the same chunk.</summary>
public sealed class DeleteNextRowTrigger : ICamusTrigger
{
    public Task FireAsync(ITriggerContext context) =>
        context.Old?.GetInt64("id") == 1
            ? context.Sql.ExecuteAsync("DELETE FROM " + context.TableName + " WHERE id = 2")
            : Task.CompletedTask;
}

/// <summary>Test-only BEFORE UPDATE trigger: sets the large <c>wide</c> column, which the statement does
/// not assign, so the update must not copy the old stored cell for this row.</summary>
public sealed class SetWideTrigger : ICamusTrigger
{
    public Task FireAsync(ITriggerContext context)
    {
        context.New?.Set("wide", "changed by trigger");
        return Task.CompletedTask;
    }
}

/// <summary>Test-only BEFORE trigger: replaces a NULL <c>name</c> with "repaired".</summary>
public sealed class RepairNullTrigger : ICamusTrigger
{
    public Task FireAsync(ITriggerContext context)
    {
        if (context.New is { } row && row.IsNull("name"))
            row.Set("name", "repaired");

        return Task.CompletedTask;
    }
}

/// <summary>Test-only AFTER trigger: runs an INSERT that violates a foreign key, and swallows the
/// error. The statement must still fail.</summary>
public sealed class SwallowErrorTrigger : ICamusTrigger
{
    public async Task FireAsync(ITriggerContext context)
    {
        try
        {
            await context.Sql.ExecuteAsync("INSERT INTO child (id, name) VALUES (99, 'missing-parent')");
        }
        catch (Exception)
        {
            // A reasonable-looking local recovery; without savepoints it cannot be honored.
        }
    }
}

/// <summary>Test-only: a type that is not a trigger, for the CREATE TRIGGER refusal.</summary>
public sealed class NotATrigger
{
}
