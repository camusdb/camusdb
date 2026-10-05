/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// <c>ADD CONSTRAINT ... CHECK</c> and <c>ALTER COLUMN ... SET NOT NULL</c> enforce the constraint before
/// they read the existing rows. A write that commits while the rows are read is therefore checked by its
/// own statement, and the constraint is never published over a row that breaks it. Each scenario runs
/// through SQL, on a standalone engine and on a cluster-mode engine.
/// </summary>
internal static class RowConstraintValidationScenarios
{
    private const string SetNotNull = "ALTER TABLE items ALTER COLUMN price SET NOT NULL";

    private const string AddCheck = "ALTER TABLE items ADD CONSTRAINT items_price_pos CHECK (price > 0)";

    /// <summary>
    /// A violating UPDATE that runs after the constraint is enforced and before the rows are read is
    /// refused. With the read first, it committed, and the ALTER succeeded over a NULL row.
    /// </summary>
    public static async Task SetNotNullRefusesAViolatingWriteDuringTheRead(CommandExecutor executor, string dbname)
    {
        await CreateItems(executor, dbname, rows: 3);

        string? update = await WithHook(executor, () => Outcome(() => Dml(executor, dbname, "UPDATE items SET price = NULL WHERE id = 0")),
            () => Ddl(executor, dbname, SetNotNull));

        StringAssert.StartsWith(CamusDBErrorCodes.NotNullViolation, update);
        Assert.AreEqual(0, await Count(executor, dbname, "SELECT id FROM items WHERE price IS NULL"));
        Assert.IsTrue((await Column(executor, dbname)).NotNull);
        Assert.IsEmpty(await executor.Catalogs.LoadCoordinatorJobsAsync(await executor.OpenDatabase(dbname)));
    }

    /// <summary>The same order for CHECK: a violating UPDATE during the read is refused.</summary>
    public static async Task AddCheckRefusesAViolatingWriteDuringTheRead(CommandExecutor executor, string dbname)
    {
        await CreateItems(executor, dbname, rows: 3);

        string? update = await WithHook(executor, () => Outcome(() => Dml(executor, dbname, "UPDATE items SET price = -1 WHERE id = 0")),
            () => Ddl(executor, dbname, AddCheck));

        StringAssert.StartsWith(CamusDBErrorCodes.CheckConstraintViolation, update);
        Assert.AreEqual(0, await Count(executor, dbname, "SELECT id FROM items WHERE price <= 0"));
        Assert.IsTrue((await Table(executor, dbname)).CheckConstraints!.Exists(c => c.Name == "items_price_pos"));
        Assert.IsEmpty(await executor.Catalogs.LoadCoordinatorJobsAsync(await executor.OpenDatabase(dbname)));
    }

    /// <summary>
    /// A transaction that wrote a NULL before SET NOT NULL, and commits while the rows are read, is
    /// refused by the write-shape fence. Its row is neither stored nor seen by the read.
    /// </summary>
    public static async Task TransactionThatWroteBeforeIsRefusedWhenItCommitsDuringTheRead(CommandExecutor executor, string dbname)
    {
        await CreateItems(executor, dbname, rows: 3);

        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, "INSERT INTO items (id, price) VALUES (100, NULL)", null));

        string? commit = await WithHook(executor, () => Outcome(() => database.Transactions.CommitAsync(tx)),
            () => Ddl(executor, dbname, SetNotNull));
        await database.Transactions.RollbackIfNotCompletedAsync(tx);

        StringAssert.StartsWith(CamusDBErrorCodes.TransactionConflict, commit);
        Assert.AreEqual(0, await Count(executor, dbname, "SELECT id FROM items WHERE id = 100"));
        Assert.IsTrue((await Column(executor, dbname)).NotNull);
    }

    /// <summary>
    /// A row that breaks the new NOT NULL fails the statement and takes the constraint back: the column is
    /// nullable again, it has no constraint name, a NULL is accepted, and no job is left.
    /// </summary>
    public static async Task SetNotNullOverANullRowLeavesTheColumnNullable(CommandExecutor executor, string dbname)
    {
        await CreateItems(executor, dbname, rows: 3);
        await Dml(executor, dbname, "INSERT INTO items (id, price) VALUES (50, NULL)");

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () => await Ddl(executor, dbname, SetNotNull))!;

        Assert.AreEqual(CamusDBErrorCodes.NotNullViolation, exception.Code);
        TableColumnSchema column = await Column(executor, dbname);
        Assert.IsFalse(column.NotNull);
        Assert.IsNull(column.NotNullConstraintName);
        await Dml(executor, dbname, "INSERT INTO items (id, price) VALUES (51, NULL)");
        Assert.IsEmpty(await executor.Catalogs.LoadCoordinatorJobsAsync(await executor.OpenDatabase(dbname)));
    }

    /// <summary>A row that breaks the new CHECK fails the statement and removes the constraint.</summary>
    public static async Task AddCheckOverAViolatingRowRemovesTheConstraint(CommandExecutor executor, string dbname)
    {
        await CreateItems(executor, dbname, rows: 3);
        await Dml(executor, dbname, "INSERT INTO items (id, price) VALUES (50, -7)");

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () => await Ddl(executor, dbname, AddCheck))!;

        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, exception.Code);
        Assert.IsFalse((await Table(executor, dbname)).CheckConstraints?.Exists(c => c.Name == "items_price_pos") ?? false);
        await Dml(executor, dbname, "INSERT INTO items (id, price) VALUES (51, -8)");
        Assert.IsEmpty(await executor.Catalogs.LoadCoordinatorJobsAsync(await executor.OpenDatabase(dbname)));

        // The name is free again, and the constraint can be added once the rows are fixed.
        await Dml(executor, dbname, "DELETE FROM items WHERE price <= 0");
        await Ddl(executor, dbname, AddCheck);
        await ExpectDml(executor, dbname, "INSERT INTO items (id, price) VALUES (52, 0)", CamusDBErrorCodes.CheckConstraintViolation);
    }

    /// <summary>
    /// SET NOT NULL on a column that is already NOT NULL reads no row and takes the generated name. A
    /// failure path that made it nullable would be wrong here, so the flag must stay.
    /// </summary>
    public static async Task SetNotNullOnANotNullColumnOnlyRenames(CommandExecutor executor, string dbname)
    {
        await Ddl(executor, dbname, "CREATE TABLE items (id int64 PRIMARY KEY NOT NULL, price int64 NOT NULL)");
        await Dml(executor, dbname, "INSERT INTO items (id, price) VALUES (1, 1)");

        bool hookRan = false;
        await WithHook(executor, () => { hookRan = true; return Task.FromResult<string?>(null); }, () => Ddl(executor, dbname, SetNotNull));

        Assert.IsFalse(hookRan, "A column that is already NOT NULL needs no read of its rows");
        TableColumnSchema column = await Column(executor, dbname);
        Assert.IsTrue(column.NotNull);
        Assert.AreEqual("items_price_not_null", column.NotNullConstraintName);
    }

    /// <summary>
    /// Many runs of the race that the old order lost: an autocommit UPDATE writes a violating value into
    /// the first row while the ALTER reads a large table. Whatever the timing, either the ALTER fails or
    /// no violating row exists after it.
    /// </summary>
    public static async Task ConcurrentViolatingUpdatesNeverSurviveTheAlter(Func<Task<(string dbname, CommandExecutor executor)>> newDatabase)
    {
        List<string> broken = [];

        foreach ((string alter, string update, string violating) in new[]
        {
            (SetNotNull, "UPDATE items SET price = NULL WHERE id = 0", "SELECT id FROM items WHERE price IS NULL"),
            (AddCheck, "UPDATE items SET price = -1 WHERE id = 0", "SELECT id FROM items WHERE price <= 0"),
        })
        {
            foreach (int delayMs in new[] { 0, 2, 5, 10 })
            {
                (string dbname, CommandExecutor executor) = await newDatabase();
                await CreateItems(executor, dbname, rows: 10_000);

                Task<string?> alterTask = Task.Run(() => Outcome(() => Ddl(executor, dbname, alter)));
                if (delayMs > 0)
                    await Task.Delay(delayMs);
                await Outcome(() => Dml(executor, dbname, update));
                string? alterOutcome = await alterTask;

                int rows = await Count(executor, dbname, violating);
                if (alterOutcome is null && rows > 0)
                    broken.Add($"{alter} (delay {delayMs} ms): succeeded with {rows} violating row(s)");
            }
        }

        Assert.IsEmpty(broken, string.Join("\n", broken));
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    internal static async Task CreateItems(CommandExecutor executor, string dbname, int rows)
    {
        await Ddl(executor, dbname, "CREATE TABLE items (id int64 PRIMARY KEY NOT NULL, price int64)");

        const int Batch = 500;
        for (int start = 0; start < rows; start += Batch)
        {
            StringBuilder sql = new("INSERT INTO items (id, price) VALUES ");
            for (int i = start; i < Math.Min(rows, start + Batch); i++)
            {
                if (i > start)
                    sql.Append(", ");
                sql.Append('(').Append(i).Append(", 1)");
            }

            await Dml(executor, dbname, sql.ToString());
        }
    }

    /// <summary>
    /// Runs <paramref name="statement"/> with <paramref name="hook"/> installed in front of the row read,
    /// and returns what the hook returned. The hook runs once and is cleared before it runs.
    /// </summary>
    internal static async Task<string?> WithHook(CommandExecutor executor, Func<Task<string?>> hook, Func<Task> statement)
    {
        string? hookOutcome = null;

        executor.TestInterceptBeforeRowConstraintValidation = async () =>
        {
            executor.TestInterceptBeforeRowConstraintValidation = null;
            hookOutcome = await hook();
        };

        try
        {
            await statement();
        }
        finally
        {
            executor.TestInterceptBeforeRowConstraintValidation = null;
        }

        return hookOutcome;
    }

    /// <summary>Null when <paramref name="action"/> succeeds, else the error code and message.</summary>
    internal static async Task<string?> Outcome(Func<Task> action)
    {
        try
        {
            await action();
            return null;
        }
        catch (CamusDBException ex)
        {
            return ex.Code + " " + ex.Message;
        }
    }

    internal static async Task<TableSchema> Table(CommandExecutor executor, string dbname) =>
        (await executor.OpenDatabase(dbname)).Schema.Tables["items"];

    internal static async Task<TableColumnSchema> Column(CommandExecutor executor, string dbname) =>
        (await Table(executor, dbname)).Columns!.Single(c => c.Name == "price");

    internal static Task Ddl(CommandExecutor executor, string dbname, string sql) => ForeignKeyAlterScenarios.Ddl(executor, dbname, sql);

    internal static Task Dml(CommandExecutor executor, string dbname, string sql) => ForeignKeyAlterScenarios.Dml(executor, dbname, sql);

    internal static async Task<int> Count(CommandExecutor executor, string dbname, string sql) =>
        (await ForeignKeyAlterScenarios.Query(executor, dbname, sql)).Count;

    private static async Task ExpectDml(CommandExecutor executor, string dbname, string sql, string code)
    {
        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () => await Dml(executor, dbname, sql))!;
        Assert.AreEqual(code, exception.Code, $"{sql}: {exception.Message}");
    }
}

/// <summary>The row constraint scenarios on a standalone engine.</summary>
[TestFixture]
[NonParallelizable]
public sealed class TestRowConstraintValidationOrder : BaseTest
{
    [Test] public async Task SetNotNullRefusesAViolatingWriteDuringTheRead() => await Run(RowConstraintValidationScenarios.SetNotNullRefusesAViolatingWriteDuringTheRead);
    [Test] public async Task AddCheckRefusesAViolatingWriteDuringTheRead() => await Run(RowConstraintValidationScenarios.AddCheckRefusesAViolatingWriteDuringTheRead);
    [Test] public async Task TransactionThatWroteBeforeIsRefusedWhenItCommitsDuringTheRead() => await Run(RowConstraintValidationScenarios.TransactionThatWroteBeforeIsRefusedWhenItCommitsDuringTheRead);
    [Test] public async Task SetNotNullOverANullRowLeavesTheColumnNullable() => await Run(RowConstraintValidationScenarios.SetNotNullOverANullRowLeavesTheColumnNullable);
    [Test] public async Task AddCheckOverAViolatingRowRemovesTheConstraint() => await Run(RowConstraintValidationScenarios.AddCheckOverAViolatingRowRemovesTheConstraint);
    [Test] public async Task SetNotNullOnANotNullColumnOnlyRenames() => await Run(RowConstraintValidationScenarios.SetNotNullOnANotNullColumnOnlyRenames);

    [Test]
    public async Task ConcurrentViolatingUpdatesNeverSurviveTheAlter()
    {
        await RowConstraintValidationScenarios.ConcurrentViolatingUpdatesNeverSurviveTheAlter(async () =>
        {
            (string dbname, _, CommandExecutor executor) = await CreateDatabase();
            return (dbname, executor);
        });
    }

    private async Task Run(Func<CommandExecutor, string, Task> scenario)
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await scenario(executor, dbname);
    }
}

/// <summary>
/// The row constraint scenarios on a cluster-mode engine, where the constraint replicates with a
/// coordinator job, and the resume of a job that a previous leader left.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestRowConstraintValidationOrderCluster : SharedNodeBaseTest
{
    [Test] public async Task SetNotNullRefusesAViolatingWriteDuringTheRead() => await Run(RowConstraintValidationScenarios.SetNotNullRefusesAViolatingWriteDuringTheRead);
    [Test] public async Task AddCheckRefusesAViolatingWriteDuringTheRead() => await Run(RowConstraintValidationScenarios.AddCheckRefusesAViolatingWriteDuringTheRead);
    [Test] public async Task TransactionThatWroteBeforeIsRefusedWhenItCommitsDuringTheRead() => await Run(RowConstraintValidationScenarios.TransactionThatWroteBeforeIsRefusedWhenItCommitsDuringTheRead);
    [Test] public async Task SetNotNullOverANullRowLeavesTheColumnNullable() => await Run(RowConstraintValidationScenarios.SetNotNullOverANullRowLeavesTheColumnNullable);
    [Test] public async Task AddCheckOverAViolatingRowRemovesTheConstraint() => await Run(RowConstraintValidationScenarios.AddCheckOverAViolatingRowRemovesTheConstraint);
    [Test] public async Task SetNotNullOnANotNullColumnOnlyRenames() => await Run(RowConstraintValidationScenarios.SetNotNullOnANotNullColumnOnlyRenames);

    /// <summary>
    /// A leader stopped after the CHECK replicated and before it read the rows: the constraint is
    /// enforced, a violating row exists, and the job is recorded. The next leader removes the constraint.
    /// </summary>
    [Test]
    public async Task ResumedCheckJobOverAViolatingRowRemovesTheConstraint()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await RowConstraintValidationScenarios.CreateItems(executor, dbname, rows: 3);
        await RowConstraintValidationScenarios.Dml(executor, dbname, "INSERT INTO items (id, price) VALUES (50, -7)");
        string tableId = (await RowConstraintValidationScenarios.Table(executor, dbname)).Id;

        await PersistJob(executor, database, tableId, "items_price_pos", SchemaElementKind.Check);
        await executor.Catalogs.ReplicateAddCheckConstraintAsync(database, "items", "items_price_pos", "price > 0", ["price"]);

        await Coordinator(executor).ResumeJobsAsync(database);

        Assert.IsFalse((await RowConstraintValidationScenarios.Table(executor, dbname)).CheckConstraints?.Exists(c => c.Name == "items_price_pos") ?? false);
        Assert.IsEmpty(await executor.Catalogs.LoadCoordinatorJobsAsync(database));
    }

    /// <summary>The same stop for NOT NULL: the next leader makes the column nullable again.</summary>
    [Test]
    public async Task ResumedNotNullJobOverANullRowMakesTheColumnNullable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await RowConstraintValidationScenarios.CreateItems(executor, dbname, rows: 3);
        await RowConstraintValidationScenarios.Dml(executor, dbname, "INSERT INTO items (id, price) VALUES (50, NULL)");
        string tableId = (await RowConstraintValidationScenarios.Table(executor, dbname)).Id;

        await PersistJob(executor, database, tableId, "items_price_not_null", SchemaElementKind.NotNull);
        await executor.Catalogs.ReplicateSetColumnNotNullAsync(database, "items", "price", notNull: true, "items_price_not_null");

        await Coordinator(executor).ResumeJobsAsync(database);

        TableColumnSchema column = (await RowConstraintValidationScenarios.Table(executor, dbname)).Columns!.Single(c => c.Name == "price");
        Assert.IsFalse(column.NotNull);
        Assert.IsNull(column.NotNullConstraintName);
        Assert.IsEmpty(await executor.Catalogs.LoadCoordinatorJobsAsync(database));
    }

    /// <summary>A resumed job over rows that satisfy the constraint keeps it and deletes the job.</summary>
    [Test]
    public async Task ResumedJobOverValidRowsKeepsTheConstraint()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await RowConstraintValidationScenarios.CreateItems(executor, dbname, rows: 3);
        string tableId = (await RowConstraintValidationScenarios.Table(executor, dbname)).Id;

        await PersistJob(executor, database, tableId, "items_price_not_null", SchemaElementKind.NotNull);
        await executor.Catalogs.ReplicateSetColumnNotNullAsync(database, "items", "price", notNull: true, "items_price_not_null");

        await Coordinator(executor).ResumeJobsAsync(database);

        Assert.IsTrue((await RowConstraintValidationScenarios.Table(executor, dbname)).Columns!.Single(c => c.Name == "price").NotNull);
        Assert.IsEmpty(await executor.Catalogs.LoadCoordinatorJobsAsync(database));
    }

    /// <summary>A job whose constraint never replicated, or was dropped since, is deleted, not retried.</summary>
    [Test]
    public async Task ResumedJobWithoutItsConstraintIsDeleted()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await RowConstraintValidationScenarios.CreateItems(executor, dbname, rows: 3);
        string tableId = (await RowConstraintValidationScenarios.Table(executor, dbname)).Id;

        await PersistJob(executor, database, tableId, "items_price_pos", SchemaElementKind.Check);

        await Coordinator(executor).ResumeJobsAsync(database);

        Assert.IsEmpty(await executor.Catalogs.LoadCoordinatorJobsAsync(database));
    }

    private static SchemaChangeCoordinator Coordinator(CommandExecutor executor) => new(executor.Catalogs)
    {
        RowConstraintValidationAsync = (db, table, kind, name) => executor.ValidateRowConstraintAsync(db, table, kind, name)
    };

    private static Task PersistJob(CommandExecutor executor, DatabaseDescriptor database, string tableId, string name, SchemaElementKind kind) =>
        executor.Catalogs.PersistCoordinatorJobAsync(database, new PersistedCoordinatorJob
        {
            TableName = "items",
            TableId = tableId,
            ElementName = name,
            TargetState = SchemaElementState.Public,
            ElementKind = kind,
        });

    private async Task Run(Func<CommandExecutor, string, Task> scenario)
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await scenario(executor, dbname);
    }
}
