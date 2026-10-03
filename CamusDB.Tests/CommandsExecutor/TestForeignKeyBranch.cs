/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna.Shared.KeyValue;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// One engine and one source database, plus a way to fork it. A scenario forks as deep as it needs,
/// and the fixture tracks every branch for cleanup.
/// </summary>
internal sealed record ForeignKeyBranchContext(CommandExecutor Executor, string Root, Func<string, Task<string>> Fork);

/// <summary>
/// Foreign keys on database branches. A fork copies the schema records unchanged, so a branch inherits
/// every constraint by id. The branch then enforces them against its own keys merged with the frozen
/// ancestry: a tombstone on the branch hides an inherited row, and a write in another database of the
/// lineage never changes a check here.
/// </summary>
internal static class ForeignKeyBranchScenarios
{
    private const string CreateCities =
        "CREATE TABLE cities (id int64 PRIMARY KEY NOT NULL, name string NOT NULL, UNIQUE KEY cities_name (name))";

    private const string CreateWeather =
        "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string, CONSTRAINT weather_city_fk FOREIGN KEY (city) REFERENCES cities (name))";

    /// <summary>
    /// The source has cities lima, quito and bogota. Lima has two children and quito one; bogota has
    /// none. One child has a NULL city.
    /// </summary>
    internal static async Task SeedWithConstraint(CommandExecutor executor, string dbname)
    {
        await ForeignKeyAlterScenarios.Ddl(executor, dbname, CreateCities);
        await ForeignKeyAlterScenarios.Ddl(executor, dbname, CreateWeather);
        await ForeignKeyAlterScenarios.Dml(executor, dbname, "INSERT INTO cities (id, name) VALUES (1, 'lima'), (2, 'quito'), (3, 'bogota')");
        await ForeignKeyAlterScenarios.Dml(executor, dbname, "INSERT INTO weather (id, city) VALUES (1, 'lima'), (2, 'quito'), (3, 'lima'), (4, NULL)");
    }

    /// <summary>The branch carries the constraint unchanged and enforces both sides of it.</summary>
    public static async Task BranchInheritsTheConstraintAndEnforcesBothSides(ForeignKeyBranchContext context)
    {
        await SeedWithConstraint(context.Executor, context.Root);
        string branch = await context.Fork(context.Root);

        DatabaseDescriptor source = await context.Executor.OpenDatabase(context.Root);
        DatabaseDescriptor forked = await context.Executor.OpenDatabase(branch);

        ForeignKeySchema inherited = forked.Schema.Tables["weather"].ForeignKeys!.Single();
        ForeignKeySchema original = source.Schema.Tables["weather"].ForeignKeys!.Single();

        Assert.AreEqual(original.Id, inherited.Id, "A fork must keep the constraint id");
        Assert.AreEqual(original.BackingIndexId, inherited.BackingIndexId);
        Assert.AreEqual(SchemaElementState.Public, inherited.State);
        Assert.IsTrue(forked.Schema.ForeignKeys.ChildPlansOf(forked.Schema.Tables["weather"].Id!).Single().IsEnforced);

        await ExpectCode(context.Executor, branch, "INSERT INTO weather (id, city) VALUES (900, 'atlantis')", CamusDBErrorCodes.ForeignKeyViolation);
        await ExpectCode(context.Executor, branch, "DELETE FROM cities WHERE id = 1", CamusDBErrorCodes.ForeignKeyRestrictDelete);
        await ExpectCode(context.Executor, branch, "UPDATE cities SET name = 'lima2' WHERE id = 2", CamusDBErrorCodes.ForeignKeyRestrictUpdate);

        await ForeignKeyAlterScenarios.Dml(context.Executor, branch, "INSERT INTO weather (id, city) VALUES (900, 'bogota')");
    }

    /// <summary>
    /// After the fork, each database checks against its own rows only. A parent that the source removes
    /// stays a parent on the branch, and a parent that the branch adds is unknown to the source.
    /// </summary>
    public static async Task WritesAfterTheForkStayInTheirOwnDatabase(ForeignKeyBranchContext context)
    {
        CommandExecutor executor = context.Executor;
        await SeedWithConstraint(executor, context.Root);
        string branch = await context.Fork(context.Root);

        // The source removes lima with its children, and adds paris.
        await ForeignKeyAlterScenarios.Dml(executor, context.Root, "DELETE FROM weather WHERE city = 'lima'");
        await ForeignKeyAlterScenarios.Dml(executor, context.Root, "DELETE FROM cities WHERE id = 1");
        await ForeignKeyAlterScenarios.Dml(executor, context.Root, "INSERT INTO cities (id, name) VALUES (10, 'paris')");

        // The branch still has lima and its children, and has no paris.
        await ExpectCode(executor, branch, "DELETE FROM cities WHERE id = 1", CamusDBErrorCodes.ForeignKeyRestrictDelete);
        await ForeignKeyAlterScenarios.Dml(executor, branch, "INSERT INTO weather (id, city) VALUES (100, 'lima')");
        await ExpectCode(executor, branch, "INSERT INTO weather (id, city) VALUES (101, 'paris')", CamusDBErrorCodes.ForeignKeyViolation);

        // The branch adds oslo with a child, and removes quito with its child.
        await ForeignKeyAlterScenarios.Dml(executor, branch, "INSERT INTO cities (id, name) VALUES (20, 'oslo')");
        await ForeignKeyAlterScenarios.Dml(executor, branch, "INSERT INTO weather (id, city) VALUES (102, 'oslo')");
        await ForeignKeyAlterScenarios.Dml(executor, branch, "DELETE FROM weather WHERE id = 2");
        await ForeignKeyAlterScenarios.Dml(executor, branch, "DELETE FROM cities WHERE id = 2");

        // The source does not see oslo, and quito still has its child there.
        await ExpectCode(executor, context.Root, "INSERT INTO weather (id, city) VALUES (102, 'oslo')", CamusDBErrorCodes.ForeignKeyViolation);
        await ExpectCode(executor, context.Root, "DELETE FROM cities WHERE id = 2", CamusDBErrorCodes.ForeignKeyRestrictDelete);
        await ForeignKeyAlterScenarios.Dml(executor, context.Root, "INSERT INTO weather (id, city) VALUES (103, 'paris')");

        Assert.IsEmpty(await ForeignKeyAlterScenarios.Orphans(executor, context.Root));
        Assert.IsEmpty(await ForeignKeyAlterScenarios.Orphans(executor, branch));
    }

    /// <summary>A parent and its child both inherited: the DELETE of the parent on the branch is refused.</summary>
    public static async Task DeleteOfAnInheritedParentWithAnInheritedChildIsRefused(ForeignKeyBranchContext context)
    {
        await SeedWithConstraint(context.Executor, context.Root);
        string branch = await context.Fork(context.Root);

        CamusDBException refused = await ExpectCode(context.Executor, branch, "DELETE FROM cities WHERE id = 2", CamusDBErrorCodes.ForeignKeyRestrictDelete);
        Assert.That(refused.Message, Does.Contain("key (name)=(quito)"));

        Assert.AreEqual(3, (await ForeignKeyAlterScenarios.Query(context.Executor, branch, "SELECT id FROM cities")).Count,
            "The refused statement must delete nothing");
    }

    /// <summary>
    /// The branch deletes the inherited child, then the inherited parent. The child's tombstone on the
    /// branch hides its inherited index entry, so the parent-side probe finds no child.
    /// </summary>
    public static async Task DeleteOfTheInheritedChildFirstLetsTheParentGo(ForeignKeyBranchContext context)
    {
        CommandExecutor executor = context.Executor;
        await SeedWithConstraint(executor, context.Root);
        string branch = await context.Fork(context.Root);

        await ForeignKeyAlterScenarios.Dml(executor, branch, "DELETE FROM weather WHERE id = 2");
        await ForeignKeyAlterScenarios.Dml(executor, branch, "DELETE FROM cities WHERE id = 2");

        // Lima has two inherited children. One tombstone is not enough.
        await ForeignKeyAlterScenarios.Dml(executor, branch, "DELETE FROM weather WHERE id = 1");
        await ExpectCode(executor, branch, "DELETE FROM cities WHERE id = 1", CamusDBErrorCodes.ForeignKeyRestrictDelete);

        await ForeignKeyAlterScenarios.Dml(executor, branch, "DELETE FROM weather WHERE id = 3");
        await ForeignKeyAlterScenarios.Dml(executor, branch, "DELETE FROM cities WHERE id = 1");

        Assert.AreEqual(1, (await ForeignKeyAlterScenarios.Query(executor, branch, "SELECT id FROM cities")).Count);
        Assert.AreEqual(3, (await ForeignKeyAlterScenarios.Query(executor, context.Root, "SELECT id FROM cities")).Count,
            "The source keeps its rows");
    }

    /// <summary>
    /// A child that references an inherited parent is accepted. A child that references a parent the
    /// branch deleted is refused: the tombstone on the branch hides the inherited unique entry.
    /// </summary>
    public static async Task ChildOfAnInheritedParentIsAcceptedAndOfADeletedOneRefused(ForeignKeyBranchContext context)
    {
        CommandExecutor executor = context.Executor;
        await SeedWithConstraint(executor, context.Root);
        string branch = await context.Fork(context.Root);

        await ForeignKeyAlterScenarios.Dml(executor, branch, "INSERT INTO weather (id, city) VALUES (100, 'bogota')");
        await ForeignKeyAlterScenarios.Dml(executor, branch, "DELETE FROM weather WHERE id = 100");
        await ForeignKeyAlterScenarios.Dml(executor, branch, "DELETE FROM cities WHERE id = 3");

        await ExpectCode(executor, branch, "INSERT INTO weather (id, city) VALUES (101, 'bogota')", CamusDBErrorCodes.ForeignKeyViolation);

        // The source still has bogota.
        await ForeignKeyAlterScenarios.Dml(executor, context.Root, "INSERT INTO weather (id, city) VALUES (101, 'bogota')");
    }

    /// <summary>
    /// ADD CONSTRAINT on a branch validates the merged view. The orphan exists only in the source, before
    /// the fork, so the branch inherits it and the validation must find it there.
    /// </summary>
    public static async Task AddOnABranchFindsAnOrphanThatOnlyAnAncestorHolds(ForeignKeyBranchContext context)
    {
        CommandExecutor executor = context.Executor;
        await ForeignKeyAlterScenarios.Seed(executor, context.Root, withUserIndex: false);
        await ForeignKeyAlterScenarios.Dml(executor, context.Root, "INSERT INTO weather (id, city) VALUES (50, 'atlantis')");

        string branch = await context.Fork(context.Root);

        CamusDBException refused = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ForeignKeyAlterScenarios.Ddl(executor, branch, ForeignKeyAlterScenarios.AddConstraint))!;
        Assert.AreEqual(CamusDBErrorCodes.ForeignKeyViolation, refused.Code, refused.Message);
        Assert.That(refused.Message, Does.Contain("atlantis"));

        DatabaseDescriptor forked = await executor.OpenDatabase(branch);
        Assert.IsNull(forked.Schema.Tables["weather"].ForeignKeys, "A refused ADD leaves no constraint");
        Assert.IsFalse(forked.Schema.Tables["weather"].Indexes?.Any(i => i.Name.StartsWith("~fk_", StringComparison.Ordinal)) == true,
            "A refused ADD leaves no owned index");

        // With the inherited orphan deleted on the branch, the same ADD succeeds. The backfilled index
        // holds the inherited children, so the parent side finds them.
        await ForeignKeyAlterScenarios.Dml(executor, branch, "DELETE FROM weather WHERE id = 50");
        await ForeignKeyAlterScenarios.Ddl(executor, branch, ForeignKeyAlterScenarios.AddConstraint);

        Assert.AreEqual(SchemaElementState.Public, forked.Schema.Tables["weather"].ForeignKeys!.Single().State);
        await ExpectCode(executor, branch, "DELETE FROM cities WHERE id = 1", CamusDBErrorCodes.ForeignKeyRestrictDelete);
        await ExpectCode(executor, branch, "INSERT INTO weather (id, city) VALUES (900, 'atlantis')", CamusDBErrorCodes.ForeignKeyViolation);

        DatabaseDescriptor source = await executor.OpenDatabase(context.Root);
        Assert.IsNull(source.Schema.Tables["weather"].ForeignKeys, "The source does not get the branch's constraint");
    }

    /// <summary>
    /// A branch of a branch. The middle level deletes quito's child; the leaf inherits lima's children
    /// from the root and quito's tombstone from the middle level.
    /// </summary>
    public static async Task BranchOfABranchMergesEveryLevel(ForeignKeyBranchContext context)
    {
        CommandExecutor executor = context.Executor;
        await SeedWithConstraint(executor, context.Root);
        string middle = await context.Fork(context.Root);

        await ForeignKeyAlterScenarios.Dml(executor, middle, "DELETE FROM weather WHERE id = 2");
        await ForeignKeyAlterScenarios.Dml(executor, middle, "DELETE FROM cities WHERE id = 3");

        string leaf = await context.Fork(middle);

        // Lima: parent and children come from the root.
        await ExpectCode(executor, leaf, "DELETE FROM cities WHERE id = 1", CamusDBErrorCodes.ForeignKeyRestrictDelete);
        await ForeignKeyAlterScenarios.Dml(executor, leaf, "INSERT INTO weather (id, city) VALUES (100, 'lima')");

        // Quito: the parent comes from the root, and its only child was deleted in the middle level.
        await ForeignKeyAlterScenarios.Dml(executor, leaf, "DELETE FROM cities WHERE id = 2");
        await ExpectCode(executor, leaf, "INSERT INTO weather (id, city) VALUES (101, 'quito')", CamusDBErrorCodes.ForeignKeyViolation);

        // Bogota: deleted in the middle level, so the leaf has no such parent.
        await ExpectCode(executor, leaf, "INSERT INTO weather (id, city) VALUES (102, 'bogota')", CamusDBErrorCodes.ForeignKeyViolation);

        // The middle level still has quito, and its child is gone there too.
        await ForeignKeyAlterScenarios.Dml(executor, middle, "INSERT INTO weather (id, city) VALUES (101, 'quito')");
    }

    /// <summary>
    /// A constraint that is still WriteOnly blocks a fork: the copy leaves out the job that would
    /// validate it. The refusal must hold even when no job is left, because the state alone says that
    /// the constraint is not validated. A fork after the constraint is Public succeeds.
    /// </summary>
    public static async Task ForkIsRefusedWhileAForeignKeyIsWriteOnly(ForeignKeyBranchContext context)
    {
        CommandExecutor executor = context.Executor;
        await ForeignKeyAlterScenarios.Seed(executor, context.Root, withUserIndex: true);

        executor.TestInterceptBeforeForeignKeyValidation = () =>
        {
            executor.TestInterceptBeforeForeignKeyValidation = null;
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "The coordinator stopped before the validation");
        };

        CamusDBException stopped = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ForeignKeyAlterScenarios.Ddl(executor, context.Root, ForeignKeyAlterScenarios.AddConstraint))!;
        Assert.That(stopped.Message, Does.Contain("stopped before the validation"));

        DatabaseDescriptor source = await executor.OpenDatabase(context.Root);
        Assert.AreEqual(SchemaElementState.WriteOnly, source.Schema.Tables["weather"].ForeignKeys!.Single().State);

        CamusDBException withJob = Assert.ThrowsAsync<CamusDBException>(async () => await context.Fork(context.Root))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, withJob.Code, withJob.Message);

        // Without the job, only the state shows that the constraint is not validated.
        await executor.Catalogs.DeleteCoordinatorJobAsync(source, source.Schema.Tables["weather"].Id!, "weather_city_fk");
        Assert.IsEmpty(await executor.Catalogs.LoadCoordinatorJobsAsync(source));

        CamusDBException withoutJob = Assert.ThrowsAsync<CamusDBException>(async () => await context.Fork(context.Root))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, withoutJob.Code, withoutJob.Message);
        Assert.That(withoutJob.Message, Does.Contain("Foreign key 'weather_city_fk'"));
        Assert.That(withoutJob.Message, Does.Contain("WriteOnly"));

        // Drop the stuck constraint, add it again, and fork once it is Public.
        await ForeignKeyAlterScenarios.Ddl(executor, context.Root, "ALTER TABLE weather DROP CONSTRAINT weather_city_fk");
        await ForeignKeyAlterScenarios.Ddl(executor, context.Root, ForeignKeyAlterScenarios.AddConstraint);

        string branch = await context.Fork(context.Root);
        DatabaseDescriptor forked = await executor.OpenDatabase(branch);
        Assert.AreEqual(SchemaElementState.Public, forked.Schema.Tables["weather"].ForeignKeys!.Single().State);
        await ExpectCode(executor, branch, "INSERT INTO weather (id, city) VALUES (900, 'atlantis')", CamusDBErrorCodes.ForeignKeyViolation);
    }

    /// <summary>
    /// A fork that starts while ADD CONSTRAINT runs waits for the statement, because both hold the
    /// source's DDL gate. It then copies the Public constraint, never the WriteOnly one.
    /// </summary>
    public static async Task ForkDuringAnAddWaitsAndInheritsThePublicConstraint(ForeignKeyBranchContext context)
    {
        CommandExecutor executor = context.Executor;
        await ForeignKeyAlterScenarios.Seed(executor, context.Root, withUserIndex: true);

        Task<string>? fork = null;

        executor.TestInterceptBeforeForeignKeyValidation = async () =>
        {
            executor.TestInterceptBeforeForeignKeyValidation = null;
            fork = Task.Run(() => context.Fork(context.Root));

            // Long enough for the fork to reach the gate; the assertion below holds either way.
            await Task.Delay(200);
            Assert.IsFalse(fork.IsCompleted, "The fork must wait for the ADD to finish");
        };

        await ForeignKeyAlterScenarios.Ddl(executor, context.Root, ForeignKeyAlterScenarios.AddConstraint);

        Assert.IsNotNull(fork, "The validation hook must run");
        string branch = await fork!;

        DatabaseDescriptor forked = await executor.OpenDatabase(branch);
        Assert.AreEqual(SchemaElementState.Public, forked.Schema.Tables["weather"].ForeignKeys!.Single().State);
    }

    /// <summary>DROP TABLE of a referenced parent is refused on a branch as it is on the source.</summary>
    public static async Task DropOfAReferencedParentOnABranchIsRefused(ForeignKeyBranchContext context)
    {
        await SeedWithConstraint(context.Executor, context.Root);
        string branch = await context.Fork(context.Root);

        CamusDBException refused = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ForeignKeyAlterScenarios.Ddl(context.Executor, branch, "DROP TABLE cities"))!;
        Assert.AreEqual(CamusDBErrorCodes.DependentObjectsExist, refused.Code, refused.Message);

        DatabaseDescriptor forked = await context.Executor.OpenDatabase(branch);
        Assert.IsTrue(forked.Schema.Tables.ContainsKey("cities"));

        // The child goes first, and then the parent can go.
        await ForeignKeyAlterScenarios.Ddl(context.Executor, branch, "DROP TABLE weather");
        await ForeignKeyAlterScenarios.Ddl(context.Executor, branch, "DROP TABLE cities");

        DatabaseDescriptor source = await context.Executor.OpenDatabase(context.Root);
        Assert.IsTrue(source.Schema.Tables.ContainsKey("cities"), "A drop on the branch must not touch the source");
    }

    /// <summary>
    /// A reference names a table in its own database. A qualified name — the source of the branch, or
    /// any other database — is refused in CREATE TABLE and in ALTER TABLE.
    /// </summary>
    public static async Task AReferenceToAnotherDatabaseIsRefused(ForeignKeyBranchContext context)
    {
        CommandExecutor executor = context.Executor;
        await ForeignKeyAlterScenarios.Seed(executor, context.Root, withUserIndex: false);

        // Two levels, so the qualified name is a branch name: a generated root name can start with a
        // digit, which does not lex as an identifier.
        string source = await context.Fork(context.Root);
        string branch = await context.Fork(source);

        CamusDBException create = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ForeignKeyAlterScenarios.Ddl(executor, branch,
                $"CREATE TABLE visits (id int64 PRIMARY KEY NOT NULL, city string REFERENCES {source}.cities (name))"))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidForeignKeyDefinition, create.Code, create.Message);
        Assert.That(create.Message, Does.Contain("current database"));

        CamusDBException alter = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ForeignKeyAlterScenarios.Ddl(executor, branch,
                $"ALTER TABLE weather ADD CONSTRAINT weather_city_fk FOREIGN KEY (city) REFERENCES {source}.cities (name)"))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidForeignKeyDefinition, alter.Code, alter.Message);

        // The ticket API takes the name as it is, with no parse step to strip a qualifier.
        CamusDBException ticket = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.AlterConstraint(new AlterConstraintTicket(
                branch, "weather", "weather_city_fk", expression: null, referencedColumns: null, AlterConstraintOperation.AddForeignKey,
                foreignKey: new ForeignKeyInfo("weather_city_fk", ["city"], $"{source}.cities", ["name"]))))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidForeignKeyDefinition, ticket.Code, ticket.Message);
        Assert.That(ticket.Message, Does.Contain("own database"));

        DatabaseDescriptor forked = await executor.OpenDatabase(branch);
        Assert.IsFalse(forked.Schema.Tables.ContainsKey("visits"));
        Assert.IsNull(forked.Schema.Tables["weather"].ForeignKeys);
    }

    /// <summary>
    /// The cost of a negative child-side lookup on a branch of depth 2. A hundred distinct keys that no
    /// level holds take one batched read of the branch's own keys and one batched read per ancestry
    /// level, not one read per key.
    /// </summary>
    public static async Task NegativeLookupOnADepthTwoBranchBatchesPerLevel(ForeignKeyBranchContext context)
    {
        CommandExecutor executor = context.Executor;
        await SeedWithConstraint(executor, context.Root);
        string middle = await context.Fork(context.Root);
        string leaf = await context.Fork(middle);

        StringBuilder sql = new("INSERT INTO weather (id, city) VALUES ");
        for (int i = 0; i < 100; i++)
        {
            if (i > 0)
                sql.Append(", ");
            sql.Append('(').Append(1000 + i).Append(", 'nowhere").Append(i).Append("')");
        }

        // Warm the descriptors, so the counted statement does no first-open work.
        await ForeignKeyAlterScenarios.Query(executor, leaf, "SELECT id FROM cities");
        await ForeignKeyAlterScenarios.Query(executor, leaf, "SELECT id FROM weather");

        using ForeignKeyOperationCounter counter = ForeignKeyOperationCounter.Start();
        await ExpectCode(executor, leaf, sql.ToString(), CamusDBErrorCodes.ForeignKeyViolation);

        Assert.AreEqual(1, counter.Count("child_probe_batch"), "One batched read of the branch's own keys");
        Assert.AreEqual(100, counter.Count("child_probe_key"));
        Assert.AreEqual(2, counter.Count("ancestor_probe_batch"), "One batched read per ancestry level");
    }

    internal static async Task<CamusDBException> ExpectCode(CommandExecutor executor, string dbname, string sql, string code)
    {
        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ForeignKeyAlterScenarios.Dml(executor, dbname, sql))!;
        Assert.AreEqual(code, exception.Code, exception.Message);
        return exception;
    }
}

/// <summary>
/// The lock rendezvous on a branch, with a parent that the branch inherited. The branch deletes the
/// parent by writing a tombstone to its own copy of the parent's unique-index key, and a child locks
/// that same key, so the two still meet on one key.
/// </summary>
internal static class ForeignKeyBranchRendezvousScenarios
{
    /// <summary>The child locks first and commits while the older parent DELETE waits; the DELETE then sees it.</summary>
    public static async Task DeleteWaitsForAnOpenChildAndThenRefuses(ForeignKeyBranchContext context)
    {
        await ForeignKeyBranchScenarios.SeedWithConstraint(context.Executor, context.Root);
        string branch = await context.Fork(context.Root);
        DatabaseDescriptor database = await context.Executor.OpenDatabase(branch);

        KvTransaction parentTx = await database.Transactions.BeginAsync();
        KvTransaction childTx = await database.Transactions.BeginAsync();
        try
        {
            await Run(context.Executor, childTx, branch, "INSERT INTO weather (id, city) VALUES (100, 'bogota')");

            Task delete = Run(context.Executor, parentTx, branch, "DELETE FROM cities WHERE id = 3");

            await Task.Delay(100);
            await database.Transactions.CommitAsync(childTx);

            CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () => await delete)!;
            Assert.AreEqual(CamusDBErrorCodes.ForeignKeyRestrictDelete, exception.Code, exception.Message);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(childTx);
            await database.Transactions.RollbackIfNotCompletedAsync(parentTx);
        }

        Assert.AreEqual(3, (await ForeignKeyAlterScenarios.Query(context.Executor, branch, "SELECT id FROM cities")).Count);
        Assert.IsEmpty(await ForeignKeyAlterScenarios.Orphans(context.Executor, branch));
    }

    /// <summary>The parent DELETE writes first and stays open; a child insert must not commit against it.</summary>
    public static async Task ChildCannotCommitAgainstAnOpenDelete(ForeignKeyBranchContext context)
    {
        await ForeignKeyBranchScenarios.SeedWithConstraint(context.Executor, context.Root);
        string branch = await context.Fork(context.Root);
        DatabaseDescriptor database = await context.Executor.OpenDatabase(branch);

        KvTransaction parentTx = await database.Transactions.BeginAsync();
        try
        {
            await Run(context.Executor, parentTx, branch, "DELETE FROM cities WHERE id = 3");

            Task child = ForeignKeyAlterScenarios.Dml(context.Executor, branch, "INSERT INTO weather (id, city) VALUES (100, 'bogota')");

            await Task.Delay(100);
            await database.Transactions.CommitAsync(parentTx);

            CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () => await child)!;
            Assert.That(exception.Code, Is.AnyOf(
                CamusDBErrorCodes.ForeignKeyViolation,
                CamusDBErrorCodes.TransactionMustRetry,
                CamusDBErrorCodes.TransactionConflict), exception.Message);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(parentTx);
        }

        Assert.IsEmpty(await ForeignKeyAlterScenarios.Orphans(context.Executor, branch), "No child may reference the deleted parent");
        Assert.AreEqual(2, (await ForeignKeyAlterScenarios.Query(context.Executor, branch, "SELECT id FROM cities")).Count);
    }

    private static async Task Run(CommandExecutor executor, KvTransaction tx, string dbname, string sql) =>
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
}

/// <summary>Foreign keys on branches, on a standalone engine.</summary>
[TestFixture]
[NonParallelizable]
public sealed class TestForeignKeyBranch : BaseTest
{
    [Test] public async Task BranchInheritsTheConstraintAndEnforcesBothSides() => await Run(ForeignKeyBranchScenarios.BranchInheritsTheConstraintAndEnforcesBothSides);
    [Test] public async Task WritesAfterTheForkStayInTheirOwnDatabase() => await Run(ForeignKeyBranchScenarios.WritesAfterTheForkStayInTheirOwnDatabase);
    [Test] public async Task DeleteOfAnInheritedParentWithAnInheritedChildIsRefused() => await Run(ForeignKeyBranchScenarios.DeleteOfAnInheritedParentWithAnInheritedChildIsRefused);
    [Test] public async Task DeleteOfTheInheritedChildFirstLetsTheParentGo() => await Run(ForeignKeyBranchScenarios.DeleteOfTheInheritedChildFirstLetsTheParentGo);
    [Test] public async Task ChildOfAnInheritedParentIsAcceptedAndOfADeletedOneRefused() => await Run(ForeignKeyBranchScenarios.ChildOfAnInheritedParentIsAcceptedAndOfADeletedOneRefused);
    [Test] public async Task AddOnABranchFindsAnOrphanThatOnlyAnAncestorHolds() => await Run(ForeignKeyBranchScenarios.AddOnABranchFindsAnOrphanThatOnlyAnAncestorHolds);
    [Test] public async Task BranchOfABranchMergesEveryLevel() => await Run(ForeignKeyBranchScenarios.BranchOfABranchMergesEveryLevel);
    [Test] public async Task ForkIsRefusedWhileAForeignKeyIsWriteOnly() => await Run(ForeignKeyBranchScenarios.ForkIsRefusedWhileAForeignKeyIsWriteOnly);
    [Test] public async Task ForkDuringAnAddWaitsAndInheritsThePublicConstraint() => await Run(ForeignKeyBranchScenarios.ForkDuringAnAddWaitsAndInheritsThePublicConstraint);
    [Test] public async Task DropOfAReferencedParentOnABranchIsRefused() => await Run(ForeignKeyBranchScenarios.DropOfAReferencedParentOnABranchIsRefused);
    [Test] public async Task AReferenceToAnotherDatabaseIsRefused() => await Run(ForeignKeyBranchScenarios.AReferenceToAnotherDatabaseIsRefused);
    [Test] public async Task NegativeLookupOnADepthTwoBranchBatchesPerLevel() => await Run(ForeignKeyBranchScenarios.NegativeLookupOnADepthTwoBranchBatchesPerLevel);

    private async Task Run(Func<ForeignKeyBranchContext, Task> scenario)
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await scenario(new ForeignKeyBranchContext(executor, dbname, source => ForkAsync(executor, source)));
    }

    private async Task<string> ForkAsync(CommandExecutor executor, string source)
    {
        string name = "db_" + Guid.NewGuid().ToString("n");
        await executor.CreateDatabase(new CreateDatabaseTicket(name, ifNotExists: false, branchFrom: source));
        TrackDatabase(name, executor);
        return name;
    }
}

/// <summary>Foreign keys on branches, on a cluster-mode engine.</summary>
[TestFixture]
[NonParallelizable]
public sealed class TestForeignKeyBranchCluster : SharedNodeBaseTest
{
    [Test] public async Task BranchInheritsTheConstraintAndEnforcesBothSides() => await Run(ForeignKeyBranchScenarios.BranchInheritsTheConstraintAndEnforcesBothSides);
    [Test] public async Task WritesAfterTheForkStayInTheirOwnDatabase() => await Run(ForeignKeyBranchScenarios.WritesAfterTheForkStayInTheirOwnDatabase);
    [Test] public async Task DeleteOfAnInheritedParentWithAnInheritedChildIsRefused() => await Run(ForeignKeyBranchScenarios.DeleteOfAnInheritedParentWithAnInheritedChildIsRefused);
    [Test] public async Task DeleteOfTheInheritedChildFirstLetsTheParentGo() => await Run(ForeignKeyBranchScenarios.DeleteOfTheInheritedChildFirstLetsTheParentGo);
    [Test] public async Task ChildOfAnInheritedParentIsAcceptedAndOfADeletedOneRefused() => await Run(ForeignKeyBranchScenarios.ChildOfAnInheritedParentIsAcceptedAndOfADeletedOneRefused);
    [Test] public async Task AddOnABranchFindsAnOrphanThatOnlyAnAncestorHolds() => await Run(ForeignKeyBranchScenarios.AddOnABranchFindsAnOrphanThatOnlyAnAncestorHolds);
    [Test] public async Task BranchOfABranchMergesEveryLevel() => await Run(ForeignKeyBranchScenarios.BranchOfABranchMergesEveryLevel);
    [Test] public async Task ForkIsRefusedWhileAForeignKeyIsWriteOnly() => await Run(ForeignKeyBranchScenarios.ForkIsRefusedWhileAForeignKeyIsWriteOnly);
    [Test] public async Task ForkDuringAnAddWaitsAndInheritsThePublicConstraint() => await Run(ForeignKeyBranchScenarios.ForkDuringAnAddWaitsAndInheritsThePublicConstraint);
    [Test] public async Task DropOfAReferencedParentOnABranchIsRefused() => await Run(ForeignKeyBranchScenarios.DropOfAReferencedParentOnABranchIsRefused);
    [Test] public async Task AReferenceToAnotherDatabaseIsRefused() => await Run(ForeignKeyBranchScenarios.AReferenceToAnotherDatabaseIsRefused);
    [Test] public async Task NegativeLookupOnADepthTwoBranchBatchesPerLevel() => await Run(ForeignKeyBranchScenarios.NegativeLookupOnADepthTwoBranchBatchesPerLevel);

    private async Task Run(Func<ForeignKeyBranchContext, Task> scenario)
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await scenario(new ForeignKeyBranchContext(executor, dbname, source => ForkAsync(executor, source)));
    }

    private async Task<string> ForkAsync(CommandExecutor executor, string source)
    {
        string name = "db_" + Guid.NewGuid().ToString("n");
        await executor.CreateDatabase(new CreateDatabaseTicket(name, ifNotExists: false, branchFrom: source));
        TrackDatabase(name, executor);
        return name;
    }
}

/// <summary>The rendezvous on a branch, in each isolation and locking cell, on a standalone engine.</summary>
[TestFixture(CamusIsolationLevel.ReadCommitted, KeyValueTransactionLocking.Pessimistic)]
[TestFixture(CamusIsolationLevel.Serializable, KeyValueTransactionLocking.Pessimistic)]
[TestFixture(CamusIsolationLevel.ReadCommitted, KeyValueTransactionLocking.Optimistic)]
[NonParallelizable]
public sealed class TestForeignKeyBranchRendezvous : BaseTest
{
    private readonly CamusIsolationLevel isolation;
    private readonly KeyValueTransactionLocking locking;

    public TestForeignKeyBranchRendezvous(CamusIsolationLevel isolation, KeyValueTransactionLocking locking)
    {
        this.isolation = isolation;
        this.locking = locking;
    }

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) =>
        defaults with { DefaultIsolationLevel = isolation, DefaultTransactionLocking = locking };

    [Test] public async Task DeleteWaitsForAnOpenChildAndThenRefuses() => await Run(ForeignKeyBranchRendezvousScenarios.DeleteWaitsForAnOpenChildAndThenRefuses);
    [Test] public async Task ChildCannotCommitAgainstAnOpenDelete() => await Run(ForeignKeyBranchRendezvousScenarios.ChildCannotCommitAgainstAnOpenDelete);

    private async Task Run(Func<ForeignKeyBranchContext, Task> scenario)
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await scenario(new ForeignKeyBranchContext(executor, dbname, source => ForkAsync(executor, source)));
    }

    private async Task<string> ForkAsync(CommandExecutor executor, string source)
    {
        string name = "db_" + Guid.NewGuid().ToString("n");
        await executor.CreateDatabase(new CreateDatabaseTicket(name, ifNotExists: false, branchFrom: source));
        TrackDatabase(name, executor);
        return name;
    }
}

/// <summary>The rendezvous on a branch, in each isolation and locking cell, on a cluster-mode engine.</summary>
[TestFixture(CamusIsolationLevel.ReadCommitted, KeyValueTransactionLocking.Pessimistic)]
[TestFixture(CamusIsolationLevel.Serializable, KeyValueTransactionLocking.Pessimistic)]
[TestFixture(CamusIsolationLevel.ReadCommitted, KeyValueTransactionLocking.Optimistic)]
[NonParallelizable]
public sealed class TestForeignKeyBranchRendezvousCluster : SharedNodeBaseTest
{
    private readonly CamusIsolationLevel isolation;
    private readonly KeyValueTransactionLocking locking;

    public TestForeignKeyBranchRendezvousCluster(CamusIsolationLevel isolation, KeyValueTransactionLocking locking)
    {
        this.isolation = isolation;
        this.locking = locking;
    }

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) =>
        defaults with { DefaultIsolationLevel = isolation, DefaultTransactionLocking = locking };

    [Test] public async Task DeleteWaitsForAnOpenChildAndThenRefuses() => await Run(ForeignKeyBranchRendezvousScenarios.DeleteWaitsForAnOpenChildAndThenRefuses);
    [Test] public async Task ChildCannotCommitAgainstAnOpenDelete() => await Run(ForeignKeyBranchRendezvousScenarios.ChildCannotCommitAgainstAnOpenDelete);

    private async Task Run(Func<ForeignKeyBranchContext, Task> scenario)
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await scenario(new ForeignKeyBranchContext(executor, dbname, source => ForkAsync(executor, source)));
    }

    private async Task<string> ForkAsync(CommandExecutor executor, string source)
    {
        string name = "db_" + Guid.NewGuid().ToString("n");
        await executor.CreateDatabase(new CreateDatabaseTicket(name, ifNotExists: false, branchFrom: source));
        TrackDatabase(name, executor);
        return name;
    }
}
