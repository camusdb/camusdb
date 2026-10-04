/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.DDL;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Edges where a foreign key could pass a check it should fail, or fail one it should pass:
/// <list type="bullet">
/// <item>a backing index that holds no entry for some children;</item>
/// <item>a validation pass that pages through a descending index;</item>
/// <item>a recheck whose two reads could see two different states;</item>
/// <item>a constraint name that two kinds of constraint share.</item>
/// </list>
/// Every scenario drives the SQL entry point, on a standalone engine and on a cluster-mode engine.
/// </summary>
internal static class ForeignKeyIntegrityEdgeScenarios
{
    private const string CreateParents = "CREATE TABLE parents (id int64 PRIMARY KEY NOT NULL)";

    private const string AddConstraint =
        "ALTER TABLE children ADD CONSTRAINT child_fk FOREIGN KEY (pid) REFERENCES parents (id)";

    /// <summary>
    /// A unique index wider than the constraint holds no entry for a row with a NULL in its extra
    /// column. It must not back the constraint: the parent DELETE probes the backing index and would
    /// not see the child. An owned index takes its place.
    /// </summary>
    public static async Task WiderUniqueIndexDoesNotHideAChildWithANullSuffix(ForeignKeyBranchContext context)
    {
        (CommandExecutor executor, string db) = (context.Executor, context.Root);

        await Ddl(executor, db, "CREATE TABLE parents (id int64 PRIMARY KEY NOT NULL, code int64 NOT NULL, UNIQUE KEY parents_code (code))");
        await Ddl(executor, db,
            "CREATE TABLE children (id int64 PRIMARY KEY NOT NULL, pid int64, tail int64, UNIQUE KEY wide (pid, tail), " +
            "CONSTRAINT child_fk FOREIGN KEY (pid) REFERENCES parents (code))");

        DatabaseDescriptor database = await executor.OpenDatabase(db);
        TableSchema children = database.Schema.Tables["children"];
        ForeignKeySchema constraint = children.ForeignKeys!.Single();
        TableIndexSchema backing = children.Indexes!.Single(i => i.KvId == constraint.BackingIndexId);

        Assert.AreEqual("~fk_child_fk", backing.Name, "The wider unique index must not back the constraint");
        Assert.AreEqual(IndexType.Multi, backing.Type);

        await Dml(executor, db, "INSERT INTO parents (id, code) VALUES (1, 1)");
        await Dml(executor, db, "INSERT INTO children (id, pid, tail) VALUES (1, 1, NULL)");

        await ExpectCode(executor, db, "DELETE FROM parents WHERE id = 1", CamusDBErrorCodes.ForeignKeyRestrictDelete);
        await ExpectCode(executor, db, "UPDATE parents SET code = 2 WHERE id = 1", CamusDBErrorCodes.ForeignKeyRestrictUpdate);
    }

    /// <summary>The ALTER path: the existing orphan has a NULL suffix, and the validation must still see it.</summary>
    public static async Task AddFindsAnOrphanThatAWiderUniqueIndexOmits(ForeignKeyBranchContext context)
    {
        (CommandExecutor executor, string db) = (context.Executor, context.Root);

        await Ddl(executor, db, CreateParents);
        await Ddl(executor, db, "CREATE TABLE children (id int64 PRIMARY KEY NOT NULL, pid int64, tail int64, UNIQUE KEY wide (pid, tail))");
        await Dml(executor, db, "INSERT INTO children (id, pid, tail) VALUES (1, 99, NULL)");

        CamusDBException refused = ExpectDdlCode(executor, db, AddConstraint, CamusDBErrorCodes.ForeignKeyViolation);
        Assert.That(refused.Message, Does.Contain("(pid)=(99)"), refused.Message);

        DatabaseDescriptor database = await executor.OpenDatabase(db);
        Assert.IsNull(database.Schema.Tables["children"].ForeignKeys, "A refused ADD leaves no constraint");
    }

    /// <summary>
    /// A unique index of exactly the constraint's width is still reused: it omits only a row with a
    /// NULL in a constraint column, which MATCH SIMPLE does not check.
    /// </summary>
    public static async Task ExactWidthUniqueIndexIsStillReused(ForeignKeyBranchContext context)
    {
        (CommandExecutor executor, string db) = (context.Executor, context.Root);

        await Ddl(executor, db, CreateParents);
        await Ddl(executor, db,
            "CREATE TABLE children (id int64 PRIMARY KEY NOT NULL, pid int64, UNIQUE KEY one_per_parent (pid), " +
            "CONSTRAINT child_fk FOREIGN KEY (pid) REFERENCES parents (id))");

        DatabaseDescriptor database = await executor.OpenDatabase(db);
        TableSchema children = database.Schema.Tables["children"];

        Assert.AreEqual("one_per_parent", children.Indexes!.Single(i => i.KvId == children.ForeignKeys!.Single().BackingIndexId).Name);
        Assert.IsFalse(children.Indexes!.Any(i => i.Name.StartsWith("~fk_", StringComparison.Ordinal)));

        await Dml(executor, db, "INSERT INTO parents (id) VALUES (1)");
        await Dml(executor, db, "INSERT INTO children (id, pid) VALUES (1, 1)");
        await ExpectCode(executor, db, "DELETE FROM parents WHERE id = 1", CamusDBErrorCodes.ForeignKeyRestrictDelete);
    }

    /// <summary>
    /// 1025 children on a descending index: the first page holds keys 1025 down to 2, and the orphan,
    /// key 1, is on the second page. The page cursor follows the index order, so the pass reaches it.
    /// </summary>
    public static async Task DescendingValidationReachesAnOrphanAfterTheFirstPage(ForeignKeyBranchContext context)
    {
        (CommandExecutor executor, string db) = (context.Executor, context.Root);
        await SeedDescending(executor, db, firstParent: 2);

        CamusDBException refused = ExpectDdlCode(executor, db, AddConstraint, CamusDBErrorCodes.ForeignKeyViolation);
        Assert.That(refused.Message, Does.Contain("(pid)=(1)"), refused.Message);
    }

    /// <summary>
    /// The same table with every parent present validates, and each distinct key is checked once:
    /// the cursor neither skips a key nor reads one twice.
    /// </summary>
    public static async Task DescendingValidationChecksEveryKeyOnce(ForeignKeyBranchContext context)
    {
        (CommandExecutor executor, string db) = (context.Executor, context.Root);
        await SeedDescending(executor, db, firstParent: 1);

        using ForeignKeyOperationCounter counter = ForeignKeyOperationCounter.Start();
        await Ddl(executor, db, AddConstraint);

        Assert.AreEqual(Children, counter.Count("validation_key"));

        DatabaseDescriptor database = await executor.OpenDatabase(db);
        Assert.AreEqual(SchemaElementState.Public, database.Schema.Tables["children"].ForeignKeys!.Single().State);
    }

    /// <summary>
    /// A composite index with one descending and one ascending column, over two pages. The first page
    /// ends at (39, 2); in index order the next key is (38, 1), which is greater in the first column's
    /// direction and smaller in value. The orphan (1, 1) is on the second page.
    /// </summary>
    public static async Task MixedDirectionValidationReachesAnOrphanOnTheSecondPage(ForeignKeyBranchContext context)
    {
        (CommandExecutor executor, string db) = (context.Executor, context.Root);

        await Ddl(executor, db, "CREATE TABLE regions (id int64 PRIMARY KEY NOT NULL, a int64 NOT NULL, b int64 NOT NULL, UNIQUE KEY regions_ab (a, b))");
        await Ddl(executor, db, "CREATE TABLE places (id int64 PRIMARY KEY NOT NULL, a int64, b int64, KEY by_region (a DESC, b))");

        const int Regions = 550;
        System.Collections.Generic.List<(int a, int b)> keys = [];
        for (int a = 1; a <= Regions; a++)
        {
            keys.Add((a, 1));
            keys.Add((a, 2));
        }

        await Dml(executor, db, "INSERT INTO regions (id, a, b) VALUES " +
            string.Join(", ", keys.Select((k, i) => (k, i)).Where(x => x.k != (1, 1)).Select(x => $"({x.i + 1}, {x.k.a}, {x.k.b})")));
        await Dml(executor, db, "INSERT INTO places (id, a, b) VALUES " +
            string.Join(", ", keys.Select((k, i) => $"({i + 1}, {k.a}, {k.b})")));

        CamusDBException refused = ExpectDdlCode(executor, db,
            "ALTER TABLE places ADD CONSTRAINT place_region_fk FOREIGN KEY (a, b) REFERENCES regions (a, b)",
            CamusDBErrorCodes.ForeignKeyViolation);
        Assert.That(refused.Message, Does.Contain("(a, b)=(1, 1)"), refused.Message);
    }

    /// <summary>
    /// On a branch, the pages merge the branch with its ancestry. Every child is inherited, and the
    /// orphan is on the second page.
    /// </summary>
    public static async Task DescendingValidationOnABranchReachesAnInheritedOrphan(ForeignKeyBranchContext context)
    {
        CommandExecutor executor = context.Executor;
        await SeedDescending(executor, context.Root, firstParent: 2);

        string branch = await context.Fork(context.Root);

        CamusDBException refused = ExpectDdlCode(executor, branch, AddConstraint, CamusDBErrorCodes.ForeignKeyViolation);
        Assert.That(refused.Message, Does.Contain("(pid)=(1)"), refused.Message);

        // With the parent created on the branch only, the same ADD passes there.
        await Dml(executor, branch, "INSERT INTO parents (id) VALUES (1)");
        await Ddl(executor, branch, AddConstraint);
    }

    /// <summary>
    /// The recheck reads the child and the parent at one snapshot. Key 99 is an orphan when the page
    /// reads it. Then, before the recheck, its parent is inserted. Between the recheck's two reads,
    /// the child and then the parent are deleted. At no committed point after the parent insert was
    /// there an orphan, so the validation must pass. Two Read Committed reads would see the child,
    /// then no parent, and report one.
    /// </summary>
    public static async Task RecheckDoesNotJoinTwoStatesIntoAFalseOrphan(ForeignKeyBranchContext context)
    {
        (CommandExecutor executor, string db) = (context.Executor, context.Root);

        await Ddl(executor, db, CreateParents);
        await Ddl(executor, db, "CREATE TABLE children (id int64 PRIMARY KEY NOT NULL, pid int64, KEY by_parent (pid))");
        await Dml(executor, db, "INSERT INTO children (id, pid) VALUES (1, 99)");

        int beforeRecheck = 0;
        int betweenReads = 0;

        ForeignKeyValidationPass.BeforeRecheckForTesting = async () =>
        {
            if (beforeRecheck++ == 0)
                await Dml(executor, db, "INSERT INTO parents (id) VALUES (99)");
        };

        ForeignKeyValidationPass.BetweenRecheckReadsForTesting = async () =>
        {
            if (betweenReads++ == 0)
            {
                await Dml(executor, db, "DELETE FROM children WHERE id = 1");
                await Dml(executor, db, "DELETE FROM parents WHERE id = 99");
            }
        };

        try
        {
            await Ddl(executor, db, AddConstraint);
        }
        finally
        {
            ForeignKeyValidationPass.BeforeRecheckForTesting = null;
            ForeignKeyValidationPass.BetweenRecheckReadsForTesting = null;
        }

        Assert.AreEqual(1, beforeRecheck, "The page must have found key 99 without its parent");
        Assert.AreEqual(1, betweenReads, "The recheck must have read the child");

        DatabaseDescriptor database = await executor.OpenDatabase(db);
        Assert.AreEqual(SchemaElementState.Public, database.Schema.Tables["children"].ForeignKeys!.Single().State);
        await ExpectCode(executor, db, "INSERT INTO children (id, pid) VALUES (2, 99)", CamusDBErrorCodes.ForeignKeyViolation);
    }

    /// <summary>
    /// A CHECK must not take the name of a foreign key. If it could, DROP CONSTRAINT would remove the
    /// CHECK, which wins the name lookup, and keep the foreign key.
    /// </summary>
    public static async Task CheckCannotTakeAForeignKeyName(ForeignKeyBranchContext context)
    {
        (CommandExecutor executor, string db) = (context.Executor, context.Root);

        await Ddl(executor, db, CreateParents);
        await Ddl(executor, db, "CREATE TABLE children (id int64 PRIMARY KEY NOT NULL, pid int64, CONSTRAINT child_fk FOREIGN KEY (pid) REFERENCES parents (id))");

        ExpectDdlCode(executor, db, "ALTER TABLE children ADD CONSTRAINT child_fk CHECK (id > 0)", CamusDBErrorCodes.InvalidInput);
        ExpectDdlCode(executor, db, "ALTER TABLE children ADD CONSTRAINT CHILD_FK CHECK (id > 0)", CamusDBErrorCodes.InvalidInput);

        DatabaseDescriptor database = await executor.OpenDatabase(db);
        TableSchema children = database.Schema.Tables["children"];
        Assert.IsTrue(children.CheckConstraints is null || children.CheckConstraints.Count == 0, "A refused ADD CHECK leaves no CHECK");
        Assert.AreEqual("child_fk", children.ForeignKeys!.Single().Name);

        await Ddl(executor, db, "ALTER TABLE children DROP CONSTRAINT child_fk");
        Assert.IsTrue(database.Schema.Tables["children"].ForeignKeys is null or { Count: 0 }, "DROP CONSTRAINT removes the foreign key");
    }

    /// <summary>The other order: a foreign key must not take the name of a CHECK.</summary>
    public static async Task ForeignKeyCannotTakeACheckName(ForeignKeyBranchContext context)
    {
        (CommandExecutor executor, string db) = (context.Executor, context.Root);

        await Ddl(executor, db, CreateParents);
        await Ddl(executor, db, "CREATE TABLE children (id int64 PRIMARY KEY NOT NULL, pid int64)");
        await Ddl(executor, db, "ALTER TABLE children ADD CONSTRAINT child_fk CHECK (id > 0)");

        ExpectDdlCode(executor, db, AddConstraint, CamusDBErrorCodes.InvalidInput);

        DatabaseDescriptor database = await executor.OpenDatabase(db);
        Assert.IsNull(database.Schema.Tables["children"].ForeignKeys);
    }

    /// <summary>A CHECK must not take the name of a named NOT NULL constraint.</summary>
    public static async Task CheckCannotTakeANotNullName(ForeignKeyBranchContext context)
    {
        (CommandExecutor executor, string db) = (context.Executor, context.Root);

        await Ddl(executor, db, "CREATE TABLE children (id int64 PRIMARY KEY NOT NULL, pid int64)");
        await Ddl(executor, db, "ALTER TABLE children ALTER COLUMN pid SET NOT NULL");

        ExpectDdlCode(executor, db, "ALTER TABLE children ADD CONSTRAINT children_pid_not_null CHECK (id > 0)", CamusDBErrorCodes.InvalidInput);
    }

    /// <summary>
    /// SET NOT NULL names its constraint <c>{table}_{column}_not_null</c>. When a foreign key already
    /// has that name, the statement is refused and the column keeps NULL allowed.
    /// </summary>
    public static async Task SetNotNullCannotTakeAForeignKeyName(ForeignKeyBranchContext context)
    {
        (CommandExecutor executor, string db) = (context.Executor, context.Root);

        await Ddl(executor, db, CreateParents);
        await Ddl(executor, db,
            "CREATE TABLE children (id int64 PRIMARY KEY NOT NULL, pid int64, CONSTRAINT children_pid_not_null FOREIGN KEY (pid) REFERENCES parents (id))");

        ExpectDdlCode(executor, db, "ALTER TABLE children ALTER COLUMN pid SET NOT NULL", CamusDBErrorCodes.InvalidInput);

        DatabaseDescriptor database = await executor.OpenDatabase(db);
        TableColumnSchema pid = database.Schema.Tables["children"].Columns!.Single(c => c.Name == "pid");
        Assert.IsFalse(pid.NotNull);
        Assert.IsNull(pid.NotNullConstraintName);

        // SET NOT NULL on a column that already has it keeps its own name: the statement is idempotent.
        await Ddl(executor, db, "ALTER TABLE children ALTER COLUMN id SET NOT NULL");
        await Ddl(executor, db, "ALTER TABLE children ALTER COLUMN id SET NOT NULL");
    }

    private const int Children = 1025;

    /// <summary>
    /// Parents from <paramref name="firstParent"/> to 1025, and children 1 to 1025, each referencing
    /// the parent with its own id, on a descending index and with no constraint yet.
    /// </summary>
    private static async Task SeedDescending(CommandExecutor executor, string db, int firstParent)
    {
        await Ddl(executor, db, CreateParents);
        await Ddl(executor, db, "CREATE TABLE children (id int64 PRIMARY KEY NOT NULL, pid int64, KEY by_parent (pid DESC))");
        await Dml(executor, db, "INSERT INTO parents (id) VALUES " +
            string.Join(", ", Enumerable.Range(firstParent, Children - firstParent + 1).Select(i => $"({i})")));
        await Dml(executor, db, "INSERT INTO children (id, pid) VALUES " +
            string.Join(", ", Enumerable.Range(1, Children).Select(i => $"({i}, {i})")));
    }

    private static Task Ddl(CommandExecutor executor, string db, string sql) => ForeignKeyAlterScenarios.Ddl(executor, db, sql);

    private static Task Dml(CommandExecutor executor, string db, string sql) => ForeignKeyAlterScenarios.Dml(executor, db, sql);

    private static Task<CamusDBException> ExpectCode(CommandExecutor executor, string db, string sql, string code) =>
        ForeignKeyBranchScenarios.ExpectCode(executor, db, sql, code);

    private static CamusDBException ExpectDdlCode(CommandExecutor executor, string db, string sql, string code)
    {
        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () => await Ddl(executor, db, sql))!;
        Assert.AreEqual(code, exception.Code, exception.Message);
        return exception;
    }
}

/// <summary>The integrity edges on a standalone engine.</summary>
[TestFixture]
[NonParallelizable]
public sealed class TestForeignKeyIntegrityEdges : BaseTest
{
    [Test] public async Task WiderUniqueIndexDoesNotHideAChildWithANullSuffix() => await Run(ForeignKeyIntegrityEdgeScenarios.WiderUniqueIndexDoesNotHideAChildWithANullSuffix);
    [Test] public async Task AddFindsAnOrphanThatAWiderUniqueIndexOmits() => await Run(ForeignKeyIntegrityEdgeScenarios.AddFindsAnOrphanThatAWiderUniqueIndexOmits);
    [Test] public async Task ExactWidthUniqueIndexIsStillReused() => await Run(ForeignKeyIntegrityEdgeScenarios.ExactWidthUniqueIndexIsStillReused);
    [Test] public async Task DescendingValidationReachesAnOrphanAfterTheFirstPage() => await Run(ForeignKeyIntegrityEdgeScenarios.DescendingValidationReachesAnOrphanAfterTheFirstPage);
    [Test] public async Task DescendingValidationChecksEveryKeyOnce() => await Run(ForeignKeyIntegrityEdgeScenarios.DescendingValidationChecksEveryKeyOnce);
    [Test] public async Task MixedDirectionValidationReachesAnOrphanOnTheSecondPage() => await Run(ForeignKeyIntegrityEdgeScenarios.MixedDirectionValidationReachesAnOrphanOnTheSecondPage);
    [Test] public async Task DescendingValidationOnABranchReachesAnInheritedOrphan() => await Run(ForeignKeyIntegrityEdgeScenarios.DescendingValidationOnABranchReachesAnInheritedOrphan);
    [Test] public async Task RecheckDoesNotJoinTwoStatesIntoAFalseOrphan() => await Run(ForeignKeyIntegrityEdgeScenarios.RecheckDoesNotJoinTwoStatesIntoAFalseOrphan);
    [Test] public async Task CheckCannotTakeAForeignKeyName() => await Run(ForeignKeyIntegrityEdgeScenarios.CheckCannotTakeAForeignKeyName);
    [Test] public async Task ForeignKeyCannotTakeACheckName() => await Run(ForeignKeyIntegrityEdgeScenarios.ForeignKeyCannotTakeACheckName);
    [Test] public async Task CheckCannotTakeANotNullName() => await Run(ForeignKeyIntegrityEdgeScenarios.CheckCannotTakeANotNullName);
    [Test] public async Task SetNotNullCannotTakeAForeignKeyName() => await Run(ForeignKeyIntegrityEdgeScenarios.SetNotNullCannotTakeAForeignKeyName);

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

/// <summary>The integrity edges on a cluster-mode engine, where the validation runs in the rollout coordinator.</summary>
[TestFixture]
[NonParallelizable]
public sealed class TestForeignKeyIntegrityEdgesCluster : SharedNodeBaseTest
{
    [Test] public async Task WiderUniqueIndexDoesNotHideAChildWithANullSuffix() => await Run(ForeignKeyIntegrityEdgeScenarios.WiderUniqueIndexDoesNotHideAChildWithANullSuffix);
    [Test] public async Task AddFindsAnOrphanThatAWiderUniqueIndexOmits() => await Run(ForeignKeyIntegrityEdgeScenarios.AddFindsAnOrphanThatAWiderUniqueIndexOmits);
    [Test] public async Task ExactWidthUniqueIndexIsStillReused() => await Run(ForeignKeyIntegrityEdgeScenarios.ExactWidthUniqueIndexIsStillReused);
    [Test] public async Task DescendingValidationReachesAnOrphanAfterTheFirstPage() => await Run(ForeignKeyIntegrityEdgeScenarios.DescendingValidationReachesAnOrphanAfterTheFirstPage);
    [Test] public async Task DescendingValidationChecksEveryKeyOnce() => await Run(ForeignKeyIntegrityEdgeScenarios.DescendingValidationChecksEveryKeyOnce);
    [Test] public async Task MixedDirectionValidationReachesAnOrphanOnTheSecondPage() => await Run(ForeignKeyIntegrityEdgeScenarios.MixedDirectionValidationReachesAnOrphanOnTheSecondPage);
    [Test] public async Task DescendingValidationOnABranchReachesAnInheritedOrphan() => await Run(ForeignKeyIntegrityEdgeScenarios.DescendingValidationOnABranchReachesAnInheritedOrphan);
    [Test] public async Task RecheckDoesNotJoinTwoStatesIntoAFalseOrphan() => await Run(ForeignKeyIntegrityEdgeScenarios.RecheckDoesNotJoinTwoStatesIntoAFalseOrphan);
    [Test] public async Task CheckCannotTakeAForeignKeyName() => await Run(ForeignKeyIntegrityEdgeScenarios.CheckCannotTakeAForeignKeyName);
    [Test] public async Task ForeignKeyCannotTakeACheckName() => await Run(ForeignKeyIntegrityEdgeScenarios.ForeignKeyCannotTakeACheckName);
    [Test] public async Task CheckCannotTakeANotNullName() => await Run(ForeignKeyIntegrityEdgeScenarios.CheckCannotTakeANotNullName);
    [Test] public async Task SetNotNullCannotTakeAForeignKeyName() => await Run(ForeignKeyIntegrityEdgeScenarios.SetNotNullCannotTakeAForeignKeyName);

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
