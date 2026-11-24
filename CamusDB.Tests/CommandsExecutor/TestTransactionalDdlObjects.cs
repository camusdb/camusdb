/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Meta;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Catalogs.Transactional;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Tests.InlineFunctions;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.Sequences;
using NUnit.Framework;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Every kind of object inside an explicit transaction with <c>transactional_ddl_enabled</c> on, on a single
/// node: sequences and their counters, views and materialized views, triggers, inline functions, extensions,
/// constraints that end, comments, table settings and <c>CREATE TABLE ... AS</c>. For each, the statements
/// after it see the change, no other transaction does, <c>COMMIT</c> publishes it and <c>ROLLBACK</c> leaves
/// nothing.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestTransactionalDdlObjects : InlineFunctionTestBase
{
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) =>
        base.ConfigureOptions(defaults) with
        {
            TransactionalDdlEnabled = true,
            ExtensionAssemblies = [FixturePath("Fixture.Good")],
            // The sweep would run a deferred record that a test holds back.
            CoordinatorJobSweepIntervalMs = 0,
        };

    private static async Task<KvTransaction> BeginSessionAsync(DatabaseDescriptor database)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        tx.MarkSessionOwned();
        return tx;
    }

    private static Task Ddl(CommandExecutor executor, KvTransaction tx, string dbname, string sql) =>
        executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));

    private static async Task<int> Write(CommandExecutor executor, KvTransaction tx, string dbname, string sql) =>
        (await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null))).ModifiedRows;

    private static async Task<List<Dictionary<string, ColumnValue>>> Rows(CommandExecutor executor, KvTransaction tx, string dbname, string sql)
    {
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));

        List<Dictionary<string, ColumnValue>> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add(new Dictionary<string, ColumnValue>(row.Row));

        return rows;
    }

    private static async Task<ColumnValue> One(CommandExecutor executor, KvTransaction tx, string dbname, string sql) =>
        (await Rows(executor, tx, dbname, sql)).Single().Values.Single();

    /// <summary>The error code of <paramref name="sql"/> run in a transaction of its own.</summary>
    private static Task<string> CodeOutside(CommandExecutor executor, string dbname, string sql) =>
        Code(() => ExecQuery(executor, dbname, sql));

    private static async Task<DatabaseDescriptor> ReopenAsync(CommandExecutor executor, string dbname)
    {
        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        return await executor.OpenDatabase(dbname);
    }

    private static async Task<bool> CounterExistsAsync(DatabaseDescriptor database, string sequenceId)
    {
        (SequenceResponseType type, _) = await database.Kahuna.Kahuna.LocateAndGetSequence(
            MetaKeys.KahunaSequenceName(database.Id, sequenceId), SequenceDurability.Persistent, CancellationToken.None);

        return type == SequenceResponseType.Success;
    }

    private static Task EndAsync(DatabaseDescriptor database, KvTransaction tx, bool commit) =>
        commit ? database.Transactions.CommitAsync(tx) : database.Transactions.RollbackAsync(tx);

    private static void AssertNoTrace(DatabaseDescriptor database, long versionBefore)
    {
        Assert.AreEqual(versionBefore, database.Schema.SchemaVersion, "a rollback proposes nothing");
        Assert.IsNull(database.Schema.PendingChangeSet);
    }

    // ── sequences ─────────────────────────────────────────────────────────────

    [Test]
    public async Task CreateSequence_TheTransactionDrawsFromIt_AndItEndsWithTheTransaction([Values] bool commit)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        long versionBefore = database.Schema.SchemaVersion;

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, db, "CREATE SEQUENCE order_no START WITH 100 INCREMENT BY 5 CACHE 1");
        await Ddl(executor, tx, db, "CREATE TABLE orders (id INT64 PRIMARY KEY, no INT64 DEFAULT(nextval('order_no')))");
        await Write(executor, tx, db, "INSERT INTO orders (id) VALUES (1)");

        Assert.AreEqual(100, (await One(executor, tx, db, "SELECT no FROM orders WHERE id = 1")).LongValue);
        Assert.AreEqual(105, (await One(executor, tx, db, "SELECT nextval('order_no')")).LongValue);
        Assert.AreEqual(CamusDBErrorCodes.SequenceDoesntExist, await CodeOutside(executor, db, "SELECT nextval('order_no')"));

        string sequenceId = ((TransactionSchemaOverlay)tx.SchemaChanges!).Schema.Sequences["order_no"].Id!;
        Assert.IsTrue(await CounterExistsAsync(database, sequenceId));

        await EndAsync(database, tx, commit);

        Assert.AreEqual(commit, database.Schema.Sequences.ContainsKey("order_no"));
        Assert.AreEqual(commit, await CounterExistsAsync(database, sequenceId), "a rollback removes the counter too");

        if (!commit)
        {
            AssertNoTrace(database, versionBefore);
            return;
        }

        Assert.AreEqual(110, await Number(executor, db, "SELECT nextval('order_no')"));
        await ReopenAsync(executor, db);
        await ExecNonQuery(executor, db, "INSERT INTO orders (id) VALUES (2)");
        Assert.AreEqual(115, await Number(executor, db, "SELECT no FROM orders WHERE id = 2"));
    }

    [Test]
    public async Task DropSequence_KeepsTheCounterUntilThePublish([Values] bool commit)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, db, "CREATE SEQUENCE s CACHE 1");
        Assert.AreEqual(1, await Number(executor, db, "SELECT nextval('s')"));
        string sequenceId = database.Schema.Sequences["s"].Id!;

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, db, "DROP SEQUENCE s");

        Assert.AreEqual(CamusDBErrorCodes.SequenceDoesntExist, await Code(() => Rows(executor, tx, db, "SELECT nextval('s')")));
        Assert.AreEqual(2, await Number(executor, db, "SELECT nextval('s')"), "every other transaction still draws");

        // The failed draw above left the transaction to roll back; a fresh one decides.
        await database.Transactions.RollbackAsync(tx);
        Assert.AreEqual(3, await Number(executor, db, "SELECT nextval('s')"));

        tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, db, "DROP SEQUENCE s");
        await EndAsync(database, tx, commit);

        Assert.AreEqual(!commit, database.Schema.Sequences.ContainsKey("s"));
        Assert.AreEqual(!commit, await CounterExistsAsync(database, sequenceId));
    }

    [Test]
    public async Task RenameAndCommentSequence_EndWithTheTransaction([Values] bool commit)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, db, "CREATE SEQUENCE s CACHE 1");
        Assert.AreEqual(1, await Number(executor, db, "SELECT nextval('s')"));

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, db, "ALTER SEQUENCE s RENAME TO t");
        await Ddl(executor, tx, db, "COMMENT ON SEQUENCE t IS 'ticket numbers'");
        Assert.AreEqual(2, (await One(executor, tx, db, "SELECT nextval('t')")).LongValue, "a rename moves no counter");
        Assert.AreEqual("s", database.Schema.Sequences["s"].Name, "the shared record keeps its name");

        await EndAsync(database, tx, commit);
        DatabaseDescriptor reopened = await ReopenAsync(executor, db);

        SequenceSchema sequence = reopened.Schema.Sequences[commit ? "t" : "s"];
        Assert.AreEqual(1, reopened.Schema.Sequences.Count);
        Assert.AreEqual(commit ? "ticket numbers" : null, sequence.Comment);
        Assert.AreEqual(3, await Number(executor, db, $"SELECT nextval('{sequence.Name}')"));
    }

    /// <summary>
    /// The counter follows the old definition until the publish, for the transaction itself too, because
    /// every other transaction draws from it. The commit returns after the counter has the new one.
    /// </summary>
    [Test]
    public async Task AlterSequence_TheCounterFollowsTheOutcomeOfTheTransaction([Values] bool commit)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, db, "CREATE SEQUENCE s CACHE 1");
        Assert.AreEqual(1, await Number(executor, db, "SELECT nextval('s')"));

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, db, "ALTER SEQUENCE s INCREMENT BY 10 MAXVALUE 25");

        Assert.AreEqual(10, ((TransactionSchemaOverlay)tx.SchemaChanges!).Schema.Sequences["s"].Increment);
        Assert.AreEqual(1, database.Schema.Sequences["s"].Increment);
        Assert.AreEqual(2, (await One(executor, tx, db, "SELECT nextval('s')")).LongValue);
        Assert.AreEqual(3, await Number(executor, db, "SELECT nextval('s')"));

        await EndAsync(database, tx, commit);

        Assert.AreEqual(commit ? 10 : 1, database.Schema.Sequences["s"].Increment);
        Assert.AreEqual(commit ? 13 : 4, await Number(executor, db, "SELECT nextval('s')"));

        if (!commit)
            return;

        Assert.AreEqual(23, await Number(executor, db, "SELECT nextval('s')"));
        Assert.AreEqual(CamusDBErrorCodes.SequenceExhausted, await CodeOutside(executor, db, "SELECT nextval('s')"), "the counter has the new maximum");

        DatabaseDescriptor reopened = await ReopenAsync(executor, db);
        Assert.AreEqual(25, reopened.Schema.Sequences["s"].MaxValue);
        reopened.MarkDeferredPurgesDue();
        Assert.AreEqual(0, await executor.DeferredPurgesForTests.RunDueAsync(reopened), "the commit finished the record and removed it");
    }

    [Test]
    public async Task AlterSequence_ANodeThatStopsAfterThePublish_LeavesTheCounterToTheSweep()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, db, "CREATE SEQUENCE s CACHE 1");
        Assert.AreEqual(1, await Number(executor, db, "SELECT nextval('s')"));

        executor.DeferredPurgesForTests.TestSkipRunsAfterPublish = true;

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, db, "ALTER SEQUENCE s INCREMENT BY 10");
        await database.Transactions.CommitAsync(tx);

        Assert.AreEqual(2, await Number(executor, db, "SELECT nextval('s')"), "the counter still has the old definition");

        DatabaseDescriptor reopened = await ReopenAsync(executor, db);
        executor.DeferredPurgesForTests.TestSkipRunsAfterPublish = false;

        Assert.AreEqual(1, await executor.DeferredPurgesForTests.RunDueAsync(reopened), "the sweep after the reopen finds the record");
        Assert.AreEqual(12, await Number(executor, db, "SELECT nextval('s')"));

        reopened.MarkDeferredPurgesDue();
        Assert.AreEqual(0, await executor.DeferredPurgesForTests.RunDueAsync(reopened));
    }

    /// <summary>A record that runs late must not bring back the definition that a later ALTER replaced.</summary>
    [Test]
    public async Task AlterSequence_ALateRecord_KeepsTheDefinitionOfALaterAlter()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, db, "CREATE SEQUENCE s CACHE 1");

        executor.DeferredPurgesForTests.TestSkipRunsAfterPublish = true;
        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, db, "ALTER SEQUENCE s INCREMENT BY 10");
        await database.Transactions.CommitAsync(tx);
        executor.DeferredPurgesForTests.TestSkipRunsAfterPublish = false;

        await ExecDdl(executor, db, "ALTER SEQUENCE s INCREMENT BY 3");
        long before = await Number(executor, db, "SELECT nextval('s')");

        database.MarkDeferredPurgesDue();
        Assert.AreEqual(1, await executor.DeferredPurgesForTests.RunDueAsync(database));
        Assert.AreEqual(before + 3, await Number(executor, db, "SELECT nextval('s')"));
    }

    [Test]
    public async Task AlterSequence_AMaximumThatDrawsOvertook_LeavesAnExhaustedSequence()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, db, "CREATE SEQUENCE s CACHE 1");
        Assert.AreEqual(1, await Number(executor, db, "SELECT nextval('s')"));

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, db, "ALTER SEQUENCE s MAXVALUE 2");

        // The old definition has no maximum, so other transactions pass the new one before the commit.
        for (int expected = 2; expected <= 5; expected++)
            Assert.AreEqual(expected, await Number(executor, db, "SELECT nextval('s')"));

        await database.Transactions.CommitAsync(tx);

        Assert.AreEqual(2, database.Schema.Sequences["s"].MaxValue);
        Assert.AreEqual(CamusDBErrorCodes.SequenceExhausted, await CodeOutside(executor, db, "SELECT nextval('s')"));

        database.MarkDeferredPurgesDue();
        Assert.AreEqual(0, await executor.DeferredPurgesForTests.RunDueAsync(database), "the record is done");
    }

    [Test]
    public async Task AlterSequence_AMaximumBelowTheCounter_IsRefused()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, db, "CREATE SEQUENCE s CACHE 1");
        for (int i = 0; i < 5; i++)
            await Number(executor, db, "SELECT nextval('s')");

        KvTransaction tx = await BeginSessionAsync(database);
        Assert.AreEqual(CamusDBErrorCodes.InvalidSequenceDefinition, await Code(() => Ddl(executor, tx, db, "ALTER SEQUENCE s MAXVALUE 3")));
        await database.Transactions.RollbackAsync(tx);

        Assert.IsNull(database.Schema.Sequences["s"].MaxValue);
    }

    [Test]
    public async Task SetValAfterAnAlter_FollowsTheDefinitionOfTheCounter()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, db, "CREATE SEQUENCE shared_s CACHE 1");

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, db, "CREATE SEQUENCE own_s CACHE 1");

        foreach (string sequence in new[] { "shared_s", "own_s" })
        {
            await Ddl(executor, tx, db, $"ALTER SEQUENCE {sequence} INCREMENT BY 10");
            await Rows(executor, tx, db, $"SELECT setval('{sequence}', 500, false)");
            Assert.AreEqual(500, (await One(executor, tx, db, $"SELECT nextval('{sequence}')")).LongValue, sequence);
            Assert.AreEqual(501, (await One(executor, tx, db, $"SELECT nextval('{sequence}')")).LongValue, sequence);
        }

        await database.Transactions.CommitAsync(tx);

        foreach (string sequence in new[] { "shared_s", "own_s" })
            Assert.AreEqual(511, await Number(executor, db, $"SELECT nextval('{sequence}')"), sequence);
    }

    [Test]
    public async Task RestartSequence_IsRefused()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, db, "CREATE SEQUENCE s CACHE 1");

        KvTransaction tx = await BeginSessionAsync(database);
        Assert.AreEqual(CamusDBErrorCodes.StatementNotAllowedInTransaction, await Code(() => Ddl(executor, tx, db, "ALTER SEQUENCE s RESTART WITH 9")));
        await database.Transactions.RollbackAsync(tx);

        Assert.AreEqual(1, await Number(executor, db, "SELECT nextval('s')"));
    }

    // ── views and materialized views ──────────────────────────────────────────

    [Test]
    public async Task CreateViewOverATableOfTheTransaction_EndsWithTheTransaction([Values] bool commit)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        long versionBefore = database.Schema.SchemaVersion;

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, db, "CREATE TABLE t (id INT64 PRIMARY KEY, v INT64)");
        await Ddl(executor, tx, db, "CREATE VIEW big AS SELECT id, v FROM t WHERE v > 10");
        await Ddl(executor, tx, db, "COMMENT ON VIEW big IS 'the big ones'");
        await Write(executor, tx, db, "INSERT INTO t (id, v) VALUES (1, 5), (2, 50)");

        Assert.AreEqual(2, (await One(executor, tx, db, "SELECT id FROM big")).LongValue);
        Assert.AreEqual(CamusDBErrorCodes.TableDoesntExist, await CodeOutside(executor, db, "SELECT id FROM big"));

        await EndAsync(database, tx, commit);

        if (!commit)
        {
            AssertNoTrace(database, versionBefore);
            Assert.IsEmpty(database.Schema.Views);
            return;
        }

        DatabaseDescriptor reopened = await ReopenAsync(executor, db);
        Assert.AreEqual(2, await Number(executor, db, "SELECT id FROM big"));
        Assert.AreEqual("the big ones", reopened.Schema.Views["big"].Comment);
    }

    [Test]
    public async Task ReplaceRenameAndDropView_EndWithTheTransaction([Values] bool commit)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, db, "CREATE TABLE t (id INT64 PRIMARY KEY, v INT64)");
        await ExecNonQuery(executor, db, "INSERT INTO t (id, v) VALUES (1, 5), (2, 50)");
        await ExecDdl(executor, db, "CREATE VIEW kept AS SELECT id FROM t");
        await ExecDdl(executor, db, "CREATE VIEW doomed AS SELECT id FROM t");
        await ExecDdl(executor, db, "COMMENT ON VIEW kept IS 'old'");

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, db, "CREATE OR REPLACE VIEW kept AS SELECT id FROM t WHERE v > 10");
        await Ddl(executor, tx, db, "ALTER VIEW kept RENAME TO moved");
        await Ddl(executor, tx, db, "COMMENT ON VIEW moved IS NULL");
        await Ddl(executor, tx, db, "DROP VIEW doomed");

        Assert.AreEqual(1, (await Rows(executor, tx, db, "SELECT id FROM moved")).Count);
        Assert.AreEqual(2, (await ExecQuery(executor, db, "SELECT id FROM kept")).Count);

        await EndAsync(database, tx, commit);
        DatabaseDescriptor reopened = await ReopenAsync(executor, db);

        if (commit)
        {
            CollectionAssert.AreEquivalent(new[] { "moved" }, reopened.Schema.Views.Keys);
            Assert.IsNull(reopened.Schema.Views["moved"].Comment);
            Assert.AreEqual(1, (await ExecQuery(executor, db, "SELECT id FROM moved")).Count);
        }
        else
        {
            CollectionAssert.AreEquivalent(new[] { "kept", "doomed" }, reopened.Schema.Views.Keys);
            Assert.AreEqual("old", reopened.Schema.Views["kept"].Comment);
            Assert.AreEqual(2, (await ExecQuery(executor, db, "SELECT id FROM kept")).Count);
        }
    }

    [Test]
    public async Task RenameAndCommentMaterializedView_EndWithTheTransaction([Values] bool commit)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateTableAndMaterializedViewAsync(executor, db);

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, db, "ALTER MATERIALIZED VIEW mv RENAME TO mv2");
        await Ddl(executor, tx, db, "COMMENT ON MATERIALIZED VIEW mv2 IS 'totals'");

        Assert.AreEqual(2, (await Rows(executor, tx, db, "SELECT id FROM mv2")).Count);
        Assert.AreEqual(2, (await ExecQuery(executor, db, "SELECT id FROM mv")).Count);

        await EndAsync(database, tx, commit);
        DatabaseDescriptor reopened = await ReopenAsync(executor, db);

        TableSchema view = reopened.Schema.Tables[commit ? "mv2" : "mv"];
        Assert.AreEqual(commit ? "totals" : null, view.Comment);
        Assert.AreEqual(2, (await ExecQuery(executor, db, $"SELECT id FROM {view.Name}")).Count);
        Assert.IsFalse(reopened.Schema.Tables.ContainsKey(commit ? "mv" : "mv2"));
    }

    [Test]
    public async Task DropMaterializedView_EndsWithTheTransaction([Values] bool commit, [Values] bool cascadeFromAView)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        if (cascadeFromAView)
        {
            await ExecDdl(executor, db, "CREATE TABLE t (id INT64 PRIMARY KEY, v INT64)");
            await ExecNonQuery(executor, db, "INSERT INTO t (id, v) VALUES (1, 5), (2, 50)");
            await ExecDdl(executor, db, "CREATE VIEW base AS SELECT id, v FROM t");
            await ExecDdl(executor, db, "CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM base");
        }
        else
            await CreateTableAndMaterializedViewAsync(executor, db);

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, db, cascadeFromAView ? "DROP VIEW base CASCADE" : "DROP MATERIALIZED VIEW mv");

        Assert.AreEqual(CamusDBErrorCodes.TableDoesntExist, await Code(() => Rows(executor, tx, db, "SELECT id FROM mv")));
        await database.Transactions.RollbackAsync(tx);
        Assert.AreEqual(2, (await ExecQuery(executor, db, "SELECT id FROM mv")).Count, "the rollback keeps the view and its rows");

        tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, db, cascadeFromAView ? "DROP VIEW base CASCADE" : "DROP MATERIALIZED VIEW IF EXISTS mv");
        await EndAsync(database, tx, commit);

        DatabaseDescriptor reopened = await ReopenAsync(executor, db);
        Assert.AreEqual(!commit, reopened.Schema.Tables.ContainsKey("mv"));
        Assert.AreEqual(cascadeFromAView && !commit, reopened.Schema.Views.ContainsKey("base"));

        if (!commit)
            Assert.AreEqual(2, (await ExecQuery(executor, db, "SELECT id FROM mv")).Count);
    }

    /// <summary>
    /// The reclaim of an unfinished refresh drops its staging relation for every transaction at once, so a
    /// drop of the view inside a transaction refuses rather than do it.
    /// </summary>
    [Test]
    public async Task DropMaterializedView_WithAnUnfinishedRefresh_IsRefused()
    {
        (string db, DatabaseDescriptor database, CatalogsManager catalogs, CommandExecutor executor) = await CreateDatabaseWithCatalogsAsync();
        await CreateTableAndMaterializedViewAsync(executor, db);
        string viewId = database.Schema.Tables["mv"].Id!;

        await catalogs.PersistRefreshJobAsync(database, new MaterializedViewRefreshJob
        {
            JobId = Guid.NewGuid().ToString("N"),
            DatabaseId = database.Id,
            ViewTableId = viewId,
            ViewName = "mv",
            StagingName = "no_such_staging_relation",
        });

        KvTransaction tx = await BeginSessionAsync(database);
        Assert.AreEqual(CamusDBErrorCodes.StatementNotAllowedInTransaction, await Code(() => Ddl(executor, tx, db, "DROP MATERIALIZED VIEW mv")));
        await database.Transactions.RollbackAsync(tx);

        Assert.IsNotNull(await catalogs.TryGetRefreshJobAsync(database, viewId), "the record is not touched");
        Assert.AreEqual(2, (await ExecQuery(executor, db, "SELECT id FROM mv")).Count);

        await ExecDdl(executor, db, "DROP MATERIALIZED VIEW mv");
        Assert.IsNull(await catalogs.TryGetRefreshJobAsync(database, viewId), "outside a transaction the drop reclaims it");
    }

    private static async Task CreateTableAndMaterializedViewAsync(CommandExecutor executor, string db)
    {
        await ExecDdl(executor, db, "CREATE TABLE t (id INT64 PRIMARY KEY, v INT64)");
        await ExecNonQuery(executor, db, "INSERT INTO t (id, v) VALUES (1, 5), (2, 50)");
        await ExecDdl(executor, db, "CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM t");
    }

    private async Task<(string Db, DatabaseDescriptor Database, CatalogsManager Catalogs, CommandExecutor Executor)> CreateDatabaseWithCatalogsAsync()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        return (db, database, executor.CatalogsForTests, executor);
    }

    // ── inline functions ──────────────────────────────────────────────────────

    [Test]
    public async Task CreateFunction_OnlyTheTransactionCallsIt_AndItEndsWithTheTransaction([Values] bool commit)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, db, "CREATE TABLE posts (id INT64 PRIMARY KEY, title STRING)");
        await ExecNonQuery(executor, db, "INSERT INTO posts (id, title) VALUES (1, 'A B')");
        long versionBefore = database.Schema.SchemaVersion;

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, db, Slugify);
        await Ddl(executor, tx, db, "CREATE TABLE notes (id INT64 PRIMARY KEY, slug STRING DEFAULT (slugify('X Y')), CHECK (slugify('a') = 'a'))");
        await Write(executor, tx, db, "INSERT INTO notes (id) VALUES (1)");

        Assert.AreEqual("a-b", (await One(executor, tx, db, "SELECT slugify(title) FROM posts")).StrValue);
        Assert.AreEqual("x-y", (await One(executor, tx, db, "SELECT slug FROM notes")).StrValue);
        Assert.IsTrue((await Rows(executor, tx, db, "SHOW FUNCTIONS")).Exists(row => row["name"].StrValue == "slugify"));

        Assert.AreEqual(CamusDBErrorCodes.InvalidAstStmt, await CodeOutside(executor, db, "SELECT slugify(title) FROM posts"));
        Assert.IsNull(await ShownFunction(executor, db, "slugify"));

        await EndAsync(database, tx, commit);

        if (!commit)
        {
            AssertNoTrace(database, versionBefore);
            Assert.AreEqual(CamusDBErrorCodes.InvalidAstStmt, await CodeOutside(executor, db, "SELECT slugify(title) FROM posts"));
            return;
        }

        Assert.AreEqual("a-b", await Text(executor, db, "SELECT slugify(title) FROM posts"));
        await ReopenAsync(executor, db);
        Assert.AreEqual("a-b", await Text(executor, db, "SELECT slugify(title) FROM posts"));
        Assert.AreEqual("x-y", await Text(executor, db, "SELECT slug FROM notes"));
    }

    [Test]
    public async Task ReplaceFunction_OthersCallTheOldBodyUntilTheCommit([Values] bool commit)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, db, "CREATE FUNCTION f(x INT64) RETURNS INT64 LANGUAGE csharp AS $$ return x + 1; $$");
        Assert.AreEqual(11, await Number(executor, db, "SELECT f(10)"));

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, db, "CREATE OR REPLACE FUNCTION f(x INT64) RETURNS INT64 LANGUAGE csharp AS $$ return x + 2; $$");

        Assert.AreEqual(12, (await One(executor, tx, db, "SELECT f(10)")).LongValue);
        Assert.AreEqual(11, await Number(executor, db, "SELECT f(10)"));
        Assert.AreEqual(12, (await One(executor, tx, db, "SELECT f(10)")).LongValue, "the statement of another transaction changed nothing here");

        await EndAsync(database, tx, commit);

        Assert.AreEqual(commit ? 12 : 11, await Number(executor, db, "SELECT f(10)"));
        await ReopenAsync(executor, db);
        Assert.AreEqual(commit ? 12 : 11, await Number(executor, db, "SELECT f(10)"));
    }

    [Test]
    public async Task DropFunction_EndsWithTheTransaction([Values] bool commit)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, db, Slugify);

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, db, "DROP FUNCTION slugify");
        Assert.IsEmpty(await Rows(executor, tx, db, "SHOW FUNCTIONS"));
        Assert.AreEqual("a-b", await Text(executor, db, "SELECT slugify('A B')"));

        await EndAsync(database, tx, commit);
        await ReopenAsync(executor, db);

        if (commit)
            Assert.AreEqual(CamusDBErrorCodes.InvalidAstStmt, await CodeOutside(executor, db, "SELECT slugify('A B')"));
        else
            Assert.AreEqual("a-b", await Text(executor, db, "SELECT slugify('A B')"));
    }

    [Test]
    public async Task DropFunction_WithADependentOfTheTransaction_IsRefused()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, db, Slugify);

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, db, "CREATE TABLE notes (id INT64 PRIMARY KEY, title STRING, CHECK (slugify(title) <> 'admin'))");
        Assert.AreEqual(CamusDBErrorCodes.FunctionHasDependents, await Code(() => Ddl(executor, tx, db, "DROP FUNCTION slugify")));
        await database.Transactions.RollbackAsync(tx);

        Assert.AreEqual("a-b", await Text(executor, db, "SELECT slugify('A B')"));
    }

    /// <summary>
    /// The scope object of a database is shared by all its statements. A statement of the transaction opens
    /// the database and must not put the transaction's functions there, where a cursor of another
    /// transaction that is still open reads its next row.
    /// </summary>
    [Test]
    public async Task AnOpenCursorOfAnotherTransaction_NeverCallsTheFunctionsOfTheTransaction()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, db, "CREATE FUNCTION f(x INT64) RETURNS INT64 LANGUAGE csharp AS $$ return x + 1; $$");
        await ExecDdl(executor, db, "CREATE TABLE n (id INT64 PRIMARY KEY)");
        await ExecNonQuery(executor, db, "INSERT INTO n (id) VALUES (1), (2), (3)");

        KvTransaction reader = await database.Transactions.BeginAsync();
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(reader, db, "SELECT f(id) AS v FROM n ORDER BY id", null));
        await using IAsyncEnumerator<QueryResultRow> rows = cursor.GetAsyncEnumerator();

        Assert.IsTrue(await rows.MoveNextAsync());
        Assert.AreEqual(2, rows.Current.Row["v"].LongValue);

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, db, "CREATE OR REPLACE FUNCTION f(x INT64) RETURNS INT64 LANGUAGE csharp AS $$ return x + 100; $$");
        Assert.AreEqual(101, (await One(executor, tx, db, "SELECT f(1)")).LongValue);

        Assert.IsTrue(await rows.MoveNextAsync());
        Assert.AreEqual(3, rows.Current.Row["v"].LongValue, "the cursor keeps the function it was planned with");

        Assert.AreEqual(101, (await One(executor, tx, db, "SELECT f(1)")).LongValue);
        Assert.IsTrue(await rows.MoveNextAsync());
        Assert.AreEqual(4, rows.Current.Row["v"].LongValue);

        await database.Transactions.RollbackAsync(tx);
        await database.Transactions.RollbackAsync(reader);
    }

    /// <summary>
    /// A statement of a transaction with a private schema holds the code of the functions it calls through
    /// the catalog of that schema. The end of the transaction gives it back: a drop of the function then
    /// unloads its code, so the same function compiles again when it is created again and called.
    /// </summary>
    [Test]
    public async Task TheEndOfTheTransaction_GivesTheCompiledCodeBack([Values] bool commit)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, db, Slugify);
        Assert.AreEqual("a-b", await Text(executor, db, "SELECT slugify('A B')"));
        long compiledOnce = executor.InlineFunctions.Compilations;

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, db, "CREATE TABLE t (id INT64 PRIMARY KEY)");
        Assert.AreEqual("a-b", (await One(executor, tx, db, "SELECT slugify('A B')")).StrValue);
        Assert.AreEqual(compiledOnce, executor.InlineFunctions.Compilations, "the transaction shares the code that is loaded");
        await EndAsync(database, tx, commit);

        await ExecDdl(executor, db, "DROP FUNCTION slugify");
        await ExecDdl(executor, db, Slugify);
        long afterCreate = executor.InlineFunctions.Compilations;
        Assert.AreEqual("a-b", await Text(executor, db, "SELECT slugify('A B')"));
        Assert.AreEqual(afterCreate + 1, executor.InlineFunctions.Compilations, "nothing held the code of the dropped function");
    }

    /// <summary>
    /// Readers call a function and write through a trigger without a pause, while transactions replace the
    /// function, fire the trigger and roll back, one after the other. No reader ever sees a body of a
    /// transaction, and no call fails.
    /// </summary>
    [Test]
    public async Task ReadersOfAFunction_NeverSeeTheBodyOfATransactionThatRollsBack()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, db, "CREATE FUNCTION f(x INT64) RETURNS INT64 LANGUAGE csharp AS $$ return x + 1; $$");
        await ExecDdl(executor, db, "CREATE TABLE n (id INT64 PRIMARY KEY, v INT64)");
        await ExecDdl(executor, db, "CREATE TRIGGER fill ON n BEFORE INSERT LANGUAGE csharp AS $$ NEW!.v = f(NEW.id); $$");
        await ExecNonQuery(executor, db, "INSERT INTO n (id) VALUES (1)");

        using CancellationTokenSource stop = new();
        long nextId = 1_000;
        int reads = 0;

        Task[] readers = [.. Enumerable.Range(0, 4).Select(_ => Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested)
            {
                foreach (Dictionary<string, ColumnValue> row in await ExecQuery(executor, db, "SELECT id, f(id) AS v FROM n WHERE id = 1"))
                    Assert.AreEqual(2, row["v"].LongValue);

                long id = Interlocked.Increment(ref nextId);
                await ExecNonQuery(executor, db, $"INSERT INTO n (id) VALUES ({id})");
                Interlocked.Increment(ref reads);
            }
        }))];

        for (int i = 0; i < 15; i++)
        {
            KvTransaction tx = await BeginSessionAsync(database);
            await Ddl(executor, tx, db, "CREATE OR REPLACE FUNCTION f(x INT64) RETURNS INT64 LANGUAGE csharp AS $$ return x + 1000000; $$");
            Assert.AreEqual(1_000_001, (await One(executor, tx, db, "SELECT f(1)")).LongValue);
            await Write(executor, tx, db, "INSERT INTO n (id) VALUES (2)");
            Assert.AreEqual(1_000_002, (await One(executor, tx, db, "SELECT v FROM n WHERE id = 2")).LongValue);
            await database.Transactions.RollbackAsync(tx);
        }

        await stop.CancelAsync();
        await Task.WhenAll(readers);

        Assert.Greater(reads, 0);
        Assert.AreEqual(0, await Number(executor, db, "SELECT COUNT(*) FROM n WHERE v <> id + 1"));
        Assert.AreEqual(0, await Number(executor, db, "SELECT COUNT(*) FROM n WHERE id = 2"));
    }

    // ── triggers ──────────────────────────────────────────────────────────────

    private const string Posts = "CREATE TABLE posts (id INT64 PRIMARY KEY, title STRING, slug STRING)";

    private const string SlugTrigger =
        "CREATE TRIGGER slug ON posts BEFORE INSERT LANGUAGE csharp AS $$ if (NEW!.title is { } title) NEW.slug = slugify(title); $$";

    [Test]
    public async Task CreateTrigger_FiresForTheTransactionOnly_AndEndsWithTheTransaction([Values] bool commit)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, db, Posts);

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, db, Slugify);
        await Ddl(executor, tx, db, SlugTrigger);
        await Write(executor, tx, db, "INSERT INTO posts (id, title) VALUES (1, 'A B')");

        Assert.AreEqual("a-b", (await One(executor, tx, db, "SELECT slug FROM posts WHERE id = 1")).StrValue);

        await ExecNonQuery(executor, db, "INSERT INTO posts (id, title) VALUES (2, 'C D')");
        Assert.IsNull(await Text(executor, db, "SELECT slug FROM posts WHERE id = 2"), "the trigger is not there for another transaction");

        await EndAsync(database, tx, commit);
        DatabaseDescriptor reopened = await ReopenAsync(executor, db);

        Assert.AreEqual(commit ? 1 : 0, reopened.Schema.Tables["posts"].Triggers?.Count ?? 0);
        Assert.AreEqual(commit ? 1 : 0, reopened.Schema.Functions.Count);

        await ExecNonQuery(executor, db, "INSERT INTO posts (id, title) VALUES (3, 'E F')");
        Assert.AreEqual(commit ? "e-f" : null, await Text(executor, db, "SELECT slug FROM posts WHERE id = 3"));
        Assert.AreEqual(commit ? 3 : 2, (await ExecQuery(executor, db, "SELECT id FROM posts")).Count);
    }

    [Test]
    public async Task DropTrigger_EndsWithTheTransaction([Values] bool commit)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, db, Posts);
        await ExecDdl(executor, db, Slugify);
        await ExecDdl(executor, db, SlugTrigger);

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, db, "DROP TRIGGER slug ON posts");
        await Write(executor, tx, db, "INSERT INTO posts (id, title) VALUES (1, 'A B')");
        Assert.IsNull((await One(executor, tx, db, "SELECT slug FROM posts WHERE id = 1")).StrValue);

        await ExecNonQuery(executor, db, "INSERT INTO posts (id, title) VALUES (2, 'C D')");
        Assert.AreEqual("c-d", await Text(executor, db, "SELECT slug FROM posts WHERE id = 2"));

        await EndAsync(database, tx, commit);

        await ExecNonQuery(executor, db, "INSERT INTO posts (id, title) VALUES (3, 'E F')");
        Assert.AreEqual(commit ? null : "e-f", await Text(executor, db, "SELECT slug FROM posts WHERE id = 3"));
    }

    /// <summary>
    /// A transaction that fires a trigger of a shared table compiles against its own columns and functions.
    /// Its binding must not replace the one every other transaction fires, or each side would make the
    /// other compile again at every statement.
    /// </summary>
    [Test]
    public async Task FiringASharedTrigger_FromBothSides_CompilesOncePerSide()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, db, Posts);
        await ExecDdl(executor, db, Slugify);
        await ExecDdl(executor, db, SlugTrigger);
        await ExecNonQuery(executor, db, "INSERT INTO posts (id, title) VALUES (1, 'A')");

        // The wider row of the transaction needs a unit of its own; the replaced function does not, because
        // a trigger calls it through a stub.
        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, db, "ALTER TABLE posts ADD COLUMN extra INT64");
        await Ddl(executor, tx, db, "CREATE OR REPLACE FUNCTION slugify(s STRING) RETURNS STRING LANGUAGE csharp AS $$ return \"own-\" + s; $$");
        await Write(executor, tx, db, "INSERT INTO posts (id, title) VALUES (10, 'A')");
        long compiled = executor.InlineFunctions.Compilations;

        for (int i = 0; i < 3; i++)
        {
            await ExecNonQuery(executor, db, $"INSERT INTO posts (id, title) VALUES ({2 + i}, 'B')");
            await Write(executor, tx, db, $"INSERT INTO posts (id, title) VALUES ({11 + i}, 'B')");
        }

        Assert.AreEqual(compiled, executor.InlineFunctions.Compilations);
        Assert.AreEqual("b", await Text(executor, db, "SELECT slug FROM posts WHERE id = 4"));
        Assert.AreEqual("own-B", (await One(executor, tx, db, "SELECT slug FROM posts WHERE id = 13")).StrValue);

        await database.Transactions.RollbackAsync(tx);
        await ExecNonQuery(executor, db, "INSERT INTO posts (id, title) VALUES (5, 'C')");
        Assert.AreEqual(compiled, executor.InlineFunctions.Compilations);
        Assert.AreEqual("c", await Text(executor, db, "SELECT slug FROM posts WHERE id = 5"));
    }

    // ── extensions ────────────────────────────────────────────────────────────

    [Test]
    public async Task CreateExtension_OnlyTheTransactionCallsIt_AndItEndsWithTheTransaction([Values] bool commit)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        long versionBefore = database.Schema.SchemaVersion;

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, db, "CREATE EXTENSION good");
        Assert.AreEqual(5, (await One(executor, tx, db, "SELECT fx_add(2, 3)")).LongValue);
        Assert.AreEqual(CamusDBErrorCodes.InvalidAstStmt, await CodeOutside(executor, db, "SELECT fx_add(2, 3)"));

        await EndAsync(database, tx, commit);

        if (!commit)
            AssertNoTrace(database, versionBefore);

        DatabaseDescriptor reopened = await ReopenAsync(executor, db);
        Assert.AreEqual(commit, reopened.Schema.Extensions.ContainsKey("good"));

        if (commit)
            Assert.AreEqual(5, await Number(executor, db, "SELECT fx_add(2, 3)"));
        else
            Assert.AreEqual(CamusDBErrorCodes.InvalidAstStmt, await CodeOutside(executor, db, "SELECT fx_add(2, 3)"));
    }

    [Test]
    public async Task DropExtension_EndsWithTheTransaction([Values] bool commit)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, db, "CREATE EXTENSION good");

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, db, "DROP EXTENSION good");
        Assert.AreEqual(5, await Number(executor, db, "SELECT fx_add(2, 3)"), "another transaction still calls it");

        await EndAsync(database, tx, commit);

        if (commit)
            Assert.AreEqual(CamusDBErrorCodes.InvalidAstStmt, await CodeOutside(executor, db, "SELECT fx_add(2, 3)"));
        else
            Assert.AreEqual(5, await Number(executor, db, "SELECT fx_add(2, 3)"));
    }

    // ── constraints that end, storage, settings ───────────────────────────────

    [Test]
    public async Task DropCheckAndNotNull_EndWithTheTransaction([Values] bool commit)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, db, "CREATE TABLE t (id INT64 PRIMARY KEY, v INT64, w INT64 NOT NULL)");
        await ExecDdl(executor, db, "ALTER TABLE t ADD CONSTRAINT v_positive CHECK (v > 0)");

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, db, "ALTER TABLE t DROP CONSTRAINT v_positive");
        await Ddl(executor, tx, db, "ALTER TABLE t ALTER COLUMN w DROP NOT NULL");
        await Write(executor, tx, db, "INSERT INTO t (id, v, w) VALUES (1, -1, NULL)");

        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, await Code(() => ExecNonQuery(executor, db, "INSERT INTO t (id, v, w) VALUES (2, -1, 1)")));
        Assert.AreEqual(CamusDBErrorCodes.NotNullViolation, await Code(() => ExecNonQuery(executor, db, "INSERT INTO t (id, v, w) VALUES (3, 1, NULL)")));

        await EndAsync(database, tx, commit);
        DatabaseDescriptor reopened = await ReopenAsync(executor, db);

        Assert.AreEqual(commit ? 0 : 1, reopened.Schema.Tables["t"].CheckConstraints?.Count ?? 0);
        Assert.AreEqual(!commit, reopened.Schema.Tables["t"].Columns!.Single(c => c.Name == "w").NotNull);
        Assert.AreEqual(commit ? 1 : 0, (await ExecQuery(executor, db, "SELECT id FROM t")).Count);

        if (commit)
            await ExecNonQuery(executor, db, "INSERT INTO t (id, v, w) VALUES (4, -5, NULL)");
        else
            Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, await Code(() => ExecNonQuery(executor, db, "INSERT INTO t (id, v, w) VALUES (4, -5, 1)")));
    }

    [Test]
    public async Task DropForeignKey_EndsWithTheTransaction([Values] bool commit)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateParentAndChildAsync(executor, db);
        await ExecDdl(executor, db, "ALTER TABLE child ADD CONSTRAINT fk FOREIGN KEY (pid) REFERENCES parent (id)");
        int indexes = database.Schema.Tables["child"].Indexes!.Count;

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, db, "ALTER TABLE child DROP CONSTRAINT fk");
        await Write(executor, tx, db, "INSERT INTO child (id, pid) VALUES (2, 99)");

        Assert.AreEqual(CamusDBErrorCodes.ForeignKeyViolation, await Code(() => ExecNonQuery(executor, db, "INSERT INTO child (id, pid) VALUES (3, 99)")));

        await EndAsync(database, tx, commit);
        await executor.DeferredPurgesForTests.WaitForScheduledRunsForTestsAsync();
        DatabaseDescriptor reopened = await ReopenAsync(executor, db);

        Assert.AreEqual(commit ? 0 : 1, reopened.Schema.Tables["child"].ForeignKeys?.Count ?? 0);
        Assert.AreEqual(commit ? indexes - 1 : indexes, reopened.Schema.Tables["child"].Indexes!.Count, "the index the constraint owned goes with it");
        Assert.AreEqual(commit ? 2 : 1, (await ExecQuery(executor, db, "SELECT id FROM child")).Count);

        if (commit)
            await ExecNonQuery(executor, db, "DELETE FROM parent WHERE id = 1");
        else
            Assert.AreEqual(CamusDBErrorCodes.ForeignKeyRestrictDelete, await Code(() => ExecNonQuery(executor, db, "DELETE FROM parent WHERE id = 1")));
    }

    /// <summary>
    /// A constraint that the transaction staged is in the shared schema, under a job record that names the
    /// transaction. A drop of it in the same transaction must keep that record until the outcome: it is
    /// what removes the constraint after a rollback.
    /// </summary>
    [Test]
    public async Task DropOfAConstraintThatTheTransactionStaged_LeavesNoConstraint([Values] bool commit)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateParentAndChildAsync(executor, db);

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, db, "ALTER TABLE child ADD CONSTRAINT fk FOREIGN KEY (pid) REFERENCES parent (id)");
        await Ddl(executor, tx, db, "ALTER TABLE parent ADD CONSTRAINT id_positive CHECK (id > 0)");
        await Ddl(executor, tx, db, "ALTER TABLE child DROP CONSTRAINT fk");
        await Ddl(executor, tx, db, "ALTER TABLE parent DROP CONSTRAINT id_positive");

        Assert.AreEqual(CamusDBErrorCodes.ForeignKeyViolation, await Code(() => ExecNonQuery(executor, db, "INSERT INTO child (id, pid) VALUES (3, 99)")),
            "the drop is the transaction's own until it commits");

        await EndAsync(database, tx, commit);
        await executor.DeferredPurgesForTests.WaitForScheduledRunsForTestsAsync();
        DatabaseDescriptor reopened = await ReopenAsync(executor, db);

        Assert.AreEqual(0, reopened.Schema.Tables["child"].ForeignKeys?.Count ?? 0);
        Assert.AreEqual(0, reopened.Schema.Tables["parent"].CheckConstraints?.Count ?? 0);
        Assert.IsEmpty(await executor.CatalogsForTests.LoadCoordinatorJobsAsync(reopened), "no job record is left");
        await ExecNonQuery(executor, db, "INSERT INTO child (id, pid) VALUES (4, 99)");
        await ExecNonQuery(executor, db, "INSERT INTO parent (id) VALUES (-1)");
    }

    private static async Task CreateParentAndChildAsync(CommandExecutor executor, string db)
    {
        await ExecDdl(executor, db, "CREATE TABLE parent (id INT64 PRIMARY KEY)");
        await ExecDdl(executor, db, "CREATE TABLE child (id INT64 PRIMARY KEY, pid INT64)");
        await ExecNonQuery(executor, db, "INSERT INTO parent (id) VALUES (1)");
        await ExecNonQuery(executor, db, "INSERT INTO child (id, pid) VALUES (1, 1)");
    }

    [Test]
    public async Task SetStorage_EndsWithTheTransaction([Values] bool commit)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, db, "CREATE TABLE t (id INT64 PRIMARY KEY, body STRING)");
        ColumnStorageStrategy? before = database.Schema.Tables["t"].Columns!.Single(c => c.Name == "body").Storage;
        string large = new('x', 20_000);

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, db, "ALTER TABLE t ALTER COLUMN body SET STORAGE EXTERNAL");
        await Write(executor, tx, db, $"INSERT INTO t (id, body) VALUES (1, '{large}')");
        Assert.AreEqual(before, database.Schema.Tables["t"].Columns!.Single(c => c.Name == "body").Storage);

        await EndAsync(database, tx, commit);
        DatabaseDescriptor reopened = await ReopenAsync(executor, db);

        Assert.AreEqual(commit ? ColumnStorageStrategy.External : before, reopened.Schema.Tables["t"].Columns!.Single(c => c.Name == "body").Storage);
        Assert.AreEqual(commit ? 1 : 0, (await ExecQuery(executor, db, "SELECT id FROM t")).Count);

        if (commit)
            Assert.AreEqual(large, await Text(executor, db, "SELECT body FROM t"));
    }

    [Test]
    public async Task TableSettings_EndWithTheTransaction([Values] bool commit)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, db, "CREATE TABLE t (id INT64 PRIMARY KEY, expires_at DATETIME)");
        await ExecDdl(executor, db, "CREATE TABLE u (id INT64 PRIMARY KEY, expires_at DATETIME)");
        await ExecDdl(executor, db, "ALTER TABLE u SET (ttl_expiration_expression = 'expires_at')");

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, db, "ALTER TABLE t SET (ttl_expiration_expression = 'expires_at')");
        await Ddl(executor, tx, db, "ALTER TABLE u RESET (ttl_expiration_expression)");
        Assert.IsFalse(HasTtl(database.Schema, "t"));
        Assert.IsTrue(HasTtl(database.Schema, "u"));

        await EndAsync(database, tx, commit);
        DatabaseDescriptor reopened = await ReopenAsync(executor, db);

        Assert.AreEqual(commit, HasTtl(reopened.Schema, "t"));
        Assert.AreEqual(!commit, HasTtl(reopened.Schema, "u"));

        static bool HasTtl(Schema schema, string table) =>
            schema.Tables[table].Settings?.ContainsKey(TableSettings.TtlExpirationExpressionKey) == true;
    }

    // ── CREATE TABLE ... AS ───────────────────────────────────────────────────

    [Test]
    public async Task CreateTableAsSelect_EndsWithTheTransaction([Values] bool commit, [Values] bool withData)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, db, "CREATE TABLE src (id INT64 PRIMARY KEY, v INT64)");
        await ExecNonQuery(executor, db, "INSERT INTO src (id, v) VALUES (1, 5), (2, 50)");
        long versionBefore = database.Schema.SchemaVersion;

        KvTransaction tx = await BeginSessionAsync(database);
        await Write(executor, tx, db, "INSERT INTO src (id, v) VALUES (3, 500)");
        await Ddl(executor, tx, db, "CREATE TABLE copy AS SELECT id, v FROM src WHERE v > 10" + (withData ? "" : " WITH NO DATA"));

        Assert.AreEqual(withData ? 2 : 0, (await Rows(executor, tx, db, "SELECT id FROM copy")).Count, "the copy holds the transaction's own row too");
        Assert.AreEqual(CamusDBErrorCodes.TableDoesntExist, await CodeOutside(executor, db, "SELECT id FROM copy"));

        await EndAsync(database, tx, commit);

        if (!commit)
        {
            AssertNoTrace(database, versionBefore);
            Assert.AreEqual(2, (await ExecQuery(executor, db, "SELECT id FROM src")).Count);
            return;
        }

        await ReopenAsync(executor, db);
        Assert.AreEqual(withData ? 2 : 0, (await ExecQuery(executor, db, "SELECT id FROM copy")).Count);
    }

    [Test]
    public async Task CreateTableAsSelect_ThatFails_LeavesNothingAndNeedsNoCompensation()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, db, "CREATE TABLE src (id INT64 PRIMARY KEY, v INT64)");
        await ExecNonQuery(executor, db, "INSERT INTO src (id, v) VALUES (1, 5), (2, 0)");
        long versionBefore = database.Schema.SchemaVersion;

        KvTransaction tx = await BeginSessionAsync(database);
        Assert.IsNotNull(await Code(() => Ddl(executor, tx, db, "CREATE TABLE copy AS SELECT id, 10 / v AS q FROM src")));
        Assert.AreEqual(CamusDBErrorCodes.TransactionRollbackOnly, await Code(() => Rows(executor, tx, db, "SELECT id FROM src")), "the failed statement aborts the transaction");
        await database.Transactions.RollbackAsync(tx);

        AssertNoTrace(database, versionBefore);
        Assert.IsFalse(database.Schema.Tables.ContainsKey("copy"));
        await ExecDdl(executor, db, "CREATE TABLE copy (id INT64 PRIMARY KEY)");
    }

    // ── owners, and what a dump reads ─────────────────────────────────────────

    [Test]
    public async Task ChangeOfOwner_EndsWithTheTransaction([Values] bool commit)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        string user = "u" + Guid.NewGuid().ToString("N")[..12];
        await ExecDdl(executor, db, $"CREATE USER {user} IDENTIFIED BY 'secret-{user}'");
        await ExecDdl(executor, db, "CREATE TABLE t (id INT64 PRIMARY KEY)");
        await ExecDdl(executor, db, "CREATE VIEW v AS SELECT id FROM t");
        await ExecDdl(executor, db, Slugify);

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, db, $"ALTER VIEW v OWNER TO {user}");
        await Ddl(executor, tx, db, $"ALTER FUNCTION slugify OWNER TO {user}");
        Assert.IsNull(database.Schema.Views["v"].Definition!.Owner);
        Assert.IsNull(database.Schema.Functions["slugify"].Owner);

        await EndAsync(database, tx, commit);
        DatabaseDescriptor reopened = await ReopenAsync(executor, db);

        Assert.AreEqual(commit ? user : null, reopened.Schema.Views["v"].Definition!.Owner);
        Assert.AreEqual(commit ? user : null, reopened.Schema.Functions["slugify"].Owner);
        Assert.AreEqual("a-b", await Text(executor, db, "SELECT slugify('A B')"));
    }

    /// <summary>
    /// The statements a dump reads describe the schema without the open transaction: its change set, and the
    /// index, the columns and the sequence that it staged on a shared table and that are not readable yet.
    /// </summary>
    [Test]
    public async Task WhatADumpReads_LeavesOutTheOpenTransaction()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, db, Posts);
        await ExecDdl(executor, db, "CREATE TABLE more (id INT64 PRIMARY KEY, v INT64)");
        await ExecDdl(executor, db, "CREATE TABLE third (id INT64 PRIMARY KEY, v INT64)");
        await ExecNonQuery(executor, db, "INSERT INTO posts (id, title) VALUES (1, 'A')");
        await ExecNonQuery(executor, db, "INSERT INTO more (id, v) VALUES (1, 1)");
        List<string> before = await DescribeAsync();

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, db, "CREATE INDEX posts_title ON posts (title)");
        await Ddl(executor, tx, db, "ALTER TABLE more ADD COLUMN n INT64 GENERATED ALWAYS AS IDENTITY");
        await Ddl(executor, tx, db, "ALTER TABLE third ADD COLUMN w INT64 DEFAULT(7)");
        await Ddl(executor, tx, db, Slugify);
        await Ddl(executor, tx, db, "CREATE TRIGGER slug ON third BEFORE INSERT LANGUAGE csharp AS $$ NEW!.v = 1; $$");
        await Ddl(executor, tx, db, "CREATE SEQUENCE s");
        await Ddl(executor, tx, db, "CREATE EXTENSION good");
        await Ddl(executor, tx, db, "CREATE VIEW v AS SELECT id FROM posts");
        await Ddl(executor, tx, db, "CREATE TABLE created (id INT64 PRIMARY KEY)");

        Assert.IsTrue(database.Schema.Sequences.ContainsKey("more_n_seq"), "the staged column has its sequence in the shared schema");
        CollectionAssert.AreEqual(before, await DescribeAsync());
        Assert.IsTrue((await Rows(executor, tx, db, "SHOW SEQUENCES")).Exists(row => row["sequence"].StrValue == "more_n_seq"), "the owner reads it");

        await database.Transactions.CommitAsync(tx);
        Assert.IsTrue((await ExecQuery(executor, db, "SHOW SEQUENCES")).Exists(row => row["sequence"].StrValue == "more_n_seq"));
        Assert.AreNotEqual(before.Count, (await DescribeAsync()).Count);

        async Task<List<string>> DescribeAsync()
        {
            List<string> text = [];
            List<string> statements =
            [
                "SHOW TABLES", "SHOW VIEWS", "SHOW SEQUENCES", "SHOW FUNCTIONS", "SHOW TRIGGERS",
                "SHOW CREATE TABLE posts", "SHOW CREATE TABLE more", "SHOW CREATE TABLE third", "SHOW CREATE TABLE posts WITHOUT INDEXES",
                "SHOW INDEXES FROM posts", "SHOW INDEXES FROM more",
            ];

            foreach (string statement in statements)
            {
                foreach (Dictionary<string, ColumnValue> row in await ExecQuery(executor, db, statement))
                    text.Add(statement + ": " + string.Join(" | ", row.OrderBy(pair => pair.Key).Select(pair => pair.Key + "=" + pair.Value)));
            }

            foreach (Dictionary<string, ColumnValue> row in await ExecQuery(executor, db, "SHOW EXTENSIONS"))
            {
                if (row["status"].StrValue == "installed")
                    text.Add("extension " + row["name"].StrValue);
            }

            return text;
        }
    }

    // ── recovery ──────────────────────────────────────────────────────────────

    /// <summary>
    /// The node stops between the KV commit and the publish: the reopen restores the pending change set from
    /// its record, without a database to apply it with, and the resolver publishes every kind of object.
    /// </summary>
    [Test]
    public async Task PendingObjects_SurviveAReopen_AndTheResolverPublishesThem()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, db, Posts);
        await ExecDdl(executor, db, "ALTER TABLE posts ADD CONSTRAINT titled CHECK (title <> '')");
        await ExecDdl(executor, db, "CREATE SEQUENCE old_s CACHE 1");
        Assert.AreEqual(1, await Number(executor, db, "SELECT nextval('old_s')"));

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, db, "CREATE EXTENSION good");
        await Ddl(executor, tx, db, Slugify);
        await Ddl(executor, tx, db, SlugTrigger);
        await Ddl(executor, tx, db, "ALTER TABLE posts DROP CONSTRAINT titled");
        await Ddl(executor, tx, db, "CREATE SEQUENCE new_s START WITH 7 CACHE 1");
        await Ddl(executor, tx, db, "ALTER SEQUENCE old_s INCREMENT BY 10");
        await Ddl(executor, tx, db, "CREATE TABLE copy AS SELECT id, title FROM posts");
        await Write(executor, tx, db, "INSERT INTO posts (id, title) VALUES (1, 'A B')");

        TransactionSchemaOverlay overlay = (TransactionSchemaOverlay)tx.SchemaChanges!;
        await overlay.PrepareCommitAsync(tx, CancellationToken.None);
        await database.Kahuna.Kahuna.LocateAndCommitTransaction(tx.Handle, CancellationToken.None);
        overlay.OutcomeUnknown = true;
        await overlay.CloseAsync();

        DatabaseDescriptor reopened = await ReopenAsync(executor, db);
        Assert.IsNotNull(reopened.Schema.PendingChangeSet);
        Assert.IsEmpty(reopened.Schema.Functions);

        Assert.AreEqual(true, await executor.ResolvePendingChangeSetForTests(reopened));
        Assert.AreEqual(1, await executor.DeferredPurgesForTests.RunDueAsync(reopened), "the counter of the altered sequence");

        DatabaseDescriptor again = await ReopenAsync(executor, db);
        Assert.AreEqual(0, again.Schema.Tables["posts"].CheckConstraints?.Count ?? 0);
        Assert.IsTrue(again.Schema.Tables.ContainsKey("copy"));
        Assert.AreEqual(5, await Number(executor, db, "SELECT fx_add(2, 3)"));
        Assert.AreEqual("a-b", await Text(executor, db, "SELECT slug FROM posts WHERE id = 1"));
        Assert.AreEqual(7, await Number(executor, db, "SELECT nextval('new_s')"));
        Assert.AreEqual(11, await Number(executor, db, "SELECT nextval('old_s')"));

        await ExecNonQuery(executor, db, "INSERT INTO posts (id, title) VALUES (2, 'C D')");
        Assert.AreEqual("c-d", await Text(executor, db, "SELECT slug FROM posts WHERE id = 2"));
    }

    // ── statements that change nothing, and statements that stay refused ───────

    [TestCase("CREATE SEQUENCE IF NOT EXISTS s")]
    [TestCase("DROP SEQUENCE IF EXISTS missing")]
    [TestCase("DROP FUNCTION IF EXISTS missing")]
    [TestCase("DROP EXTENSION IF EXISTS missing")]
    [TestCase("DROP MATERIALIZED VIEW IF EXISTS missing")]
    [TestCase("CREATE TABLE IF NOT EXISTS t AS SELECT id FROM t")]
    public async Task AStatementThatChangesNothing_LeavesNoPrivateSchema(string sql)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, db, "CREATE TABLE t (id INT64 PRIMARY KEY)");
        await ExecDdl(executor, db, "CREATE SEQUENCE s");

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, db, sql);
        Assert.IsNull(tx.SchemaChanges, "the DDL lock is free again");

        await ExecDdl(executor, db, "CREATE TABLE other (id INT64 PRIMARY KEY)");
        await database.Transactions.CommitAsync(tx);
    }

    private const string F = "CREATE FUNCTION f(x INT64) RETURNS INT64 LANGUAGE csharp AS $$ return x + 1; $$";

    private const string T = "CREATE TABLE t (id INT64 PRIMARY KEY, v INT64, name STRING)";

    private const string Trg = "CREATE TRIGGER trg ON t BEFORE INSERT LANGUAGE csharp AS $$ NEW!.v = 1; $$";

    /// <summary>
    /// A statement that is refused outside a transaction is refused inside one, with the same code, when the
    /// objects that refuse it exist only in the private schema of the transaction.
    /// </summary>
    [TestCase("CREATE SEQUENCE s", "CREATE SEQUENCE s", CamusDBErrorCodes.SequenceAlreadyExists)]
    [TestCase("CREATE SEQUENCE s", "CREATE TABLE s (id INT64 PRIMARY KEY)", CamusDBErrorCodes.SequenceAlreadyExists)]
    [TestCase(T, "CREATE SEQUENCE t", CamusDBErrorCodes.TableAlreadyExists)]
    [TestCase("CREATE SEQUENCE s; CREATE SEQUENCE s2", "ALTER SEQUENCE s RENAME TO s2", CamusDBErrorCodes.SequenceAlreadyExists)]
    [TestCase("", "DROP SEQUENCE s", CamusDBErrorCodes.SequenceDoesntExist)]
    [TestCase("", "ALTER SEQUENCE s INCREMENT BY 2", CamusDBErrorCodes.SequenceDoesntExist)]
    [TestCase("", "COMMENT ON SEQUENCE s IS 'x'", CamusDBErrorCodes.SequenceDoesntExist)]
    [TestCase("CREATE SEQUENCE s START WITH 5", "ALTER SEQUENCE s MINVALUE 9", CamusDBErrorCodes.InvalidSequenceDefinition)]
    [TestCase("CREATE SEQUENCE s; CREATE TABLE d (id INT64 PRIMARY KEY, n INT64 DEFAULT(nextval('s')))", "DROP SEQUENCE s", CamusDBErrorCodes.SequenceInUse)]
    [TestCase("CREATE TABLE i (id INT64 PRIMARY KEY, n INT64 GENERATED ALWAYS AS IDENTITY)", "DROP SEQUENCE i_n_seq", CamusDBErrorCodes.SequenceInUse)]
    [TestCase(F, F, CamusDBErrorCodes.FunctionAlreadyExists)]
    [TestCase("", "DROP FUNCTION f", CamusDBErrorCodes.FunctionNotFound)]
    [TestCase("CREATE EXTENSION good", "CREATE FUNCTION fx_add(x INT64) RETURNS INT64 LANGUAGE csharp AS $$ return x; $$", CamusDBErrorCodes.FunctionAlreadyExists)]
    [TestCase(F + ";" + T + "; ALTER TABLE t ADD CONSTRAINT c CHECK (f(v) > 0)", "CREATE OR REPLACE FUNCTION f(x INT64) RETURNS INT64 LANGUAGE csharp AS $$ return 5; $$", CamusDBErrorCodes.FunctionHasDependents)]
    [TestCase(F + ";" + T + "; CREATE VIEW w AS SELECT f(v) AS fv FROM t", "DROP FUNCTION f", CamusDBErrorCodes.FunctionHasDependents)]
    [TestCase("CREATE EXTENSION good", "CREATE EXTENSION good", CamusDBErrorCodes.ExtensionAlreadyInstalled)]
    [TestCase("", "DROP EXTENSION good", CamusDBErrorCodes.ExtensionNotInstalled)]
    [TestCase("CREATE EXTENSION good;" + T + "; CREATE VIEW w AS SELECT fx_add(v, 1) AS fv FROM t", "DROP EXTENSION good", CamusDBErrorCodes.ExtensionHasDependents)]
    [TestCase(T + ";" + Trg, Trg, CamusDBErrorCodes.TriggerAlreadyExists)]
    [TestCase(T, "DROP TRIGGER trg ON t", CamusDBErrorCodes.TriggerNotFound)]
    [TestCase(T + "; CREATE VIEW w AS SELECT id FROM t", "CREATE TRIGGER trg ON w BEFORE INSERT LANGUAGE csharp AS $$ return; $$", CamusDBErrorCodes.TriggerTargetInvalid)]
    [TestCase(T, "ALTER TABLE t DROP CONSTRAINT missing", CamusDBErrorCodes.InvalidInput)]
    [TestCase(T, "ALTER TABLE t ALTER COLUMN missing DROP NOT NULL", CamusDBErrorCodes.InvalidInput)]
    [TestCase(T, "ALTER TABLE t ALTER COLUMN id DROP NOT NULL", CamusDBErrorCodes.InvalidInput)]
    [TestCase(T, "ALTER TABLE t ALTER COLUMN missing SET STORAGE EXTERNAL", CamusDBErrorCodes.UnknownColumn)]
    [TestCase(T, "ALTER TABLE t ALTER COLUMN v SET STORAGE EXTERNAL", CamusDBErrorCodes.ColumnStorageNotApplicable)]
    [TestCase("", "COMMENT ON VIEW w IS 'x'", CamusDBErrorCodes.ViewDoesntExist)]
    [TestCase(T, "COMMENT ON VIEW t IS 'x'", CamusDBErrorCodes.ViewDoesntExist)]
    [TestCase(T, "COMMENT ON MATERIALIZED VIEW t IS 'x'", CamusDBErrorCodes.ViewDoesntExist)]
    [TestCase(T, "ALTER MATERIALIZED VIEW t RENAME TO u", CamusDBErrorCodes.ViewDoesntExist)]
    [TestCase(T, "DROP MATERIALIZED VIEW t", CamusDBErrorCodes.ViewDoesntExist)]
    [TestCase(T + "; CREATE VIEW w AS SELECT id FROM t; CREATE VIEW w2 AS SELECT id FROM w", "DROP VIEW w", CamusDBErrorCodes.DependentObjectsExist)]
    [TestCase(T, "CREATE TABLE t AS SELECT 1 AS id", CamusDBErrorCodes.TableAlreadyExists)]
    public async Task ARefusalOfAutocommit_AlsoHappensInsideATransaction(string setup, string statement, string code)
    {
        string[] setupStatements = SplitOutsideBodies(setup);

        (string outsideDb, _, CommandExecutor outside) = await CreateDatabase();
        foreach (string sql in setupStatements)
            await ExecDdl(outside, outsideDb, sql);
        Assert.AreEqual(code, await Code(() => ExecDdl(outside, outsideDb, statement)), "outside a transaction");

        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        long versionBefore = database.Schema.SchemaVersion;

        KvTransaction tx = await BeginSessionAsync(database);
        foreach (string sql in setupStatements)
            await Ddl(executor, tx, db, sql);
        Assert.AreEqual(code, await Code(() => Ddl(executor, tx, db, statement)), "inside a transaction");
        await database.Transactions.RollbackAsync(tx);

        AssertNoTrace(database, versionBefore);

        // A body between dollar quotes has semicolons of its own.
        static string[] SplitOutsideBodies(string text)
        {
            List<string> statements = [];
            int start = 0;
            bool body = false;

            for (int i = 0; i < text.Length; i++)
            {
                if (text[i] == '$' && i + 1 < text.Length && text[i + 1] == '$')
                {
                    body = !body;
                    i++;
                }
                else if (text[i] == ';' && !body)
                {
                    statements.Add(text[start..i].Trim());
                    start = i + 1;
                }
            }

            statements.Add(text[start..].Trim());
            return [.. statements.Where(sql => sql.Length > 0)];
        }
    }

    /// <summary>
    /// A CHECK on a committed table is checked and enforced with the functions that other transactions see, and
    /// other transactions fill a new column with the function of its default. So neither can call a member of
    /// an extension that only the transaction installed. Each can once the extension is committed, and a change
    /// of an extension that the statement does not name has no effect on it.
    /// </summary>
    [TestCase("ADD CONSTRAINT c CHECK (fx_add(v, 1) > 0)")]
    [TestCase("ADD CONSTRAINT c CHECK (good.fx_add(v, 1) > 0)")]
    [TestCase("ADD COLUMN n INT64 DEFAULT (fx_counter())")]
    public async Task AStatementOnACommittedTable_ThatCallsAMemberOfAnExtensionOfTheTransaction_IsRefused(string change)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDdl(executor, db, T);
        await ExecNonQuery(executor, db, "INSERT INTO t (id, v) VALUES (1, 5)");
        string statement = $"ALTER TABLE t {change}";
        long versionBefore = database.Schema.SchemaVersion;

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, db, "CREATE EXTENSION good");
        Assert.AreEqual(CamusDBErrorCodes.StatementNotAllowedInTransaction, await Code(() => Ddl(executor, tx, db, statement)));
        await database.Transactions.RollbackAsync(tx);
        AssertNoTrace(database, versionBefore);

        tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, db, "CREATE EXTENSION good");
        await Ddl(executor, tx, db, "ALTER TABLE t ADD CONSTRAINT other CHECK (abs(v) > 0)");
        await database.Transactions.CommitAsync(tx);

        tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, db, "CREATE SEQUENCE s");
        await Ddl(executor, tx, db, statement);
        await database.Transactions.CommitAsync(tx);

        if (database.Schema.Tables["t"].CheckConstraints!.Count == 2)
        {
            Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, await Code(() => ExecNonQuery(executor, db, "INSERT INTO t (id, v) VALUES (2, -9)")));
            return;
        }

        await ExecNonQuery(executor, db, "INSERT INTO t (id, v) VALUES (2, 6)");
        Assert.AreEqual(2, (await ExecQuery(executor, db, "SELECT DISTINCT n FROM t WHERE n IS NOT NULL")).Count);
    }

    [TestCase("CREATE MATERIALIZED VIEW mv2 AS SELECT id FROM t")]
    [TestCase("REFRESH MATERIALIZED VIEW mv")]
    [TestCase("CREATE TABLE back RELINK TO 'abc'")]
    [TestCase("TRUNCATE TABLE t")]
    [TestCase("ALTER TABLE t REWRITE STORAGE")]
    public async Task AnIrrevocableStatement_IsRefused(string sql)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateTableAndMaterializedViewAsync(executor, db);
        long versionBefore = database.Schema.SchemaVersion;

        KvTransaction tx = await BeginSessionAsync(database);
        Assert.AreEqual(CamusDBErrorCodes.StatementNotAllowedInTransaction, await Code(() => Ddl(executor, tx, db, sql)));
        await database.Transactions.RollbackAsync(tx);

        AssertNoTrace(database, versionBefore);
    }
}
