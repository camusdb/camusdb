/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna.Shared.KeyValue;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;
using CamusDB.Tests.CommandsExecutor;

namespace CamusDB.Tests.Transactions;

/// <summary>
/// A read-only transaction refuses every statement that writes rows with
/// <see cref="CamusDBErrorCodes.ReadOnlyTransaction"/>, before it takes a lock or stages a write.
/// Before this rule, the writes of a Serializable read-only transaction returned "ok" and were
/// discarded by its commit (a rollback of the Kahuna transaction), and they held their row locks
/// until the transaction ended.
///
/// <para>Each case runs every write statement shape in each kind of read-only transaction: begun
/// read-only at Serializable (a snapshot) or at Read Committed, deferred or eager, and made read-only
/// by <c>SET TRANSACTION</c>. For each statement the fixture checks the error, that nothing was staged
/// (<see cref="KvTransaction.WriteCount"/>), that no row is locked while the transaction is still open,
/// that the commit reports the transaction aborted (PostgreSQL's rule for a failed statement), and at
/// the end that no data changed.</para>
///
/// <para>Serial: boots an embedded Kahuna node per test.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestReadOnlyTransactionWrites : BaseTest
{
    /// <summary>Every write statement shape, with the verb the error must name.</summary>
    private static readonly (string Sql, string Verb, bool ViaQueryEndpoint)[] Writes =
    [
        ("UPDATE jobs SET state = 'ro-write' WHERE id = 3", "UPDATE", false),
        ("INSERT INTO jobs (id, state) VALUES (99, 'ro-insert')", "INSERT", false),
        ("DELETE FROM jobs WHERE id = 1", "DELETE", false),
        ("UPSERT INTO jobs (id, state) VALUES (3, 'ro-upsert')", "UPSERT", false),
        ("INSERT INTO jobs (id, state) VALUES (3, 'x') ON CONFLICT (id) DO UPDATE SET state = 'ro-conflict'", "INSERT", false),
        ("INSERT INTO archive (id, state) SELECT id + 100, state FROM jobs", "INSERT", false),
        ("UPDATE jobs SET state = archive.state FROM archive WHERE jobs.id = archive.id", "UPDATE", false),
        ("DELETE FROM jobs USING archive WHERE jobs.id = archive.id", "DELETE", false),
        ("UPDATE jobs SET state = 'ro-returning' WHERE id = 4 RETURNING id", "UPDATE", true),
        ("CREATE TABLE jobs_copy AS SELECT id, state FROM jobs", "CREATE TABLE AS", false),
    ];

    private static readonly List<(long, string)> SeedJobs =
        [(1, "queued"), (2, "running"), (3, "done"), (4, "failed")];

    private static readonly List<(long, string)> SeedArchive = [(2, "archived")];

    // ─── Helpers ──────────────────────────────────────────────────────────────

    private static async Task Ddl(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, sql, null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task Write(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> RowsIn(CommandExecutor executor, KvTransaction tx, string db, string sql)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
        return await cursor.ToListAsync();
    }

    private static async Task<List<(long, string)>> Committed(CommandExecutor executor, DatabaseDescriptor database, string db, string table)
    {
        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);
        try
        {
            List<QueryResultRow> rows = await RowsIn(executor, tx, db, $"SELECT id, state FROM {table}");
            return rows.Select(r => (r.Row["id"].LongValue, r.Row["state"].StrValue!)).OrderBy(p => p.Item1).ToList();
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>A database with <c>jobs</c> holding rows 1 to 4 and <c>archive</c> holding row 2.</summary>
    private async Task<(string db, DatabaseDescriptor database, CommandExecutor executor)> Jobs()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE jobs (id INT64 PRIMARY KEY, state STRING NOT NULL)");
        await Ddl(executor, database, db, "CREATE TABLE archive (id INT64 PRIMARY KEY, state STRING NOT NULL)");
        await Write(executor, database, db,
            "INSERT INTO jobs (id, state) VALUES (1, 'queued'), (2, 'running'), (3, 'done'), (4, 'failed')");
        await Write(executor, database, db, "INSERT INTO archive (id, state) VALUES (2, 'archived')");
        return (db, database, executor);
    }

    /// <summary>
    /// Proves no row of <c>jobs</c> is locked: another transaction locks every row with NOWAIT, which
    /// fails at once with <see cref="CamusDBErrorCodes.LockNotAvailable"/> when any row is held.
    /// </summary>
    private static async Task AssertNoJobIsLocked(CommandExecutor executor, DatabaseDescriptor database, string db, string because)
    {
        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite, locking: KeyValueTransactionLocking.Pessimistic);
        try
        {
            List<QueryResultRow> rows = await RowsIn(executor, tx, db, "SELECT id FROM jobs FOR UPDATE NOWAIT");
            Assert.That(rows, Has.Count.EqualTo(SeedJobs.Count), because);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>
    /// Runs every statement of <see cref="Writes"/>, each in a new transaction from
    /// <paramref name="begin"/>, and checks the refusal, the absence of staged writes and of locks,
    /// the aborted commit, and finally that no data changed.
    /// </summary>
    private async Task AssertEveryWriteIsRefused(Func<DatabaseDescriptor, string, CommandExecutor, Task<KvTransaction>> begin)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Jobs();

        foreach ((string sql, string verb, bool viaQueryEndpoint) in Writes)
        {
            KvTransaction tx = await begin(database, db, executor);
            Assert.That(tx.RefusesWrites, Is.True, $"the transaction under test is read-only ({sql})");

            CamusDBException error = Assert.ThrowsAsync<CamusDBException>(async () =>
            {
                ExecuteSQLTicket ticket = new(tx, db, sql, null);
                if (viaQueryEndpoint)
                    await (await executor.ExecuteSQLQuery(ticket)).Item2.ToListAsync();
                else
                    await executor.ExecuteNonSQLQuery(ticket);
            })!;

            Assert.That(error.Code, Is.EqualTo(CamusDBErrorCodes.ReadOnlyTransaction), $"{sql}: {error.Message}");
            Assert.That(error.Message, Is.EqualTo($"cannot execute {verb} in a read-only transaction"), sql);
            Assert.That(tx.WriteCount, Is.Zero, $"{sql}: nothing was staged");

            await AssertNoJobIsLocked(executor, database, db, $"{sql}: the refused write took no row lock");

            CamusDBException commit = Assert.ThrowsAsync<CamusDBException>(async () => await database.Transactions.CommitAsync(tx))!;
            Assert.That(commit.Code, Is.EqualTo(CamusDBErrorCodes.TransactionRollbackOnly), $"{sql}: the refused statement aborted the transaction");
        }

        Assert.That(await Committed(executor, database, db, "jobs"), Is.EqualTo(SeedJobs), "no write reached the table");
        Assert.That(await Committed(executor, database, db, "archive"), Is.EqualTo(SeedArchive), "no write reached the table");
        Assert.That(database.Schema.Tables.ContainsKey("jobs_copy"), Is.False, "CREATE TABLE AS was refused before it created the table");
    }

    // ─── Every kind of read-only transaction ──────────────────────────────────

    /// <summary>The evidence of the report: a Serializable read-only transaction, whose commit is a rollback.</summary>
    [Test]
    public Task SerializableReadOnly_RefusesEveryWrite() =>
        AssertEveryWriteIsRefused((database, _, _) =>
            database.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly));

    /// <summary>A Read Committed read-only transaction uses the normal commit path, so its writes used to commit.</summary>
    [Test]
    public Task ReadCommittedReadOnly_RefusesEveryWrite() =>
        AssertEveryWriteIsRefused((database, _, _) =>
            database.Transactions.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadOnly));

    /// <summary>The explicit transaction of the HTTP and gRPC transports starts its Kahuna session late.</summary>
    [Test]
    public Task DeferredReadCommittedReadOnly_RefusesEveryWrite() =>
        AssertEveryWriteIsRefused((database, _, _) =>
            database.Transactions.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadOnly, deferStart: true));

    /// <summary>The documented way to make a transaction read-only, which used to have no effect.</summary>
    [Test]
    public Task SetTransactionSerializableReadOnly_RefusesEveryWrite() =>
        AssertEveryWriteIsRefused(async (database, db, executor) =>
        {
            KvTransaction tx = await database.Transactions.BeginAsync(deferStart: true);
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY", null));
            return tx;
        });

    /// <summary>The same statement on an eager transaction, whose Kahuna session is already open.</summary>
    [Test]
    public Task SetTransactionSerializableReadOnly_OnAnEagerTransaction_RefusesEveryWrite() =>
        AssertEveryWriteIsRefused(async (database, db, executor) =>
        {
            KvTransaction tx = await database.Transactions.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite);
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY", null));
            return tx;
        });

    /// <summary>
    /// A level change that names no access mode keeps the mode, as in PostgreSQL. It used to set
    /// READ WRITE and so made a read-only transaction writable.
    /// </summary>
    [Test]
    public Task SetTransactionIsolationLevelWithoutAMode_KeepsTheTransactionReadOnly() =>
        AssertEveryWriteIsRefused(async (database, db, executor) =>
        {
            KvTransaction tx = await database.Transactions.BeginAsync(
                CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadOnly, deferStart: true);
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", null));
            Assert.That(tx.TransactionMode, Is.EqualTo(CamusTransactionMode.ReadOnly));
            return tx;
        });

    // ─── SET TRANSACTION ──────────────────────────────────────────────────────

    /// <summary>
    /// <c>SET TRANSACTION … SERIALIZABLE READ ONLY</c> gives the transaction one snapshot, as a
    /// transaction begun that way has. Without it a read-only transaction takes no read lock, so each
    /// read saw the latest committed value.
    /// </summary>
    [Test]
    public async Task SetTransactionSerializableReadOnly_ReadsOneSnapshot()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Jobs();

        KvTransaction tx = await database.Transactions.BeginAsync(deferStart: true);
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY", null));

        Assert.That(tx.IsolationLevel, Is.EqualTo(CamusIsolationLevel.Serializable));
        Assert.That(tx.TransactionMode, Is.EqualTo(CamusTransactionMode.ReadOnly));
        Assert.That(tx.ReadTimestamp.IsNull(), Is.False, "the transaction reads at one fixed snapshot");

        List<QueryResultRow> before = await RowsIn(executor, tx, db, "SELECT state FROM jobs WHERE id = 1");
        Assert.That(before.Single().Row["state"].StrValue, Is.EqualTo("queued"));

        await Write(executor, database, db, "UPDATE jobs SET state = 'changed' WHERE id = 1");

        List<QueryResultRow> after = await RowsIn(executor, tx, db, "SELECT state FROM jobs WHERE id = 1");
        Assert.That(after.Single().Row["state"].StrValue, Is.EqualTo("queued"), "a commit after the snapshot is not visible");

        await database.Transactions.CommitAsync(tx);
        Assert.That(tx.Status, Is.EqualTo(KvTransactionStatus.Committed));
    }

    /// <summary>
    /// A transaction begun read-only at Serializable reads at a fixed snapshot and has no write session.
    /// READ WRITE cannot change that, so it is refused instead of being accepted without effect.
    /// </summary>
    [Test]
    public async Task SetTransactionReadWrite_OnASerializableReadOnlyTransaction_IsRefused()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Jobs();

        KvTransaction tx = await database.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);
        try
        {
            CamusDBException error = Assert.ThrowsAsync<CamusDBException>(async () =>
                await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ WRITE", null)))!;

            Assert.That(error.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput));
            Assert.That(tx.TransactionMode, Is.EqualTo(CamusTransactionMode.ReadOnly));
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>
    /// After <c>SET TRANSACTION … READ ONLY</c> pinned a snapshot, a second <c>SET TRANSACTION … READ
    /// WRITE</c> cannot undo it, so it is refused too.
    /// </summary>
    [Test]
    public async Task SetTransactionReadWrite_AfterSetTransactionReadOnly_IsRefused()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Jobs();

        KvTransaction tx = await database.Transactions.BeginAsync(deferStart: true);
        try
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY", null));

            CamusDBException error = Assert.ThrowsAsync<CamusDBException>(async () =>
                await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ WRITE", null)))!;

            Assert.That(error.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput));
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>
    /// A Read Committed read-only transaction has no fixed snapshot, so READ WRITE before the first
    /// statement makes it writable, as PostgreSQL allows. The write then commits.
    /// </summary>
    [Test]
    public async Task SetTransactionReadWrite_OnAReadCommittedReadOnlyTransaction_MakesItWritable()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Jobs();

        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadOnly, deferStart: true);

        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ WRITE", null));
        Assert.That(tx.RefusesWrites, Is.False);

        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, "UPDATE jobs SET state = 'written' WHERE id = 3", null));
        await database.Transactions.CommitAsync(tx);

        Assert.That((await Committed(executor, database, db, "jobs")).Single(r => r.Item1 == 3).Item2, Is.EqualTo("written"));
    }

    // ─── Statements that change state outside the transaction ────────────────

    /// <summary>
    /// Statements that change state in transactions of their own, with the command name the error
    /// must give. A rollback of the read-only transaction could not undo any of them.
    /// </summary>
    private static readonly (string Sql, string Command)[] OutsideStateChanges =
    [
        ("CREATE TABLE notes (id INT64 PRIMARY KEY, body STRING)", "CREATE TABLE"),
        ("CREATE INDEX jobs_state ON jobs (state)", "CREATE INDEX"),
        ("ALTER TABLE jobs ADD COLUMN priority INT64", "ALTER TABLE"),
        ("CREATE TABLE jobs_shape AS SELECT id, state FROM jobs WITH NO DATA", "CREATE TABLE AS"),
        ("TRUNCATE TABLE archive", "TRUNCATE TABLE"),
        ("DROP TABLE archive", "DROP TABLE"),
        ("CREATE VIEW queued AS SELECT id FROM jobs WHERE state = 'queued'", "CREATE VIEW"),
        ("CREATE SEQUENCE job_ids", "CREATE SEQUENCE"),
        ("COMMENT ON TABLE jobs IS 'work items'", "COMMENT"),
        ("CREATE MATERIALIZED VIEW done_jobs AS SELECT id FROM jobs WHERE state = 'done' WITH NO DATA", "CREATE MATERIALIZED VIEW"),
    ];

    /// <summary>
    /// A transaction that the client declared read-only refuses schema DDL, <c>COMMENT ON</c> and
    /// <c>CREATE TABLE … AS … WITH NO DATA</c>, at the DDL entry point and at the non-query one, before
    /// anything changes. The DDL commits in transactions of its own, so the read-only transaction
    /// could not undo it.
    /// </summary>
    [Test]
    public async Task SchemaDdl_InADeclaredReadOnlyTransaction_IsRefused()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Jobs();
        long schemaVersion = database.Schema.SchemaVersion;

        foreach ((string sql, string command) in OutsideStateChanges)
        {
            foreach (bool viaDdlEndpoint in new[] { true, false })
            {
                KvTransaction tx = await database.Transactions.BeginAsync(
                    CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadOnly, deferStart: true);
                try
                {
                    CamusDBException error = Assert.ThrowsAsync<CamusDBException>(async () =>
                    {
                        ExecuteSQLTicket ticket = new(tx, db, sql, null);
                        if (viaDdlEndpoint)
                            await executor.ExecuteDDLSQL(ticket);
                        else
                            await executor.ExecuteNonSQLQuery(ticket);
                    })!;

                    Assert.That(error.Code, Is.EqualTo(CamusDBErrorCodes.ReadOnlyTransaction), $"{sql}: {error.Message}");
                    Assert.That(error.Message, Is.EqualTo($"cannot execute {command} in a read-only transaction"), sql);
                }
                finally
                {
                    await database.Transactions.RollbackIfNotCompletedAsync(tx);
                }
            }
        }

        Assert.That(database.Schema.SchemaVersion, Is.EqualTo(schemaVersion), "no schema change was applied");
        foreach (string table in new[] { "notes", "jobs_shape", "done_jobs" })
            Assert.That(database.Schema.Tables.ContainsKey(table), Is.False, table);
        Assert.That(database.Schema.Views.ContainsKey("queued"), Is.False);
        Assert.That(database.Schema.Tables.ContainsKey("archive"), Is.True);
        Assert.That(await Committed(executor, database, db, "archive"), Is.EqualTo(SeedArchive));
        Assert.That(database.Schema.Tables["jobs"].Columns!.Exists(c => c.Name == "priority"), Is.False);
    }

    /// <summary>The same refusal in the other kinds of declared read-only transaction.</summary>
    [Test]
    public async Task SchemaDdl_InEveryKindOfDeclaredReadOnlyTransaction_IsRefused()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Jobs();

        Func<Task<KvTransaction>>[] begins =
        [
            () => database.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly),
            () => database.Transactions.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadOnly),
            async () =>
            {
                KvTransaction set = await database.Transactions.BeginAsync(deferStart: true);
                await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(set, db, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY", null));
                return set;
            },
            () => Task.FromResult(database.Transactions.BeginReadOnly(declaredReadOnly: true)),
        ];

        foreach (Func<Task<KvTransaction>> begin in begins)
        {
            KvTransaction tx = await begin();
            Assert.That(tx.IsDeclaredReadOnly, Is.True);
            try
            {
                CamusDBException error = Assert.ThrowsAsync<CamusDBException>(async () =>
                    await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, "DROP TABLE archive", null)))!;
                Assert.That(error.Code, Is.EqualTo(CamusDBErrorCodes.ReadOnlyTransaction));
            }
            finally
            {
                await database.Transactions.RollbackIfNotCompletedAsync(tx);
            }
        }

        Assert.That(database.Schema.Tables.ContainsKey("archive"), Is.True);
    }

    /// <summary>
    /// <c>REFRESH MATERIALIZED VIEW</c> is refused in a declared read-only transaction. A snapshot
    /// that the server picked is a different case: an internal job, such as a restarted rebuild, runs
    /// a refresh under one, and the refresh works there.
    /// </summary>
    [Test]
    public async Task Refresh_InADeclaredReadOnlyTransaction_IsRefused_AndUnderAServerSnapshotRuns()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Jobs();
        await Ddl(executor, database, db, "CREATE MATERIALIZED VIEW done_jobs AS SELECT id FROM jobs WHERE state = 'done' WITH NO DATA");

        KvTransaction declared = await database.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);
        try
        {
            CamusDBException error = Assert.ThrowsAsync<CamusDBException>(async () =>
                await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(declared, db, "REFRESH MATERIALIZED VIEW done_jobs", null)))!;

            Assert.That(error.Code, Is.EqualTo(CamusDBErrorCodes.ReadOnlyTransaction));
            Assert.That(error.Message, Is.EqualTo("cannot execute REFRESH MATERIALIZED VIEW in a read-only transaction"));
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(declared);
        }

        KvTransaction serverSnapshot = database.Transactions.BeginReadOnly();
        Assert.That(serverSnapshot.IsDeclaredReadOnly, Is.False);
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(serverSnapshot, db, "REFRESH MATERIALIZED VIEW done_jobs", null));

        List<QueryResultRow> rows = await RowsIn(executor, database.Transactions.BeginReadOnly(), db, "SELECT id FROM done_jobs");
        Assert.That(rows.Select(r => r.Row["id"].LongValue), Is.EqualTo(new[] { 3L }));
    }

    /// <summary>
    /// <c>nextval()</c> and <c>setval()</c> are refused in a declared read-only transaction, before a
    /// value is drawn or the counter moves. A sequence is not transactional, so a rollback could not
    /// return the value. <c>currval()</c> reads only and is not refused for being in a read-only
    /// transaction.
    /// </summary>
    [Test]
    public async Task SequenceFunctions_InADeclaredReadOnlyTransaction_AreRefused()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Jobs();
        await Ddl(executor, database, db, "CREATE SEQUENCE job_ids");

        (string Sql, string Function)[] calls =
        [
            ("SELECT nextval('job_ids')", "nextval()"),
            ("SELECT setval('job_ids', 500)", "setval()"),
        ];

        foreach ((string sql, string function) in calls)
        {
            KvTransaction tx = await database.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);
            try
            {
                CamusDBException error = Assert.ThrowsAsync<CamusDBException>(async () => await RowsIn(executor, tx, db, sql))!;

                Assert.That(error.Code, Is.EqualTo(CamusDBErrorCodes.ReadOnlyTransaction), sql);
                Assert.That(error.Message, Is.EqualTo($"cannot execute {function} in a read-only transaction"));
            }
            finally
            {
                await database.Transactions.RollbackIfNotCompletedAsync(tx);
            }
        }

        KvTransaction reader = await database.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);
        try
        {
            CamusDBException currval = Assert.ThrowsAsync<CamusDBException>(async () =>
                await RowsIn(executor, reader, db, "SELECT currval('job_ids')"))!;
            Assert.That(currval.Code, Is.EqualTo(CamusDBErrorCodes.SequenceValueNotYetDefined), "currval reads only; it fails for its own reason");
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(reader);
        }

        // Neither refused call moved the counter: the first value drawn afterwards is the start value.
        List<QueryResultRow> drawn = await RowsIn(executor, database.Transactions.BeginReadOnly(), db, "SELECT nextval('job_ids') AS v");
        Assert.That(drawn.Single().Row["v"].LongValue, Is.EqualTo(1L));
    }

    /// <summary>
    /// The snapshot that the server picks for an autocommit <c>SELECT</c> is not a declared read-only
    /// transaction, so <c>SELECT nextval('s')</c> works there. The same snapshot declared read-only,
    /// as for a request that asked for <c>ReadOnly</c>, refuses it.
    /// </summary>
    [Test]
    public async Task Nextval_InTheServerSnapshot_Runs_AndInADeclaredSnapshot_IsRefused()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Jobs();
        await Ddl(executor, database, db, "CREATE SEQUENCE job_ids");

        List<QueryResultRow> drawn = await RowsIn(executor, database.Transactions.BeginReadOnly(), db, "SELECT nextval('job_ids') AS v");
        Assert.That(drawn.Single().Row["v"].LongValue, Is.EqualTo(1L));

        CamusDBException error = Assert.ThrowsAsync<CamusDBException>(async () =>
            await RowsIn(executor, database.Transactions.BeginReadOnly(declaredReadOnly: true), db, "SELECT nextval('job_ids')"))!;
        Assert.That(error.Code, Is.EqualTo(CamusDBErrorCodes.ReadOnlyTransaction));
    }

    // ─── The other write entry points and the safety nets ─────────────────────

    /// <summary>The ticket path that the gRPC row service uses refuses an insert too.</summary>
    [Test]
    public async Task InsertTicket_InAReadOnlyTransaction_IsRefused()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Jobs();

        KvTransaction tx = await database.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);
        try
        {
            InsertTicket ticket = new(
                txnState: tx,
                databaseName: db,
                tableName: "jobs",
                values:
                [
                    new Dictionary<string, ColumnValue>
                    {
                        ["id"] = new(ColumnType.Integer64, 50L),
                        ["state"] = new(ColumnType.String, "ticket"),
                    },
                ]);

            CamusDBException error = Assert.ThrowsAsync<CamusDBException>(async () => await executor.Insert(ticket))!;

            Assert.That(error.Code, Is.EqualTo(CamusDBErrorCodes.ReadOnlyTransaction));
            Assert.That(error.Message, Is.EqualTo("cannot execute INSERT in a read-only transaction"));
            Assert.That(tx.WriteCount, Is.Zero);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }

        Assert.That(await Committed(executor, database, db, "jobs"), Is.EqualTo(SeedJobs));
    }

    /// <summary>A Read Committed read-only transaction cannot own a row lock either.</summary>
    [Test]
    public async Task SelectForUpdate_InAReadCommittedReadOnlyTransaction_IsRefused()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Jobs();

        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadOnly,
            locking: KeyValueTransactionLocking.Pessimistic, deferStart: true);
        try
        {
            CamusDBException error = Assert.ThrowsAsync<CamusDBException>(async () =>
                await RowsIn(executor, tx, db, "SELECT id FROM jobs WHERE id = 1 FOR UPDATE"))!;

            Assert.That(error.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput));
            StringAssert.Contains("read-only transaction", error.Message);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>
    /// The storage write paths reserve their mutations before they take the write locks. A read-only
    /// transaction is refused there too, so a write path that skips the statement check still writes
    /// nothing, and the write mark does not move.
    /// </summary>
    [Test]
    public async Task ReserveMutations_InAReadOnlyTransaction_IsRefused()
    {
        (_, DatabaseDescriptor database, _) = await Jobs();

        foreach ((CamusIsolationLevel level, bool deferStart) in new[]
                 {
                     (CamusIsolationLevel.Serializable, false),
                     (CamusIsolationLevel.ReadCommitted, false),
                     (CamusIsolationLevel.ReadCommitted, true),
                 })
        {
            KvTransaction tx = await database.Transactions.BeginAsync(level, CamusTransactionMode.ReadOnly, deferStart: deferStart);
            try
            {
                CamusDBException error = Assert.Throws<CamusDBException>(() => tx.ReserveMutations(1))!;
                Assert.That(error.Code, Is.EqualTo(CamusDBErrorCodes.ReadOnlyTransaction), $"{level}, deferred: {deferStart}");
                Assert.That(tx.WriteCount, Is.Zero);
            }
            finally
            {
                await database.Transactions.RollbackIfNotCompletedAsync(tx);
            }
        }
    }

    /// <summary>
    /// The commit of a read-only transaction that staged a write fails and rolls back. Its normal
    /// commit is a rollback of the Kahuna transaction, so without this check the write would be
    /// discarded while the client is told it committed.
    /// </summary>
    [Test]
    public async Task Commit_OfAReadOnlyTransactionThatStagedAWrite_FailsAndRollsBack()
    {
        (_, DatabaseDescriptor database, _) = await Jobs();

        foreach (CamusIsolationLevel level in new[] { CamusIsolationLevel.Serializable, CamusIsolationLevel.ReadCommitted })
        {
            KvTransaction tx = await database.Transactions.BeginAsync(level, CamusTransactionMode.ReadOnly);

            // Stands in for a write path that got past every check.
            tx.TrackModified("readonly-commit-guard/key", KeyValueDurability.Persistent);

            CamusDBException error = Assert.ThrowsAsync<CamusDBException>(async () => await database.Transactions.CommitAsync(tx))!;

            Assert.That(error.Code, Is.EqualTo(CamusDBErrorCodes.ReadOnlyTransaction), level.ToString());
            Assert.That(tx.Status, Is.EqualTo(KvTransactionStatus.RolledBack), level.ToString());
        }
    }
}

/// <summary>
/// The command names that the read-only refusal message gives, in the style of PostgreSQL command
/// tags. A pure function, so this fixture boots no node.
/// </summary>
[TestFixture]
internal sealed class TestReadOnlyStatementRuleNames
{
    /// <summary>The command names of the refusal message, in the style of PostgreSQL command tags.</summary>
    [TestCase(NodeType.CreateTable, "CREATE TABLE")]
    [TestCase(NodeType.CreateTableIfNotExists, "CREATE TABLE")]
    [TestCase(NodeType.CreateOrReplaceView, "CREATE VIEW")]
    [TestCase(NodeType.DropMaterializedViewIfExists, "DROP MATERIALIZED VIEW")]
    [TestCase(NodeType.AlterTableAddColumn, "ALTER TABLE")]
    [TestCase(NodeType.AlterTableAddUniqueIndexIfNotExists, "CREATE INDEX")]
    [TestCase(NodeType.AlterTableDropIndex, "DROP INDEX")]
    [TestCase(NodeType.CommentOnColumn, "COMMENT")]
    [TestCase(NodeType.RefreshMaterializedView, "REFRESH MATERIALIZED VIEW")]
    [TestCase(NodeType.CreateOrReplaceFunction, "CREATE FUNCTION")]
    [TestCase(NodeType.AlterSequenceRenameTo, "ALTER SEQUENCE")]
    public void CommandName_NamesTheVerbAndTheObjectType(NodeType nodeType, string expected) =>
        Assert.That(ReadOnlyStatementRule.CommandName(nodeType), Is.EqualTo(expected));
}
