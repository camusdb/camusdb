/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CamusDB.Core.CommandsExecutor.Models.Queries;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Reproduction for the reported `teams` duplicate-primary-key issue. These tests extend
/// <see cref="BaseTest"/> directly (NOT <see cref="SharedNodeBaseTest"/>) so they run against the
/// standalone, SQLite-backed persistent Kahuna node (DatabaseOpener.CreateSqlite) — the same
/// persistent backend the production server uses. The equivalent in-memory tests pass, so the
/// hypothesis is that committed index entries are not visible to a later transaction's point
/// lookup on persistent storage (breaking update/delete-by-id and unique-key enforcement).
/// </summary>
internal sealed class TestPersistentIndexLookup : BaseTest
{
    private const string Id = "1e8921c8-58ed-483e-b4f2-c0f43cbc6c22";

    private static async Task CreateTeams(string dbname, DatabaseDescriptor database, CommandExecutor executor)
    {
        ExecuteDDLSQLResult ddl = await executor.ExecuteDDLSQL(new(
            await database.Transactions.BeginAsync(),
            dbname,
            "CREATE TABLE teams (id STRING NOT NULL PRIMARY KEY, code STRING NOT NULL, name STRING NOT NULL, name_es STRING NULL)",
            null));
        Assert.IsTrue(ddl.Success);
    }

    private static async Task<int> SelectByIdCount(string dbname, DatabaseDescriptor database, CommandExecutor executor)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new(tx, dbname, $"SELECT id FROM teams WHERE id = \"{Id}\"", null));
        int count = (await cursor.ToListAsync()).Count;
        await database.Transactions.CommitAsync(tx);
        return count;
    }

    private static async Task<int> CountAll(string dbname, DatabaseDescriptor database, CommandExecutor executor)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new(tx, dbname, "SELECT id FROM teams", null));
        int count = (await cursor.ToListAsync()).Count;
        await database.Transactions.CommitAsync(tx);
        return count;
    }

    /// <summary>Core probe: after a committed insert, a fresh transaction must find the row by PK.</summary>
    [Test]
    [NonParallelizable]
    public async Task TestPersistentLookupByPrimaryKeyAfterCommit()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateTeams(dbname, database, executor);

        KvTransaction txIns = await database.Transactions.BeginAsync();
        ExecuteNonSQLResult ins = await executor.ExecuteNonSQLQuery(new(txIns, dbname,
            $"INSERT INTO teams (id, code, name, name_es) VALUES (\"{Id}\", \"BEL\", \"Belgium\", null)", null));
        Assert.AreEqual(1, ins.ModifiedRows);
        await database.Transactions.CommitAsync(txIns);

        int byId = await SelectByIdCount(dbname, database, executor);
        Assert.AreEqual(1, byId, $"PK lookup after commit must find the row, found {byId}");
    }

    /// <summary>Re-inserting the same PK in a later transaction must be rejected (unique enforcement).</summary>
    [Test]
    [NonParallelizable]
    public async Task TestPersistentDuplicatePrimaryKeyRejectedAfterCommit()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateTeams(dbname, database, executor);

        const string sql = "INSERT INTO teams (id, code, name, name_es) VALUES (\"" + Id + "\", \"BEL\", \"Belgium\", null)";

        KvTransaction tx1 = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new(tx1, dbname, sql, null));
        await database.Transactions.CommitAsync(tx1);

        KvTransaction tx2 = await database.Transactions.BeginAsync();
        try
        {
            CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
                await executor.ExecuteNonSQLQuery(new(tx2, dbname, sql, null)));
            Assert.AreEqual(CamusDBErrorCodes.DuplicateUniqueKeyValue, ex!.Code);
        }
        finally
        {
            // Duplicate detection happens after the write set is registered. Finalize the failed
            // transaction before CountAll opens a reader over the same row/index keyspace.
            await database.Transactions.RollbackIfNotCompletedAsync(tx2);
        }

        int total = await CountAll(dbname, database, executor);
        Assert.AreEqual(1, total, $"Expected 1 row after duplicate insert attempt, found {total}");
    }

    /// <summary>
    /// Regression: SQL <c>CREATE INDEX</c> via ExecuteDDLSQL must survive close/reopen.
    /// Before the fix, the SQL path used single-phase DDL and skipped the schema-replication
    /// checkpoint, so the index was only in memory and vanished after the database was closed.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task SqlCreateIndex_PersistsAcrossReopen()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        TrackDatabase(dbname, executor);

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: dbname,
            sql: "CREATE TABLE products (id OBJECT_ID PRIMARY KEY, name STRING, price FLOAT64)",
            parameters: null));

        // Create index via SQL — this is the path that was broken.
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: dbname,
            sql: "CREATE INDEX name_idx ON products (name)",
            parameters: null));

        // Close and reopen to verify the index survived.
        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);

        Assert.IsTrue(reopened.Schema.Tables["products"].Indexes!.Any(ix => ix.Name == "name_idx"),
            "CREATE INDEX via SQL must persist across close/reopen");
    }

    /// <summary>
    /// Regression: SQL <c>CREATE UNIQUE INDEX</c> via ExecuteDDLSQL must survive close/reopen
    /// for the same reason as <see cref="SqlCreateIndex_PersistsAcrossReopen"/>.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task SqlCreateUniqueIndex_PersistsAcrossReopen()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        TrackDatabase(dbname, executor);

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: dbname,
            sql: "CREATE TABLE orders (id OBJECT_ID PRIMARY KEY, code STRING)",
            parameters: null));

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: dbname,
            sql: "CREATE UNIQUE INDEX code_uidx ON orders (code)",
            parameters: null));

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);

        Assert.IsTrue(reopened.Schema.Tables["orders"].Indexes!.Any(ix => ix.Name == "code_uidx"),
            "CREATE UNIQUE INDEX via SQL must persist across close/reopen");
    }

    /// <summary>
    /// The reported end-to-end symptom: an "upsert" (update; if 0 rows, delete then insert) run
    /// twice must leave a single row — not two physical rows with old/new name_es.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestPersistentUpsertTwiceLeavesSingleRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateTeams(dbname, database, executor);

        async Task Upsert(string nameEs)
        {
            // UPDATE; if it changed nothing, DELETE then INSERT (mirrors the app flow from the logs).
            KvTransaction txU = await database.Transactions.BeginAsync();
            ExecuteNonSQLResult upd = await executor.ExecuteNonSQLQuery(new(txU, dbname,
                $"UPDATE teams SET name_es = \"{nameEs}\" WHERE id = \"{Id}\"", null));
            await database.Transactions.CommitAsync(txU);

            if (upd.ModifiedRows == 0)
            {
                KvTransaction txD = await database.Transactions.BeginAsync();
                await executor.ExecuteNonSQLQuery(new(txD, dbname, $"DELETE FROM teams WHERE id = \"{Id}\"", null));
                await database.Transactions.CommitAsync(txD);

                KvTransaction txI = await database.Transactions.BeginAsync();
                await executor.ExecuteNonSQLQuery(new(txI, dbname,
                    $"INSERT INTO teams (id, code, name, name_es) VALUES (\"{Id}\", \"BEL\", \"Belgium\", \"{nameEs}\")", null));
                await database.Transactions.CommitAsync(txI);
            }
        }

        await Upsert("Belgium");   // first run: update finds nothing → insert
        await Upsert("Belgica");   // second run: update should find the row → no insert

        int total = await CountAll(dbname, database, executor);
        Assert.AreEqual(1, total, $"Upsert run twice must leave exactly 1 row, found {total}");
    }
}
