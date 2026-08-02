
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

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Exercises the batched UPDATE path (UpdateRowsBatch) on a COW branch database, where
/// <c>KvTableStore.ancestorStores</c> is non-empty so the branch code path runs: old index entries
/// are tombstoned (not physically deleted), new unique entries go through the per-item post-lock
/// ancestry probe (<c>ResolveBranchUniqueFlagsAsync</c>), and branch writes must stay isolated from
/// the ancestor. The root-only tests in <see cref="TestRowUpdaterBatch"/> never take this path.
/// </summary>
// Serial: boots an embedded Kahuna node per test. Running node-booting fixtures concurrently
// multiplies live nodes and is what exhausted memory in the suite before they were serialized.
[NonParallelizable]
internal sealed class TestRowUpdaterBatchBranch : BaseTest
{
    private static string NewName() => "db_" + Guid.NewGuid().ToString("n");

    private static async Task RunSqlAsync(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> SelectByValAsync(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, long val)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        (_, System.Collections.Generic.IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, $"SELECT * FROM items WHERE val = {val}", null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    private static async Task<List<QueryResultRow>> SelectAllAsync(
        CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        (_, System.Collections.Generic.IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, "SELECT * FROM items", null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    /// <summary>
    /// Creates a root with an <c>items(id, val)</c> table (optionally a unique index on val), seeds
    /// rows val=0..rowCount-1, then branches from it and returns both descriptors.
    /// </summary>
    private async Task<(string rootName, DatabaseDescriptor rootDb, string branchName, DatabaseDescriptor branchDb, CommandExecutor executor)>
        CreateRootAndBranch(int rowCount, bool uniqueIndex, CamusDBOptions? options = null)
    {
        (string rootName, DatabaseDescriptor rootDb, CommandExecutor executor) = await CreateDatabase(options ?? Options);
        TrackDatabase(rootName, executor);

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(null!, rootName,
            "CREATE TABLE items (id OBJECT_ID PRIMARY KEY, val INT64)", null));
        if (uniqueIndex)
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(null!, rootName,
                "CREATE UNIQUE INDEX u_val ON items (val)", null));

        for (int i = 0; i < rowCount; i++)
            await RunSqlAsync(executor, rootDb, rootName, $"INSERT INTO items (id, val) VALUES (gen_id(), {i})");

        string branchName = NewName();
        DatabaseDescriptor branchDb = await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        return (rootName, rootDb, branchName, branchDb, executor);
    }

    [Test]
    [NonParallelizable]
    public async Task BranchBatchUpdate_NonIndexedColumn_MultipleRows_SpanningChunks_RootUnchanged()
    {
        // Non-indexed column update on a branch, forced into chunks. Exercises the branch row-blob
        // batch write (batchItems with Set, no index mutations). Branch sees new values; root is
        // untouched (COW isolation).
        // A forced threshold of 3 so the update spans several chunks.
        (string rootName, DatabaseDescriptor rootDb, string branchName, DatabaseDescriptor branchDb, CommandExecutor executor) =
            await CreateRootAndBranch(rowCount: 10, uniqueIndex: false, options: Options with { ForceSpillThresholdRows = 3 });

        KvTransaction upTx = await branchDb.Transactions.BeginAsync();
        ExecuteNonSQLResult res = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            upTx, branchName, "UPDATE items SET val = val + 1000 WHERE val >= 0", null));
        await branchDb.Transactions.CommitAsync(upTx);

        Assert.AreEqual(10, res.ModifiedRows);

        List<long> branchVals = (await SelectAllAsync(executor, branchDb, branchName))
            .Select(r => r.Row["val"].LongValue).OrderBy(v => v).ToList();
        Assert.AreEqual(Enumerable.Range(1000, 10).Select(i => (long)i).ToList(), branchVals,
            "Branch must see the updated values across all chunks");

        List<long> rootVals = (await SelectAllAsync(executor, rootDb, rootName))
            .Select(r => r.Row["val"].LongValue).OrderBy(v => v).ToList();
        Assert.AreEqual(Enumerable.Range(0, 10).Select(i => (long)i).ToList(), rootVals,
            "Root must be unchanged by the branch update");
    }

    [Test]
    [NonParallelizable]
    public async Task BranchBatchUpdate_UniqueIndex_MultiRowChunk_BranchIsolatedFromRoot()
    {
        // Unique-index update on a branch across multiple chunks with distinct per-row values.
        // Exercises tombstoning inherited index entries + the per-item branch unique write. The new
        // keys must be findable on the branch, the old keys gone on the branch, and the root's index
        // must still resolve the original values.
        // A forced threshold of 3 so the update spans several chunks.
        (string rootName, DatabaseDescriptor rootDb, string branchName, DatabaseDescriptor branchDb, CommandExecutor executor) =
            await CreateRootAndBranch(rowCount: 8, uniqueIndex: true, options: Options with { ForceSpillThresholdRows = 3 });

        KvTransaction upTx = await branchDb.Transactions.BeginAsync();
        ExecuteNonSQLResult res = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            upTx, branchName, "UPDATE items SET val = val + 100 WHERE val >= 0", null));
        await branchDb.Transactions.CommitAsync(upTx);

        Assert.AreEqual(8, res.ModifiedRows);

        for (int i = 0; i < 8; i++)
        {
            Assert.AreEqual(1, (await SelectByValAsync(executor, branchDb, branchName, 100 + i)).Count,
                $"Branch index must resolve new value {100 + i}");
            Assert.AreEqual(0, (await SelectByValAsync(executor, branchDb, branchName, i)).Count,
                $"Branch index must no longer resolve old value {i}");

            // Root's index is untouched: original value present, branch's new value absent.
            Assert.AreEqual(1, (await SelectByValAsync(executor, rootDb, rootName, i)).Count,
                $"Root index must still resolve original value {i}");
            Assert.AreEqual(0, (await SelectByValAsync(executor, rootDb, rootName, 100 + i)).Count,
                $"Root index must not resolve the branch-only value {100 + i}");
        }
    }

    [Test]
    [NonParallelizable]
    public async Task BranchBatchUpdate_UniqueIndex_DuplicateAgainstInheritedValue_Throws()
    {
        // A branch row updated to a unique value that exists only in the ancestor (never touched on
        // the branch) must be rejected. Exercises ResolveBranchUniqueFlagsAsync's post-lock ancestry
        // probe: the new key is absent branch-locally but live in the ancestor for a different row.
        (string rootName, DatabaseDescriptor rootDb, string branchName, DatabaseDescriptor branchDb, CommandExecutor executor) =
            await CreateRootAndBranch(rowCount: 2, uniqueIndex: true); // val=0, val=1 inherited

        KvTransaction upTx = await branchDb.Transactions.BeginAsync();
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                upTx, branchName, "UPDATE items SET val = 0 WHERE val = 1", null)));
        await branchDb.Transactions.RollbackIfNotCompletedAsync(upTx);

        Assert.AreEqual(CamusDBErrorCodes.DuplicateUniqueKeyValue, ex?.Code,
            "Updating a branch row onto an ancestor-owned unique value must throw DuplicateUniqueKeyValue");
    }
}
