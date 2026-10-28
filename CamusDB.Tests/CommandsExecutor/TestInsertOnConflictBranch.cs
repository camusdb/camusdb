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

using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// <c>INSERT … ON CONFLICT</c> on a branch database, where a key can live only in the ancestor. The
/// arbiter probe walks the branch's ancestry, so a key the branch inherited conflicts exactly as a key
/// it wrote; the update then writes the branch's own copy and leaves the ancestor unchanged. A key the
/// branch deleted is a tombstone at the branch's level and does not conflict.
/// </summary>
[NonParallelizable]
internal sealed class TestInsertOnConflictBranch : SharedNodeBaseTest
{
    private static string NewName() => "db_" + Guid.NewGuid().ToString("n");

    private static async Task<ExecuteNonSQLResult> Run(string dbName, DatabaseDescriptor db, CommandExecutor executor, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        try
        {
            ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbName, sql, null));
            await db.Transactions.CommitAsync(tx);
            return result;
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<List<string>> Rows(string dbName, CommandExecutor executor)
    {
        KvTransaction tx = KvTransaction.CreateReadOnly();
        (_, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbName, "SELECT k, email, n FROM kv ORDER BY k", null));

        return (await cursor.ToListAsync())
            .Select(r => $"{r.Row["k"].LongValue}|{r.Row["email"].StrValue}|{r.Row["n"].LongValue}")
            .ToList();
    }

    private async Task<(string rootName, string branchName, DatabaseDescriptor branch, CommandExecutor executor)> Setup()
    {
        string rootName = NewName();
        CommandExecutor executor = CreateCommandExecutor(Options);
        TrackDatabase(rootName, executor);

        DatabaseDescriptor root = await executor.CreateDatabase(new CreateDatabaseTicket(rootName, ifNotExists: false));

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: rootName,
            sql: "CREATE TABLE kv (k INT64 NOT NULL, email STRING, n INT64, PRIMARY KEY (k), UNIQUE KEY kv_email (email))", parameters: null));

        await Run(rootName, root, executor, "INSERT INTO kv (k, email, n) VALUES (1, 'a@x', 10), (2, 'b@x', 20)");

        string branchName = NewName();
        DatabaseDescriptor branch = (await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: rootName,
            sql: $"CREATE DATABASE {branchName} BRANCH FROM {rootName}", parameters: null))).Database!;
        TrackDatabase(branchName, executor);

        return (rootName, branchName, branch, executor);
    }

    [Test]
    public async Task KeyThatExistsOnlyInTheAncestorConflicts()
    {
        (string rootName, string branchName, DatabaseDescriptor branch, CommandExecutor executor) = await Setup();

        ExecuteNonSQLResult byKey = await Run(branchName, branch, executor,
            "INSERT INTO kv (k, email, n) VALUES (1, 'a@x', 5), (3, 'c@x', 30) ON CONFLICT (k) DO UPDATE SET n = kv.n + excluded.n");
        Assert.AreEqual(2, byKey.ModifiedRows);

        ExecuteNonSQLResult byEmail = await Run(branchName, branch, executor,
            "INSERT INTO kv (k, email, n) VALUES (9, 'b@x', 1) ON CONFLICT (email) DO UPDATE SET n = kv.n + 1");
        Assert.AreEqual(1, byEmail.ModifiedRows);

        ExecuteNonSQLResult nothing = await Run(branchName, branch, executor,
            "INSERT INTO kv (k, email, n) VALUES (2, 'z@x', 0) ON CONFLICT DO NOTHING");
        Assert.AreEqual(0, nothing.ModifiedRows);

        CollectionAssert.AreEqual(new[] { "1|a@x|15", "2|b@x|21", "3|c@x|30" }, await Rows(branchName, executor));
        CollectionAssert.AreEqual(new[] { "1|a@x|10", "2|b@x|20" }, await Rows(rootName, executor), "the ancestor is unchanged");
    }

    [Test]
    public async Task KeyTheBranchDeletedDoesNotConflict()
    {
        (string rootName, string branchName, DatabaseDescriptor branch, CommandExecutor executor) = await Setup();

        await Run(branchName, branch, executor, "DELETE FROM kv WHERE k = 1");

        ExecuteNonSQLResult result = await Run(branchName, branch, executor,
            "INSERT INTO kv (k, email, n) VALUES (1, 'a@x', 7) ON CONFLICT (k) DO UPDATE SET n = 1000");

        Assert.AreEqual(1, result.ModifiedRows);
        CollectionAssert.AreEqual(new[] { "1|a@x|7", "2|b@x|20" }, await Rows(branchName, executor));
        CollectionAssert.AreEqual(new[] { "1|a@x|10", "2|b@x|20" }, await Rows(rootName, executor));
    }
}
