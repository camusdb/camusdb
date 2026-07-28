/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Covers DML against a database that has been renamed while its descriptor stayed cached.
///
/// <para>Rename deliberately keeps the cached <c>DatabaseDescriptor</c> alive — the cache is keyed by
/// id, and evicting it would orphan the running Kahuna node. The hazard that creates is a descriptor
/// whose <c>Name</c> is the pre-rename name; any code path that feeds that name back into a by-name
/// lookup resolves a name the registry no longer knows. These tests pin the behaviour for every DML
/// verb so a future path cannot quietly reintroduce it.</para>
/// </summary>
[TestFixture]
internal sealed class TestInsertAfterRenameDatabase : BaseTest
{
    private async Task<(string dbname, CommandExecutor executor)> CreateNamedDatabase()
    {
        string dbname = "db" + Guid.NewGuid().ToString("n");

        CommandExecutor executor = CreateCommandExecutor();
        await executor.CreateDatabase(new CreateDatabaseTicket(dbname, ifNotExists: false));
        TrackDatabase(dbname, executor);

        return (dbname, executor);
    }

    private static async Task ExecuteDdl(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null));
    }

    private static async Task<int> ExecuteNonQuery(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(
            new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null));
        await database.Transactions.CommitAsync(tx);
        return result.ModifiedRows;
    }

    private static async Task<int> CountRows(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null));

        int count = 0;
        await foreach (QueryResultRow _ in cursor)
            count++;

        await database.Transactions.CommitAsync(tx);
        return count;
    }

    /// <summary>
    /// The reported reproduction: rename, then create a table and insert into it under the new name,
    /// all on the same live executor. Before the fix this failed with
    /// <c>CADB0010 Database '&lt;old&gt;' does not exist</c> — the insert creator re-resolved the
    /// database from the cached descriptor's stale <c>Name</c>.
    /// </summary>
    [Test]
    public async Task DdlAndInsertAfterRename()
    {
        (string oldName, CommandExecutor executor) = await CreateNamedDatabase();
        string newName = "db" + Guid.NewGuid().ToString("n");

        await executor.RenameDatabase(new RenameDatabaseTicket(oldName, newName));
        TrackDatabase(newName, executor);

        await ExecuteDdl(executor, newName, "CREATE TABLE xx (id int64 PRIMARY KEY NOT NULL, name string(10))");

        Assert.AreEqual(1, await ExecuteNonQuery(executor, newName, "INSERT INTO xx VALUES (1, 'noisy')"));
        Assert.AreEqual(1, await CountRows(executor, newName, "SELECT * FROM xx"));
    }

    /// <summary>
    /// The same failure with the table created <em>before</em> the rename, so the descriptor and the
    /// table descriptor are both already cached when the name changes.
    /// </summary>
    [Test]
    public async Task InsertAfterRenameIntoAPreexistingTable()
    {
        (string oldName, CommandExecutor executor) = await CreateNamedDatabase();
        string newName = "db" + Guid.NewGuid().ToString("n");

        await ExecuteDdl(executor, oldName, "CREATE TABLE xx (id int64 PRIMARY KEY NOT NULL, name string(10))");
        await ExecuteNonQuery(executor, oldName, "INSERT INTO xx VALUES (1, 'before')");

        await executor.RenameDatabase(new RenameDatabaseTicket(oldName, newName));
        TrackDatabase(newName, executor);

        Assert.AreEqual(1, await ExecuteNonQuery(executor, newName, "INSERT INTO xx VALUES (2, 'after')"));
        Assert.AreEqual(2, await CountRows(executor, newName, "SELECT * FROM xx"));
    }

    /// <summary>
    /// UPDATE and DELETE go through their own creators. They do not have the defect today; this pins
    /// that, so adopting the insert path's by-name shape would fail here.
    /// </summary>
    [Test]
    public async Task UpdateAndDeleteAfterRename()
    {
        (string oldName, CommandExecutor executor) = await CreateNamedDatabase();
        string newName = "db" + Guid.NewGuid().ToString("n");

        await ExecuteDdl(executor, oldName, "CREATE TABLE xx (id int64 PRIMARY KEY NOT NULL, name string(10))");
        await ExecuteNonQuery(executor, oldName, "INSERT INTO xx VALUES (1, 'a')");

        await executor.RenameDatabase(new RenameDatabaseTicket(oldName, newName));
        TrackDatabase(newName, executor);

        Assert.AreEqual(1, await ExecuteNonQuery(executor, newName, "UPDATE xx SET name = 'b' WHERE id = 1"));
        Assert.AreEqual(1, await ExecuteNonQuery(executor, newName, "DELETE FROM xx WHERE id = 1"));
        Assert.AreEqual(0, await CountRows(executor, newName, "SELECT * FROM xx"));
    }

    /// <summary>
    /// The cached descriptor must carry the new name after a rename, with its id unchanged — the id
    /// is what proves the descriptor was refreshed in place rather than evicted and reloaded, which
    /// would orphan the running Kahuna node.
    /// </summary>
    [Test]
    public async Task RenameRefreshesTheCachedDescriptorName()
    {
        (string oldName, CommandExecutor executor) = await CreateNamedDatabase();
        string newName = "db" + Guid.NewGuid().ToString("n");

        DatabaseDescriptor before = await executor.OpenDatabase(oldName);
        string idBefore = before.Id;

        await executor.RenameDatabase(new RenameDatabaseTicket(oldName, newName));
        TrackDatabase(newName, executor);

        DatabaseDescriptor after = await executor.OpenDatabase(newName);

        Assert.AreSame(before, after, "the descriptor must not be evicted and reloaded");
        Assert.AreEqual(idBefore, after.Id, "the id is stable across a rename");
        Assert.AreEqual(newName, after.Name, "the cached descriptor must carry the new name");
    }

    /// <summary>
    /// A rename between BEGIN and INSERT. The statement must operate on the renamed database rather
    /// than fail resolving a name that no longer exists — the transaction is bound to the descriptor,
    /// and the descriptor's identity is unchanged.
    /// </summary>
    [Test]
    public async Task InsertOnATransactionOpenedBeforeTheRename()
    {
        (string oldName, CommandExecutor executor) = await CreateNamedDatabase();
        string newName = "db" + Guid.NewGuid().ToString("n");

        await ExecuteDdl(executor, oldName, "CREATE TABLE xx (id int64 PRIMARY KEY NOT NULL, name string(10))");

        DatabaseDescriptor database = await executor.OpenDatabase(oldName);
        KvTransaction tx = await database.Transactions.BeginAsync();

        await executor.RenameDatabase(new RenameDatabaseTicket(oldName, newName));
        TrackDatabase(newName, executor);

        // The ticket carries the new name; the in-flight transaction is unaffected by the rename.
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            txnState: tx, database: newName, sql: "INSERT INTO xx VALUES (1, 'in-flight')", parameters: null));
        await database.Transactions.CommitAsync(tx);

        Assert.AreEqual(1, await CountRows(executor, newName, "SELECT * FROM xx"));
    }

    /// <summary>
    /// Renaming twice must not leave the descriptor stranded on an intermediate name.
    /// </summary>
    [Test]
    public async Task InsertAfterTwoConsecutiveRenames()
    {
        (string first, CommandExecutor executor) = await CreateNamedDatabase();
        string second = "db" + Guid.NewGuid().ToString("n");
        string third = "db" + Guid.NewGuid().ToString("n");

        await ExecuteDdl(executor, first, "CREATE TABLE xx (id int64 PRIMARY KEY NOT NULL, name string(10))");

        await executor.RenameDatabase(new RenameDatabaseTicket(first, second));
        await executor.RenameDatabase(new RenameDatabaseTicket(second, third));
        TrackDatabase(third, executor);

        Assert.AreEqual(1, await ExecuteNonQuery(executor, third, "INSERT INTO xx VALUES (1, 'x')"));
        Assert.AreEqual(third, (await executor.OpenDatabase(third)).Name);
    }
}
