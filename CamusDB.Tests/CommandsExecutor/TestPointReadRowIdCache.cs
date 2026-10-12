/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A primary-key point SELECT or UPDATE remembers the row id its key resolved to and reads the row
/// directly the next time: the second lookup of a key costs one KV read, not two. The remembered id is
/// never trusted for a negative answer, so a deleted row, and a key re-inserted under a new row id, are
/// answered through the index exactly as without the cache.
/// </summary>
[TestFixture]
public sealed class TestPointReadRowIdCache : BaseTest
{
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults)
        => defaults with { PointReadRowIdCacheEnabled = true };

    private static async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupAccounts(
        TestPointReadRowIdCache fixture)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await fixture.CreateDatabase();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "accounts",
            columns:
            [
                new("id", ColumnType.Integer64),
                new("balance", ColumnType.Integer64),
            ],
            constraints: [new(ConstraintType.PrimaryKey, CamusDBConstants.PrimaryKeyInternalName, [new("id", OrderType.Ascending)])],
            ifNotExists: false
        ));

        await NonQuery(database, executor, dbname, "INSERT INTO accounts (id, balance) VALUES (1, 100)");
        await NonQuery(database, executor, dbname, "INSERT INTO accounts (id, balance) VALUES (2, 200)");
        return (dbname, database, executor);
    }

    private static async Task NonQuery(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<List<long>> Balances(DatabaseDescriptor database, CommandExecutor executor, string dbname, long id)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: tx, database: dbname, sql: $"SELECT balance FROM accounts WHERE id = {id}", parameters: null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows.Select(r => r.Row["balance"].LongValue).ToList();
    }

    [Test]
    public async Task TheSecondLookupOfAKeySkipsTheIndexRead()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAccounts(this);
        TableDescriptor table = await database.TableDescriptors["accounts"];
        Assert.That(table.Store.RemembersUniqueRowIds, Is.True);

        Assert.That(await Balances(database, executor, dbname, 1), Is.EqualTo(new[] { 100L }));
        Assert.That(table.Store.RememberedUniqueRowIds, Is.EqualTo(1), "the first lookup resolved the key through the index and remembered it");

        long rowReadsBefore = table.Store.PrimaryRowPointReadCalls;
        Assert.That(await Balances(database, executor, dbname, 1), Is.EqualTo(new[] { 100L }));
        Assert.That(table.Store.PrimaryRowPointReadCalls - rowReadsBefore, Is.EqualTo(1), "the second lookup is one row read");
        Assert.That(table.Store.RememberedUniqueRowIds, Is.EqualTo(1));

        // A key with no row remembers nothing and keeps answering no row.
        Assert.That(await Balances(database, executor, dbname, 9), Is.Empty);
        Assert.That(table.Store.RememberedUniqueRowIds, Is.EqualTo(1));
    }

    [Test]
    public async Task AnUpdateKeepsTheRowIdAndTheRememberedMappingStaysRight()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAccounts(this);
        TableDescriptor table = await database.TableDescriptors["accounts"];

        Assert.That(await Balances(database, executor, dbname, 2), Is.EqualTo(new[] { 200L }));
        await NonQuery(database, executor, dbname, "UPDATE accounts SET balance = balance + 5 WHERE id = 2");
        Assert.That(await Balances(database, executor, dbname, 2), Is.EqualTo(new[] { 205L }));
        Assert.That(table.Store.RememberedUniqueRowIds, Is.EqualTo(1), "the update located its row through the same remembered id");
    }

    [Test]
    public async Task ADeletedKeyAnswersNoRowAndAReinsertedKeyAnswersTheNewRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAccounts(this);
        TableDescriptor table = await database.TableDescriptors["accounts"];

        Assert.That(await Balances(database, executor, dbname, 1), Is.EqualTo(new[] { 100L }));
        Assert.That(table.Store.RememberedUniqueRowIds, Is.EqualTo(1));

        await NonQuery(database, executor, dbname, "DELETE FROM accounts WHERE id = 1");
        Assert.That(await Balances(database, executor, dbname, 1), Is.Empty);
        Assert.That(table.Store.RememberedUniqueRowIds, Is.EqualTo(0), "the delete forgot the key");

        await NonQuery(database, executor, dbname, "INSERT INTO accounts (id, balance) VALUES (1, 111)");
        Assert.That(await Balances(database, executor, dbname, 1), Is.EqualTo(new[] { 111L }), "the re-inserted key resolves to the new row through the index");
        Assert.That(table.Store.RememberedUniqueRowIds, Is.EqualTo(1));
        Assert.That(await Balances(database, executor, dbname, 1), Is.EqualTo(new[] { 111L }), "and the new mapping is served from memory");
    }

    [Test]
    public async Task AStaleRememberedIdFallsBackToTheIndex()
    {
        // The hard case the cache exists to survive: another node (or an earlier store instance) deleted
        // and re-inserted the key, so the remembered id names a row that is gone while the key lives on
        // under a new id. Simulated by remembering a wrong id by hand.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAccounts(this);
        TableDescriptor table = await database.TableDescriptors["accounts"];

        Assert.That(await Balances(database, executor, dbname, 1), Is.EqualTo(new[] { 100L }));
        string kvKey = table.Store.RememberedUniqueRowIdKeysForTesting().Single();

        // Point the remembered mapping at a row id that does not exist.
        CamusDB.Core.Util.ObjectIds.ObjectIdValue bogus = new(0x7fff, 0x7fff, 0x7fff);
        table.Store.EvictCachedUniqueRowId(kvKey);
        table.Store.RememberUniqueRowIdForTesting(kvKey, bogus);

        long rowReadsBefore = table.Store.PrimaryRowPointReadCalls;
        Assert.That(await Balances(database, executor, dbname, 1), Is.EqualTo(new[] { 100L }), "the stale id is detected and the index answers");
        Assert.That(table.Store.PrimaryRowPointReadCalls - rowReadsBefore, Is.EqualTo(2), "the failed row read, then the row read after the index");

        Assert.That(table.Store.RememberedUniqueRowIdKeysForTesting(), Is.EqualTo(new[] { kvKey }));
        Assert.That(table.Store.TryGetCachedUniqueRowIdForTesting(kvKey, out CamusDB.Core.Util.ObjectIds.ObjectIdValue repaired), Is.True);
        Assert.That(repaired, Is.Not.EqualTo(bogus), "the mapping was repaired by the fallback");
    }
}
