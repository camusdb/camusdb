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
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A renamed column must read by its new name in rows written before the rename, also after the
/// database is closed and opened again. A row is decoded with the column layout of the version it
/// was written under, and the stored layouts keep the names of their own version, so a layout loaded
/// back from storage must take the current names. Before that, the first reopen after a rename made
/// every older row unreadable by the new name ("Unknown column").
///
/// <para>Cluster mode here; <see cref="TestRenameColumnAcrossReopenStandalone"/> runs every case in
/// standalone mode.</para>
/// </summary>
[NonParallelizable]
public class TestRenameColumnAcrossReopen : SetOperationsTestBase
{
    private static async Task<DatabaseDescriptor> Reopen(CommandExecutor executor, string dbname)
    {
        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        return await executor.OpenDatabase(dbname);
    }

    private static async Task<DatabaseDescriptor> Seed(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await ExecDdl(executor, database, dbname, "CREATE TABLE t (id int64 PRIMARY KEY, x int64 NULL, note string NULL)");
        await ExecNonQuery(executor, database, dbname, "INSERT INTO t (id, x, note) VALUES (1, 10, 'a'), (2, 20, 'b')");
        return database;
    }

    [Test]
    public async Task ARenamedColumn_ReadsByItsNewName_AfterAReopen()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        database = await Seed(executor, database, dbname);

        await ExecDdl(executor, database, dbname, "ALTER TABLE t RENAME COLUMN x TO xx");
        database = await Reopen(executor, dbname);

        CollectionAssert.AreEqual(new long?[] { 10, 20 },
            await Longs(executor, database, dbname, "SELECT xx FROM t ORDER BY id", "xx"));

        List<QueryResultRow> all = await ExecQuery(executor, database, dbname, "SELECT * FROM t ORDER BY id");
        CollectionAssert.AreEquivalent(new[] { "id", "xx", "note" }, all[0].Row.Keys);
    }

    [Test]
    public async Task ARenameOfATableLoadedFromStorage_IsReadable_WithAndWithoutAReopen()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        database = await Seed(executor, database, dbname);

        // After the reopen the table holds no layouts in memory, so the rename has none to relabel.
        database = await Reopen(executor, dbname);
        await ExecDdl(executor, database, dbname, "ALTER TABLE t RENAME COLUMN x TO xx");

        CollectionAssert.AreEqual(new long?[] { 10, 20 },
            await Longs(executor, database, dbname, "SELECT xx FROM t ORDER BY id", "xx"));

        database = await Reopen(executor, dbname);
        CollectionAssert.AreEqual(new long?[] { 10, 20 },
            await Longs(executor, database, dbname, "SELECT xx FROM t ORDER BY id", "xx"));
    }

    [Test]
    public async Task RowsOfEveryVersion_ReadByTheLatestName_AcrossSeveralRenames()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        database = await Seed(executor, database, dbname);

        await ExecDdl(executor, database, dbname, "ALTER TABLE t ADD COLUMN extra int64 NULL");
        await ExecNonQuery(executor, database, dbname, "INSERT INTO t (id, x, note, extra) VALUES (3, 30, 'c', 300)");

        await ExecDdl(executor, database, dbname, "ALTER TABLE t RENAME COLUMN x TO y");
        await ExecNonQuery(executor, database, dbname, "INSERT INTO t (id, y, note, extra) VALUES (4, 40, 'd', 400)");
        database = await Reopen(executor, dbname);

        await ExecDdl(executor, database, dbname, "ALTER TABLE t RENAME COLUMN y TO z");
        await ExecDdl(executor, database, dbname, "ALTER TABLE t RENAME COLUMN note TO label");
        database = await Reopen(executor, dbname);

        CollectionAssert.AreEqual(new long?[] { 10, 20, 30, 40 },
            await Longs(executor, database, dbname, "SELECT z FROM t ORDER BY id", "z"));

        List<QueryResultRow> labels = await ExecQuery(executor, database, dbname, "SELECT label FROM t ORDER BY id");
        CollectionAssert.AreEqual(new[] { "a", "b", "c", "d" }, labels.Select(r => r.Row["label"].StrValue));

        CollectionAssert.AreEqual(new long?[] { null, null, 300, 400 },
            await Longs(executor, database, dbname, "SELECT extra FROM t ORDER BY id", "extra"));
    }

    [Test]
    public async Task WritesAndIndexReads_UseTheNewName_AfterAReopen()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        database = await Seed(executor, database, dbname);
        await ExecDdl(executor, database, dbname, "CREATE INDEX t_x ON t (x)");

        await ExecDdl(executor, database, dbname, "ALTER TABLE t RENAME COLUMN x TO xx");
        database = await Reopen(executor, dbname);

        CollectionAssert.AreEqual(new long?[] { 2 },
            await Longs(executor, database, dbname, "SELECT id FROM t WHERE xx = 20", "id"));

        Assert.AreEqual(1, await ExecNonQuery(executor, database, dbname, "UPDATE t SET xx = xx + 1 WHERE id = 1"));
        Assert.AreEqual(1, await ExecNonQuery(executor, database, dbname, "DELETE FROM t WHERE xx = 20"));

        CollectionAssert.AreEqual(new long?[] { 11 },
            await Longs(executor, database, dbname, "SELECT xx FROM t ORDER BY id", "xx"));
    }

    [Test]
    public async Task AViewOverARenamedColumn_ReadsAfterAReopen()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedTwoTables(executor, database, dbname);

        await ExecDdl(executor, database, dbname, "CREATE VIEW merged AS SELECT x FROM a UNION SELECT y FROM b");
        await ExecDdl(executor, database, dbname, "ALTER TABLE a RENAME COLUMN x TO xx");
        await ExecDdl(executor, database, dbname, "ALTER TABLE b RENAME COLUMN y TO yy");
        database = await Reopen(executor, dbname);

        CollectionAssert.AreEqual(new long?[] { null, 1, 2, 3, 4 },
            await SortedLongs(executor, database, dbname, "SELECT x FROM merged", "x"));
    }
}

/// <summary>Every <see cref="TestRenameColumnAcrossReopen"/> case in standalone mode.</summary>
[NonParallelizable]
public sealed class TestRenameColumnAcrossReopenStandalone : TestRenameColumnAcrossReopen
{
    protected override CommandExecutor BuildCommandExecutor(CamusDBOptions options)
    {
        CommandValidator validator = new(options);
        CatalogsManager catalogsManager = new(logger);
        return new(validator, catalogsManager, logger, options,
            sharedNode: SharedNode, registry: sharedRegistry!, isClusterMode: false);
    }
}
