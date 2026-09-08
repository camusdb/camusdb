/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

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
/// A primary key is stored as a unique index, and a unique index holds no entry for a row with a
/// NULL key column, so every primary-key column must be NOT NULL whether or not the user wrote it.
/// These tests pin the three consequences on a standalone engine: the schema records NOT NULL, an
/// insert with a NULL key is rejected, and the planner still takes the prefix range scan a
/// composite key offers. The cluster twin lives in <see cref="TestPrimaryKeyNotNullCluster"/>.
/// </summary>
[NonParallelizable]
public class TestPrimaryKeyNotNull : BaseTest
{
    internal static async Task AssertPrimaryKeyColumnsAreNotNull(
        CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await IndexDifferentialProbe.ExecDDL(executor, database, dbname,
            "CREATE TABLE keyed (a INT, b INT, note STRING, PRIMARY KEY (a, b))");

        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        TableDescriptor keyed = await db.TableDescriptors["keyed"];

        Assert.IsTrue(keyed.Schema.Columns!.Single(c => c.Name == "a").NotNull, "PK column a must be NOT NULL");
        Assert.IsTrue(keyed.Schema.Columns!.Single(c => c.Name == "b").NotNull, "PK column b must be NOT NULL");
        Assert.IsFalse(keyed.Schema.Columns!.Single(c => c.Name == "note").NotNull, "a non-key column keeps its nullability");
    }

    internal static async Task AssertNullKeyInsertIsRejected(
        CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await IndexDifferentialProbe.ExecDDL(executor, database, dbname,
            "CREATE TABLE keyed (a INT, b INT, note STRING, PRIMARY KEY (a, b))");
        await IndexDifferentialProbe.ExecDML(executor, database, dbname,
            "INSERT INTO keyed (a, b, note) VALUES (1, 2, 'ok')");

        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            ExecuteSQLTicket ticket = new(txnState: tx, database: dbname,
                sql: "INSERT INTO keyed (a, b, note) VALUES (1, NULL, 'no key')", parameters: null);
            CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () => await executor.ExecuteNonSQLQuery(ticket))!;
            Assert.AreEqual(CamusDBErrorCodes.NotNullViolation, ex.Code);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }

        // An omitted key column is a NULL key column too.
        KvTransaction tx2 = await database.Transactions.BeginAsync();
        try
        {
            ExecuteSQLTicket ticket = new(txnState: tx2, database: dbname,
                sql: "INSERT INTO keyed (a, note) VALUES (3, 'no b')", parameters: null);
            CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () => await executor.ExecuteNonSQLQuery(ticket))!;
            Assert.AreEqual(CamusDBErrorCodes.NotNullViolation, ex.Code);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx2);
        }
    }

    [Test]
    public async Task CreateTable_PrimaryKeyColumns_AreNotNullWithoutSayingSo()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await AssertPrimaryKeyColumnsAreNotNull(executor, database, dbname);
    }

    [Test]
    public async Task Insert_NullInPrimaryKeyColumn_IsRejected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await AssertNullKeyInsertIsRejected(executor, database, dbname);
    }

    [Test]
    public async Task CompositePrimaryKey_PrefixScan_IsChosenWithoutExplicitNotNull()
    {
        // The completeness rule declines a prefix scan over a unique index with a nullable trailing
        // column. Because the key's columns are NOT NULL by rule, the prefix scan stays available.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await IndexDifferentialProbe.ExecDDL(executor, database, dbname,
            "CREATE TABLE keyed (a INT, b INT, note STRING, PRIMARY KEY (a, b))");
        await IndexDifferentialProbe.ExecDML(executor, database, dbname,
            "INSERT INTO keyed (a, b, note) VALUES (1, 2, 'x')");
        await IndexDifferentialProbe.ExecDML(executor, database, dbname,
            "INSERT INTO keyed (a, b, note) VALUES (2, 5, 'y')");

        const string query = "SELECT b FROM keyed WHERE a = 1";
        Assert.AreEqual("b=2", await IndexDifferentialProbe.QueryRendered(executor, database, dbname, query));

        string[] nodes = await IndexDifferentialProbe.ExplainNodes(executor, database, dbname, query);
        Assert.IsTrue(nodes.Any(n => n.StartsWith("index-range-scan", System.StringComparison.Ordinal)),
            $"Expected the primary-key prefix range scan for `{query}`; got: {string.Join(", ", nodes)}");
    }

    [Test]
    public async Task AlterTable_DropNotNullOnPrimaryKeyColumn_IsRejected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await IndexDifferentialProbe.ExecDDL(executor, database, dbname,
            "CREATE TABLE keyed (a INT, b INT, note STRING NOT NULL, PRIMARY KEY (a, b))");

        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            ExecuteSQLTicket ticket = new(txnState: tx, database: dbname,
                sql: "ALTER TABLE keyed ALTER COLUMN b DROP NOT NULL", parameters: null);
            CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () => await executor.ExecuteDDLSQL(ticket))!;
            Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
            StringAssert.Contains("primary key", ex.Message);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }

        // A non-key column can still drop its constraint.
        await IndexDifferentialProbe.ExecDDL(executor, database, dbname, "ALTER TABLE keyed ALTER COLUMN note DROP NOT NULL");

        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        TableDescriptor keyed = await db.TableDescriptors["keyed"];
        Assert.IsTrue(keyed.Schema.Columns!.Single(c => c.Name == "b").NotNull);
        Assert.IsFalse(keyed.Schema.Columns!.Single(c => c.Name == "note").NotNull);
    }
}

/// <summary>
/// Cluster twin of <see cref="TestPrimaryKeyNotNull"/>: table creation is replicated through the
/// schema log and the rule must reach the persisted schema on that path too.
/// </summary>
[NonParallelizable]
public class TestPrimaryKeyNotNullCluster : SharedNodeBaseTest
{
    [Test]
    public async Task CreateTable_PrimaryKeyColumns_AreNotNullWithoutSayingSo()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await TestPrimaryKeyNotNull.AssertPrimaryKeyColumnsAreNotNull(executor, database, dbname);
    }

    [Test]
    public async Task Insert_NullInPrimaryKeyColumn_IsRejected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await TestPrimaryKeyNotNull.AssertNullKeyInsertIsRejected(executor, database, dbname);
    }
}
