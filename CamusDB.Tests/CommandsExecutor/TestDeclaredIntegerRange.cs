/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// An integer type name promises a range, and the engine keeps the promise: <c>SMALLINT</c> is 16 bits,
/// <c>INT</c> and <c>INTEGER</c> are 32 bits, and <c>INT64</c> and <c>BIGINT</c> are 64 bits. A value
/// outside the range is <see cref="CamusDBErrorCodes.IntegerValueOutOfRange"/> on every write path and
/// on CAST, and the stored rows do not change. Before, all five names were int64 with no check, and
/// <c>BIGINT</c> and <c>DOUBLE PRECISION</c> were refused. Everything is driven through SQL tickets.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestDeclaredIntegerRange : BaseTest
{
    private static async Task Ddl(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, sql, null));
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<int> Dml(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
            await database.Transactions.CommitAsync(tx);
            return result.ModifiedRows;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<List<QueryResultRow>> Query(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await database.Transactions.CommitAsync(tx);
            return rows;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static void AssertOutOfRange(AsyncTestDelegate action, string typeName)
    {
        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(action)!;
        Assert.AreEqual(CamusDBErrorCodes.IntegerValueOutOfRange, exception.Code, exception.Message);
        StringAssert.Contains(typeName, exception.Message);
    }

    private static async Task<long> StoredAsync(CommandExecutor executor, DatabaseDescriptor database, string db, long id, string column = "v") =>
        (await Query(executor, database, db, $"SELECT {column} FROM t WHERE id = {id}")).Single().Row[column].LongValue;

    private static async Task<string> ShowCreateAsync(CommandExecutor executor, DatabaseDescriptor database, string db, string table) =>
        (await Query(executor, database, db, $"SHOW CREATE TABLE {table}")).Single().Row["Create Table"].StrValue!;

    [TestCase("SMALLINT", "smallint", -32768L, 32767L)]
    [TestCase("smallint", "smallint", -32768L, 32767L)]
    [TestCase("INT", "int", -2147483648L, 2147483647L)]
    [TestCase("int", "int", -2147483648L, 2147483647L)]
    [TestCase("INTEGER", "int", -2147483648L, 2147483647L)]
    public async Task Insert_TakesEachBoundAndRefusesThePastIt(string typeName, string reportedName, long min, long max)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, $"CREATE TABLE t (id int64 PRIMARY KEY, v {typeName})");

        Assert.AreEqual(2, await Dml(executor, database, db, $"INSERT INTO t (id, v) VALUES (1, {min}), (2, {max})"));
        Assert.AreEqual(min, await StoredAsync(executor, database, db, 1));
        Assert.AreEqual(max, await StoredAsync(executor, database, db, 2));

        AssertOutOfRange(() => Dml(executor, database, db, $"INSERT INTO t (id, v) VALUES (3, {max + 1})"), reportedName);
        AssertOutOfRange(() => Dml(executor, database, db, $"INSERT INTO t (id, v) VALUES (3, {min - 1})"), reportedName);

        Assert.AreEqual(2, (await Query(executor, database, db, "SELECT id FROM t")).Count);
    }

    [TestCase("INT64")]
    [TestCase("BIGINT")]
    [TestCase("bigint")]
    public async Task Insert_A64BitTypeTakesEveryInt64(string typeName)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, $"CREATE TABLE t (id int64 PRIMARY KEY, v {typeName})");

        await Dml(executor, database, db, $"INSERT INTO t (id, v) VALUES (1, {long.MaxValue}), (2, {long.MinValue}), (3, 2147483648)");

        Assert.AreEqual(long.MaxValue, await StoredAsync(executor, database, db, 1));
        Assert.AreEqual(long.MinValue, await StoredAsync(executor, database, db, 2));
        Assert.AreEqual(2147483648L, await StoredAsync(executor, database, db, 3));
    }

    [Test]
    public async Task Insert_NullAndAnOmittedColumnFit()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE t (id int64 PRIMARY KEY, v smallint)");

        await Dml(executor, database, db, "INSERT INTO t (id, v) VALUES (1, NULL)");
        await Dml(executor, database, db, "INSERT INTO t (id) VALUES (2)");

        Assert.AreEqual(2, (await Query(executor, database, db, "SELECT id FROM t WHERE v IS NULL")).Count);
    }

    [Test]
    public async Task InsertSelect_RefusesAWideSourceValue()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE src (id int64 PRIMARY KEY, w int64)");
        await Ddl(executor, database, db, "CREATE TABLE t (id int64 PRIMARY KEY, v smallint)");
        await Dml(executor, database, db, "INSERT INTO src (id, w) VALUES (1, 5), (2, 40000)");

        AssertOutOfRange(() => Dml(executor, database, db, "INSERT INTO t (id, v) SELECT id, w FROM src"), "smallint");

        Assert.AreEqual(0, (await Query(executor, database, db, "SELECT id FROM t")).Count);
    }

    [Test]
    public async Task Update_RefusesALiteralAndAnExpressionPastTheBound()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE t (id int64 PRIMARY KEY, v smallint, other int64)");
        await Dml(executor, database, db, "INSERT INTO t (id, v, other) VALUES (1, 32767, 0), (2, -32768, 0)");

        AssertOutOfRange(() => Dml(executor, database, db, "UPDATE t SET v = 32768 WHERE id = 1"), "smallint");
        AssertOutOfRange(() => Dml(executor, database, db, "UPDATE t SET v = v + 1 WHERE id = 1"), "smallint");
        AssertOutOfRange(() => Dml(executor, database, db, "UPDATE t SET v = v - 1 WHERE id = 2"), "smallint");
        Assert.AreEqual(32767L, await StoredAsync(executor, database, db, 1));
        Assert.AreEqual(-32768L, await StoredAsync(executor, database, db, 2));

        // A bound holds a value, so an update of another column of the row goes through.
        Assert.AreEqual(1, await Dml(executor, database, db, "UPDATE t SET other = 9 WHERE id = 1"));
        Assert.AreEqual(1, await Dml(executor, database, db, "UPDATE t SET v = v - 1 WHERE id = 1"));
        Assert.AreEqual(32766L, await StoredAsync(executor, database, db, 1));
    }

    [Test]
    public async Task Upsert_RefusesTheInsertArmAndTheUpdateArm()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE t (id int64 PRIMARY KEY, v int)");
        await Dml(executor, database, db, "INSERT INTO t (id, v) VALUES (1, 2147483647)");

        AssertOutOfRange(
            () => Dml(executor, database, db, "INSERT INTO t (id, v) VALUES (2, 2147483648) ON CONFLICT (id) DO NOTHING"), "int");
        AssertOutOfRange(
            () => Dml(executor, database, db, "INSERT INTO t (id, v) VALUES (1, 0) ON CONFLICT (id) DO UPDATE SET v = t.v + 1"), "int");
        AssertOutOfRange(
            () => Dml(executor, database, db, "INSERT INTO t (id, v) VALUES (1, 2147483648) ON CONFLICT (id) DO NOTHING"), "int");

        Assert.AreEqual(2147483647L, await StoredAsync(executor, database, db, 1));
        Assert.AreEqual(1, (await Query(executor, database, db, "SELECT id FROM t")).Count);
    }

    /// <summary>The typed row API takes the same path as SQL, so it takes the same rule.</summary>
    [Test]
    public async Task TypedRowTicket_IsRefused()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE t (id int64 PRIMARY KEY, v smallint)");

        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            AssertOutOfRange(() => executor.Insert(new InsertTicket(tx, db, "t",
            [
                new Dictionary<string, ColumnValue>
                {
                    ["id"] = new(ColumnType.Integer64, 1L),
                    ["v"] = new(ColumnType.Integer64, 32768L),
                },
            ])), "smallint");
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    [TestCase("CREATE TABLE t (id int64 PRIMARY KEY, v smallint DEFAULT(32768))", "smallint")]
    [TestCase("CREATE TABLE t (id int64 PRIMARY KEY, v smallint DEFAULT(-32769))", "smallint")]
    [TestCase("CREATE TABLE t (id int64 PRIMARY KEY, v int DEFAULT(2147483648))", "int")]
    public async Task Default_PastTheBoundIsRefusedByTheDdl(string ddl, string typeName)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        AssertOutOfRange(() => Ddl(executor, database, db, ddl), typeName);
    }

    [Test]
    public async Task Default_AtTheBoundIsStored()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE t (id int64 PRIMARY KEY, v smallint DEFAULT(32767))");
        await Dml(executor, database, db, "INSERT INTO t (id) VALUES (1)");

        Assert.AreEqual(32767L, await StoredAsync(executor, database, db, 1));
    }

    [Test]
    public async Task SequenceDefault_IsRefusedWhenItPassesTheBound()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE SEQUENCE seq START WITH 32767");
        await Ddl(executor, database, db, "CREATE TABLE t (id int64 PRIMARY KEY, v smallint DEFAULT(nextval('seq')))");

        await Dml(executor, database, db, "INSERT INTO t (id) VALUES (1)");
        Assert.AreEqual(32767L, await StoredAsync(executor, database, db, 1));

        AssertOutOfRange(() => Dml(executor, database, db, "INSERT INTO t (id) VALUES (2)"), "smallint");
        Assert.AreEqual(1, (await Query(executor, database, db, "SELECT id FROM t")).Count);
    }

    [Test]
    public async Task AddColumn_EnforcesTheRange()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE t (id int64 PRIMARY KEY)");
        await Dml(executor, database, db, "INSERT INTO t (id) VALUES (1)");
        await Ddl(executor, database, db, "ALTER TABLE t ADD COLUMN v smallint DEFAULT(7)");

        Assert.AreEqual(7L, await StoredAsync(executor, database, db, 1));

        AssertOutOfRange(() => Dml(executor, database, db, "UPDATE t SET v = 32768 WHERE id = 1"), "smallint");
        AssertOutOfRange(() => Ddl(executor, database, db, "ALTER TABLE t ADD COLUMN w int DEFAULT(2147483648)"), "int");
    }

    /// <summary>
    /// The fill of an added column gives each existing row a value that no INSERT judges, so a sequence
    /// that passes the width must stop the fill and remove the column.
    /// </summary>
    [Test]
    public async Task AddColumn_FillPastTheBoundFailsAndRemovesTheColumn()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE SEQUENCE seq START WITH 32767");
        await Ddl(executor, database, db, "CREATE TABLE t (id int64 PRIMARY KEY)");
        await Dml(executor, database, db, "INSERT INTO t (id) VALUES (1), (2), (3)");

        AssertOutOfRange(() => Ddl(executor, database, db, "ALTER TABLE t ADD COLUMN v smallint DEFAULT(nextval('seq'))"), "smallint");

        Assert.IsFalse(
            (await Query(executor, database, db, "SHOW COLUMNS FROM t")).Any(r => r.Row["Field"].StrValue == "v"),
            "a fill that fails removes the column");
        Assert.AreEqual(3, (await Query(executor, database, db, "SELECT id FROM t")).Count);
    }

    [TestCase("SMALLINT", "32767", 32767L)]
    [TestCase("SMALLINT", "-32768", -32768L)]
    [TestCase("INT", "2147483647", 2147483647L)]
    [TestCase("INTEGER", "-2147483648", -2147483648L)]
    [TestCase("BIGINT", "9223372036854775807", long.MaxValue)]
    [TestCase("SMALLINT", "'32767'", 32767L)]
    [TestCase("SMALLINT", "32766.5", 32767L)]
    public async Task Cast_TakesAValueThatFits(string typeName, string input, long expected)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        foreach (string sql in new[] { $"SELECT CAST({input} AS {typeName}) AS x", $"SELECT ({input})::{typeName} AS x" })
        {
            ColumnValue value = (await Query(executor, database, db, sql)).Single().Row["x"];
            Assert.AreEqual(ColumnType.Integer64, value.Type, sql);
            Assert.AreEqual(expected, value.LongValue, sql);
        }
    }

    [TestCase("SMALLINT", "32768", "smallint")]
    [TestCase("SMALLINT", "-32769", "smallint")]
    [TestCase("SMALLINT", "32767.5", "smallint")]
    [TestCase("SMALLINT", "'70000'", "smallint")]
    [TestCase("INT", "2147483648", "int")]
    [TestCase("INTEGER", "-2147483649", "int")]
    public async Task Cast_RefusesAValueThatDoesNotFit(string typeName, string input, string reportedName)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        AssertOutOfRange(() => Query(executor, database, db, $"SELECT CAST({input} AS {typeName}) AS x"), reportedName);
        AssertOutOfRange(() => Query(executor, database, db, $"SELECT ({input})::{typeName} AS x"), reportedName);
    }

    [Test]
    public async Task Cast_OfNullIsNull()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        Assert.AreEqual(ColumnType.Null, (await Query(executor, database, db, "SELECT CAST(NULL AS smallint) AS x")).Single().Row["x"].Type);
    }

    /// <summary>
    /// A view body is rendered to text and parsed again, so the renderer must keep the narrow cast.
    /// Rendered as int64, the view would return a value the query that defines it refuses.
    /// </summary>
    [Test]
    public async Task Cast_InAViewKeepsItsWidth()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE t (id int64 PRIMARY KEY, w int64)");
        await Dml(executor, database, db, "INSERT INTO t (id, w) VALUES (1, 40000)");
        await Ddl(executor, database, db, "CREATE VIEW narrow AS SELECT CAST(w AS smallint) AS s FROM t");
        await Ddl(executor, database, db, "CREATE VIEW wide AS SELECT CAST(w AS int) AS s FROM t");

        AssertOutOfRange(() => Query(executor, database, db, "SELECT s FROM narrow"), "smallint");
        Assert.AreEqual(40000L, (await Query(executor, database, db, "SELECT s FROM wide")).Single().Row["s"].LongValue);
    }

    /// <summary>A CHECK condition is also rendered to text and parsed again, on a reopen.</summary>
    [Test]
    public async Task Cast_InACheckKeepsItsWidthAfterAReopen()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE t (id int64 PRIMARY KEY, w int64, CONSTRAINT w_fits CHECK (CAST(w AS smallint) >= 0))");

        await Dml(executor, database, db, "INSERT INTO t (id, w) VALUES (1, 5)");
        AssertOutOfRange(() => Dml(executor, database, db, "INSERT INTO t (id, w) VALUES (2, 40000)"), "smallint");

        await executor.CloseDatabase(new CloseDatabaseTicket(db));
        DatabaseDescriptor reopened = await executor.OpenDatabase(db);

        AssertOutOfRange(() => Dml(executor, reopened, db, "INSERT INTO t (id, w) VALUES (2, 40000)"), "smallint");
    }

    /// <summary>Arithmetic stays int64: only a write or a cast applies the width.</summary>
    [Test]
    public async Task Arithmetic_OnANarrowColumnIsInt64()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE t (id int64 PRIMARY KEY, v smallint)");
        await Dml(executor, database, db, "INSERT INTO t (id, v) VALUES (1, 32767)");

        ColumnValue value = (await Query(executor, database, db, "SELECT v + 1 AS x FROM t")).Single().Row["x"];

        Assert.AreEqual(ColumnType.Integer64, value.Type);
        Assert.AreEqual(32768L, value.LongValue);
    }

    /// <summary>A narrow column reads and seeks as an int64, through an index too.</summary>
    [Test]
    public async Task Index_OnANarrowColumnIsUsable()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE t (id int PRIMARY KEY, v smallint, UNIQUE KEY t_v (v))");
        await Dml(executor, database, db, "INSERT INTO t (id, v) VALUES (1, 32767), (2, -32768)");

        Assert.AreEqual(1L, (await Query(executor, database, db, "SELECT id FROM t WHERE v = 32767")).Single().Row["id"].LongValue);
        Assert.AreEqual(0, (await Query(executor, database, db, "SELECT id FROM t WHERE v = 40000")).Count);
        AssertOutOfRange(() => Dml(executor, database, db, "INSERT INTO t (id, v) VALUES (2147483648, 1)"), "int");
    }

    [Test]
    public async Task Array_AppliesTheWidthToEachElement()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE t (id int64 PRIMARY KEY, s array(smallint), i array(integer), b array(bigint))");

        await Dml(executor, database, db,
            $"INSERT INTO t (id, s, i, b) VALUES (1, ARRAY[-32768, 32767, NULL], ARRAY[2147483647], ARRAY[{long.MaxValue}])");
        await Dml(executor, database, db, "INSERT INTO t (id, s) VALUES (2, ARRAY[])");

        AssertOutOfRange(() => Dml(executor, database, db, "INSERT INTO t (id, s) VALUES (3, ARRAY[1, 32768])"), "smallint");
        AssertOutOfRange(() => Dml(executor, database, db, "INSERT INTO t (id, i) VALUES (3, ARRAY[2147483648])"), "int");
        AssertOutOfRange(() => Dml(executor, database, db, "UPDATE t SET s = ARRAY[40000] WHERE id = 1"), "smallint");

        Assert.AreEqual(2, (await Query(executor, database, db, "SELECT id FROM t")).Count);

        string create = await ShowCreateAsync(executor, database, db, "t");
        StringAssert.Contains("ARRAY(SMALLINT)", create);
        StringAssert.Contains("ARRAY(INT)", create);
        StringAssert.Contains("ARRAY(INT64)", create);
    }

    [Test]
    public async Task Array_OfAWordThatIsNotAnElementTypeIsRefused()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(
            () => Ddl(executor, database, db, "CREATE TABLE t (id int64 PRIMARY KEY, j array(jsonb))"))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, exception.Code, exception.Message);
    }

    /// <summary>
    /// The DDL that <c>SHOW CREATE TABLE</c> gives is what a logical dump restores, so it must re-create
    /// the same range.
    /// </summary>
    [Test]
    public async Task ShowCreateTable_RendersTheWidthAndRecreatesTheRange()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE t (id bigint PRIMARY KEY, s smallint, i int, g integer, b bigint, l int64)");

        string create = await ShowCreateAsync(executor, database, db, "t");

        await Ddl(executor, database, db, create.Replace("TABLE `t`", "TABLE `copy`").Replace("TABLE t", "TABLE copy"));

        AssertOutOfRange(() => Dml(executor, database, db, "INSERT INTO copy (id, s) VALUES (1, 32768)"), "smallint");
        AssertOutOfRange(() => Dml(executor, database, db, "INSERT INTO copy (id, g) VALUES (1, 2147483648)"), "int");
        Assert.AreEqual(1, await Dml(executor, database, db, "INSERT INTO copy (id, b) VALUES (1, 2147483648)"));

        Dictionary<string, string> types = (await Query(executor, database, db, "SHOW COLUMNS FROM t"))
            .ToDictionary(r => r.Row["Field"].StrValue!, r => r.Row["Type"].StrValue!);
        Assert.AreEqual("SMALLINT", types["s"]);
        Assert.AreEqual("INT", types["i"]);
        Assert.AreEqual("INT", types["g"]);
        Assert.AreEqual("INT64", types["b"]);
        Assert.AreEqual("INT64", types["l"]);
    }

    /// <summary>
    /// Each of these statements rebuilds the column from its properties. A rebuild that forgets the
    /// width would keep the type and drop the check, with no error, so each one is tried by a write.
    /// </summary>
    [TestCase("ALTER TABLE t RENAME COLUMN v TO v2", "v2")]
    [TestCase("COMMENT ON COLUMN t.v IS 'a comment'", "v")]
    [TestCase("ALTER TABLE t ALTER COLUMN v SET NOT NULL", "v")]
    [TestCase("ALTER TABLE t ADD COLUMN w string", "v")]
    [TestCase("ALTER TABLE t DROP COLUMN gone", "v")]
    [TestCase("ALTER TABLE t ADD INDEX t_v (v)", "v")]
    public async Task SchemaChange_KeepsTheWidth(string change, string columnAfter)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE t (id int64 PRIMARY KEY, v smallint, gone int64)");
        await Dml(executor, database, db, "INSERT INTO t (id, v) VALUES (1, 1)");

        await Ddl(executor, database, db, change);

        AssertOutOfRange(() => Dml(executor, database, db, $"UPDATE t SET {columnAfter} = 32768 WHERE id = 1"), "smallint");
        StringAssert.Contains("SMALLINT", await ShowCreateAsync(executor, database, db, "t"));
    }

    /// <summary>A key column declared at table level is rebuilt NOT NULL before the table is created.</summary>
    [Test]
    public async Task CompositePrimaryKey_KeepsTheWidth()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE t (a smallint, b int, PRIMARY KEY (a, b))");

        await Dml(executor, database, db, "INSERT INTO t (a, b) VALUES (32767, 2147483647)");

        AssertOutOfRange(() => Dml(executor, database, db, "INSERT INTO t (a, b) VALUES (32768, 1)"), "smallint");
        AssertOutOfRange(() => Dml(executor, database, db, "INSERT INTO t (a, b) VALUES (1, 2147483648)"), "int");
    }

    /// <summary>SET STORAGE applies to an array column, and it rebuilds the column.</summary>
    [Test]
    public async Task SetStorage_KeepsTheWidthOfAnArray()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE t (id int64 PRIMARY KEY, a array(smallint))");
        await Ddl(executor, database, db, "ALTER TABLE t ALTER COLUMN a SET STORAGE PLAIN");

        AssertOutOfRange(() => Dml(executor, database, db, "INSERT INTO t (id, a) VALUES (1, ARRAY[32768])"), "smallint");
    }

    /// <summary>A relink rebuilds each column from the schema of the dropped table.</summary>
    [Test]
    public async Task Relink_KeepsTheWidth()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE t (id int64 PRIMARY KEY, v smallint)");
        string tableId = database.Schema.Tables["t"].Id!;

        Assert.IsTrue(await executor.DropTable(new DropTableTicket(db, "t", ifExists: false)));
        await Ddl(executor, database, db, $"CREATE TABLE back RELINK TO \"{tableId}\"");

        AssertOutOfRange(() => Dml(executor, database, db, "INSERT INTO back (id, v) VALUES (1, 32768)"), "smallint");
    }

    [Test]
    public async Task RenameTable_KeepsTheWidth()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE t (id int64 PRIMARY KEY, v smallint)");
        await Ddl(executor, database, db, "ALTER TABLE t RENAME TO renamed");

        AssertOutOfRange(() => Dml(executor, database, db, "INSERT INTO renamed (id, v) VALUES (1, 32768)"), "smallint");
    }

    /// <summary>A reopen reads the schema from storage, so this is the persisted form under test.</summary>
    [Test]
    public async Task Reopen_KeepsTheWidth()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE t (id int64 PRIMARY KEY, s smallint, i int, a array(smallint), b bigint)");
        await Ddl(executor, database, db, "ALTER TABLE t ADD COLUMN added int");
        await Dml(executor, database, db, $"INSERT INTO t (id, s, i, b) VALUES (1, 32767, 2147483647, {long.MaxValue})");

        await executor.CloseDatabase(new CloseDatabaseTicket(db));
        DatabaseDescriptor reopened = await executor.OpenDatabase(db);

        Assert.AreEqual(32767L, await StoredAsync(executor, reopened, db, 1, "s"));
        AssertOutOfRange(() => Dml(executor, reopened, db, "UPDATE t SET s = 32768 WHERE id = 1"), "smallint");
        AssertOutOfRange(() => Dml(executor, reopened, db, "UPDATE t SET i = 2147483648 WHERE id = 1"), "int");
        AssertOutOfRange(() => Dml(executor, reopened, db, "UPDATE t SET added = 2147483648 WHERE id = 1"), "int");
        AssertOutOfRange(() => Dml(executor, reopened, db, "UPDATE t SET a = ARRAY[32768] WHERE id = 1"), "smallint");
        Assert.AreEqual(1, await Dml(executor, reopened, db, "UPDATE t SET b = 2147483648 WHERE id = 1"));
    }

    /// <summary>
    /// A column persisted before the width existed has no such field. It loads with no limit, because
    /// it was stored with no limit and can hold any int64.
    /// </summary>
    [Test]
    public void PersistedColumnWithoutAWidth_LoadsWithNoLimit()
    {
        const string json = """{"Id":"c1","Name":"v","Type":2,"NotNull":false,"DefaultValue":null,"State":0}""";

        TableColumnSchema column = MetaJsonSerializer.DeserializeCompat(Encoding.UTF8.GetBytes(json), MetaJsonContext.Default.TableColumnSchema);

        Assert.IsNull(column.IntegerBits);
    }

    [Test]
    public void WidthRoundTripsThroughThePersistedForm()
    {
        TableColumnSchema written = new("c1", "v", ColumnType.Integer64, notNull: false, defaultValue: null, integerBits: 16);

        byte[] bytes = MetaJsonSerializer.Serialize(written, MetaJsonContext.Default.TableColumnSchema);
        TableColumnSchema read = MetaJsonSerializer.DeserializeCompat(bytes, MetaJsonContext.Default.TableColumnSchema);

        Assert.AreEqual(16, read.IntegerBits);
    }

    [TestCase(8)]
    [TestCase(64)]
    [TestCase(0)]
    public async Task Ticket_WithABadWidthIsRefused(int bits)
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(() => executor.CreateTable(new CreateTableTicket(
            db, "t",
            [
                new ColumnInfo("id", ColumnType.Integer64, notNull: true),
                new ColumnInfo("v", ColumnType.Integer64, integerBits: bits),
            ],
            [new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])],
            ifNotExists: false)))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, exception.Code, exception.Message);
    }

    [Test]
    public async Task Ticket_WithAWidthOnAStringIsRefused()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(() => executor.CreateTable(new CreateTableTicket(
            db, "t",
            [
                new ColumnInfo("id", ColumnType.Integer64, notNull: true),
                new ColumnInfo("v", ColumnType.String, integerBits: 16),
            ],
            [new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])],
            ifNotExists: false)))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, exception.Code, exception.Message);
    }

    /// <summary>The new spellings must not take a word away from a table or column name.</summary>
    [Test]
    public async Task Bigint_AndPrecisionStayUsableAsNames()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE bigint (id bigint PRIMARY KEY, bigint bigint, precision double precision)");
        await Ddl(executor, database, db, "CREATE TABLE precision (id int64 PRIMARY KEY)");
        await Dml(executor, database, db, "INSERT INTO bigint (id, bigint, precision) VALUES (1, 2147483648, 1.5)");

        QueryResultRow row = (await Query(executor, database, db, "SELECT bigint, precision FROM bigint")).Single();

        Assert.AreEqual(2147483648L, row.Row["bigint"].LongValue);
        Assert.AreEqual(1.5, row.Row["precision"].FloatValue);
    }

    [Test]
    public async Task DoublePrecision_IsFloat64Everywhere()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE t (id int64 PRIMARY KEY, d DOUBLE PRECISION, a array(double   precision))");
        await Ddl(executor, database, db, "ALTER TABLE t ADD COLUMN e double\nprecision");
        await Ddl(executor, database, db, "ALTER TABLE t ADD COLUMN b BIGINT");

        Dictionary<string, string> types = (await Query(executor, database, db, "SHOW COLUMNS FROM t"))
            .ToDictionary(r => r.Row["Field"].StrValue!, r => r.Row["Type"].StrValue!);
        Assert.AreEqual("FLOAT64", types["d"]);
        Assert.AreEqual("ARRAY(FLOAT64)", types["a"]);
        Assert.AreEqual("FLOAT64", types["e"]);
        Assert.AreEqual("INT64", types["b"]);

        foreach (string sql in new[] { "SELECT CAST(1 AS DOUBLE PRECISION) AS x", "SELECT (1)::double precision AS x", "SELECT CAST(1 AS DOUBLE) AS x" })
            Assert.AreEqual(ColumnType.Float64, (await Query(executor, database, db, sql)).Single().Row["x"].Type, sql);
    }

    [Test]
    public async Task Double_FollowedByAnotherWordIsNotAType()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        Assert.ThrowsAsync<CamusDBException>(() => Ddl(executor, database, db, "CREATE TABLE t (id int64 PRIMARY KEY, d double trouble)"));
        Assert.ThrowsAsync<CamusDBException>(() => Ddl(executor, database, db, "CREATE TABLE t (id int64 PRIMARY KEY, d double)"));
    }

    [Test]
    public async Task Real_PastTheFloat32RangeIsAnErrorAndNeverInfinity()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE t (id int64 PRIMARY KEY, r real)");

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(() => Dml(executor, database, db, "INSERT INTO t (id, r) VALUES (1, 1e300)"))!;
        Assert.AreEqual(CamusDBErrorCodes.FloatValueOutOfRange, exception.Code, exception.Message);

        exception = Assert.ThrowsAsync<CamusDBException>(() => Dml(executor, database, db, "INSERT INTO t (id, r) VALUES (1, 1e-300)"))!;
        Assert.AreEqual(CamusDBErrorCodes.FloatValueOutOfRange, exception.Code, exception.Message);

        Assert.AreEqual(0, (await Query(executor, database, db, "SELECT id FROM t")).Count);
    }

    /// <summary>
    /// A value inside the range still narrows: <c>REAL</c> and <c>DOUBLE PRECISION</c> are different
    /// types with different precision. 2^24 + 1 is the first integer a Float32 cannot hold, and 0.1 has
    /// no finite binary form, so each rounds differently at each width. A range test alone would pass
    /// if both columns were the same type.
    /// </summary>
    [Test]
    public async Task Real_AndDoublePrecisionHoldDifferentValues()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE t (id int64 PRIMARY KEY, r real, d double precision)");
        await Dml(executor, database, db, "INSERT INTO t (id, r, d) VALUES (1, 16777217, 16777217), (2, 0.1, 0.1)");

        async Task AssertValues(DatabaseDescriptor current)
        {
            List<QueryResultRow> rows = await Query(executor, current, db, "SELECT r, d FROM t ORDER BY id");

            Assert.AreEqual(ColumnType.Float32, rows[0].Row["r"].Type);
            Assert.AreEqual(ColumnType.Float64, rows[0].Row["d"].Type);

            Assert.AreEqual(16777216.0, rows[0].Row["r"].FloatValue);
            Assert.AreEqual(16777217.0, rows[0].Row["d"].FloatValue);
            Assert.AreEqual((double)0.1f, rows[1].Row["r"].FloatValue);
            Assert.AreEqual(0.1, rows[1].Row["d"].FloatValue);
        }

        await AssertValues(database);

        await executor.CloseDatabase(new CloseDatabaseTicket(db));
        DatabaseDescriptor reopened = await executor.OpenDatabase(db);

        await AssertValues(reopened);

        Dictionary<string, string> types = (await Query(executor, reopened, db, "SHOW COLUMNS FROM t"))
            .ToDictionary(r => r.Row["Field"].StrValue!, r => r.Row["Type"].StrValue!);
        Assert.AreEqual("FLOAT32", types["r"]);
        Assert.AreEqual("FLOAT64", types["d"]);
    }

    [Test]
    public void IntegerWidth_TakesOnlyThePostgreSqlRanges()
    {
        Assert.IsTrue(IntegerWidth.IsValid(IntegerWidth.SmallIntBits));
        Assert.IsTrue(IntegerWidth.IsValid(IntegerWidth.IntBits));
        Assert.IsFalse(IntegerWidth.IsValid(64));

        Assert.DoesNotThrow(() => IntegerWidth.EnforceOnColumn(16, ColumnValue.Null, "v"));
        Assert.DoesNotThrow(() => IntegerWidth.EnforceOnColumn(16, new ColumnValue(ColumnType.String, "32768"), "v"));
        Assert.Throws<CamusDBException>(() => IntegerWidth.EnforceOnColumn(16, new ColumnValue(ColumnType.Integer64, long.MinValue), "v"));
        Assert.Throws<CamusDBException>(() => IntegerWidth.EnforceOnColumn(32, new ColumnValue(ColumnType.Integer64, long.MaxValue), "v"));
    }
}
