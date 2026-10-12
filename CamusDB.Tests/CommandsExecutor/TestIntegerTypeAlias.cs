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

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// <c>integer</c> is a documented alias of <c>int64</c>, but it has its own lexer token, and only the
/// cast rules accepted that token: <c>CREATE TABLE t (x integer)</c> failed with "Syntax error,
/// unexpected TINTEGER". It is now accepted everywhere a type is, in any case.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestIntegerTypeAlias : BaseTest
{
    private static async Task DdlAsync(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
    }

    private static async Task<List<QueryResultRow>> QueryAsync(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction tx = await db.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await db.Transactions.CommitAsync(tx);
        return rows;
    }

    [Test]
    public async Task Integer_IsAcceptedAsAColumnType_InAnyCase()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await DdlAsync(executor, dbname,
            "CREATE TABLE t (id int64 PRIMARY KEY, x integer, y INTEGER NOT NULL DEFAULT(3), z Integer, xs array(integer))");

        TableSchema schema = (await executor.OpenDatabase(dbname)).Schema.Tables["t"];

        Assert.AreEqual(ColumnType.Integer64, schema.Columns!.Single(c => c.Name == "x").Type);
        Assert.AreEqual(ColumnType.Integer64, schema.Columns!.Single(c => c.Name == "y").Type);
        Assert.AreEqual(ColumnType.Integer64, schema.Columns!.Single(c => c.Name == "z").Type);

        TableColumnSchema xs = schema.Columns!.Single(c => c.Name == "xs");
        Assert.AreEqual(ColumnType.Array, xs.Type);
        Assert.AreEqual(ColumnType.Integer64, xs.ArrayElementType);
    }

    [Test]
    public async Task Integer_IsAcceptedByAlterTableAddColumn()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await DdlAsync(executor, dbname, "CREATE TABLE t (id int64 PRIMARY KEY)");
        await DdlAsync(executor, dbname, "ALTER TABLE t ADD COLUMN n integer");
        await DdlAsync(executor, dbname, "ALTER TABLE t ADD m INTEGER NULL");

        TableSchema schema = (await executor.OpenDatabase(dbname)).Schema.Tables["t"];

        Assert.AreEqual(ColumnType.Integer64, schema.Columns!.Single(c => c.Name == "n").Type);
        Assert.AreEqual(ColumnType.Integer64, schema.Columns!.Single(c => c.Name == "m").Type);
    }

    [Test]
    public async Task Integer_IsAcceptedByCastAndColonColon()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        List<QueryResultRow> rows = await QueryAsync(executor, dbname,
            "SELECT CAST('41' AS integer) AS a, '42'::INTEGER AS b, CAST(43.0 AS Integer) AS c");

        Assert.AreEqual(ColumnType.Integer64, rows[0].Row["a"].Type);
        Assert.AreEqual(41L, rows[0].Row["a"].LongValue);
        Assert.AreEqual(42L, rows[0].Row["b"].LongValue);
        Assert.AreEqual(43L, rows[0].Row["c"].LongValue);
    }

    /// <summary>The alias is spelling only: the column renders, and round-trips, as INT64.</summary>
    [Test]
    public async Task Integer_RendersAsInt64()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await DdlAsync(executor, dbname, "CREATE TABLE t (id int64 PRIMARY KEY, x integer, xs array(integer))");

        string ddl = (await QueryAsync(executor, dbname, "SHOW CREATE TABLE t"))[0].Row["Create Table"].StrValue!;

        StringAssert.Contains("`x` INT64 NULL", ddl);
        StringAssert.Contains("`xs` ARRAY(INT64) NULL", ddl);
    }
}
