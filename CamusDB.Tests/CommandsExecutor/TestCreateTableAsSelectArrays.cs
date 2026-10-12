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
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// <c>CREATE TABLE … AS SELECT</c> and <c>CREATE MATERIALIZED VIEW</c> create an array column with the
/// element type of the query's column. They used to create it with none, which left a column
/// <c>SHOW CREATE TABLE</c> could not render.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestCreateTableAsSelectArrays : BaseTest
{
    // Committed: CREATE TABLE ... AS SELECT loads its rows in the caller's transaction.
    private static async Task DdlAsync(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction tx = await db.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
            await db.Transactions.CommitAsync(tx);
        }
        catch
        {
            await db.Transactions.RollbackAsync(tx);
            throw;
        }
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

    private async Task<(string dbname, CommandExecutor executor)> SetupSourceAsync()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await DdlAsync(executor, dbname,
            "CREATE TABLE src (id int64 PRIMARY KEY, ad array(date), ab array(bytes), af array(float64))");

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(txn, dbname, "src", new()
        {
            new()
            {
                { "id", new ColumnValue(ColumnType.Integer64, 1L) },
                { "ad", ColumnValue.FromArray(ColumnType.Date, [new ColumnValue(ColumnType.Date, new DateTime(2000, 1, 1).Ticks)]) },
                { "ab", ColumnValue.FromArray(ColumnType.Bytes, [new ColumnValue(new byte[] { 1, 2 })]) },
                { "af", ColumnValue.FromArray(ColumnType.Float64, [new ColumnValue(ColumnType.Float64, 1.5)]) },
            }
        }));
        await database.Transactions.CommitAsync(txn);

        return (dbname, executor);
    }

    [Test]
    public async Task Ctas_CopiesArrayElementTypes()
    {
        (string dbname, CommandExecutor executor) = await SetupSourceAsync();

        await DdlAsync(executor, dbname, "CREATE TABLE copy AS SELECT id, ad, ab, af, ARRAY[1, 2] AS lit FROM src");

        string ddl = (await QueryAsync(executor, dbname, "SHOW CREATE TABLE copy"))[0].Row["Create Table"].StrValue!;

        StringAssert.Contains("`ad` ARRAY(DATE)", ddl);
        StringAssert.Contains("`ab` ARRAY(BYTES)", ddl);
        StringAssert.Contains("`af` ARRAY(FLOAT64)", ddl);
        StringAssert.Contains("`lit` ARRAY(INT64)", ddl);

        QueryResultRow row = (await QueryAsync(executor, dbname, "SELECT ad, ab, af, lit FROM copy"))[0];

        Assert.AreEqual(ColumnType.Date, row.Row["ad"].ArrayElementType);
        Assert.AreEqual(new DateTime(2000, 1, 1).Ticks, row.Row["ad"].ArrayValues![0].LongValue);
        Assert.AreEqual(new byte[] { 1, 2 }, row.Row["ab"].ArrayValues![0].BytesValue);
        Assert.AreEqual(1.5, row.Row["af"].ArrayValues![0].FloatValue);
        Assert.AreEqual(2L, row.Row["lit"].ArrayValues![1].LongValue);

        // The rendered DDL is replayable: the dump case.
        await DdlAsync(executor, dbname, ddl.Replace("`copy`", "`copy2`", StringComparison.Ordinal));
    }

    [Test]
    public async Task MaterializedView_CopiesArrayElementTypes()
    {
        (string dbname, CommandExecutor executor) = await SetupSourceAsync();

        await DdlAsync(executor, dbname, "CREATE MATERIALIZED VIEW mv AS SELECT id, ad, af FROM src");

        TableSchema schema = (await executor.OpenDatabase(dbname)).Schema.Tables["mv"];

        Assert.AreEqual(ColumnType.Date, schema.Columns!.Single(c => c.Name == "ad").ArrayElementType);
        Assert.AreEqual(ColumnType.Float64, schema.Columns!.Single(c => c.Name == "af").ArrayElementType);
    }

    /// <summary>
    /// An array whose element type is only known once evaluated is refused with a message that names
    /// the column, rather than creating a column nothing can describe.
    /// </summary>
    [Test]
    public async Task Ctas_ArrayWithUnknownElementType_IsRefused()
    {
        (string dbname, CommandExecutor executor) = await SetupSourceAsync();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            () => DdlAsync(executor, dbname, "CREATE TABLE bad AS SELECT id, regexp_split_to_array('a,b', ',') AS parts FROM src"))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("'parts'", ex.Message);
        Assert.IsFalse((await executor.OpenDatabase(dbname)).Schema.Tables.ContainsKey("bad"));
    }
}
