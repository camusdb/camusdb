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
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// <c>substring</c> and <c>substr</c> are one function with the bounds of PostgreSQL. Each expected result was
/// measured on PostgreSQL 16.11 in the C locale. A NULL renders as an empty cell, bytes render as lowercase
/// hexadecimal digits, and an expected value that starts with <c>CADB</c> is an error code where PostgreSQL
/// also gave an error.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestSubstringBounds : BaseTest
{
    private static string Render(ColumnValue value) => value.Type switch
    {
        ColumnType.Bool => value.BoolValue ? "t" : "f",
        ColumnType.Null => "",
        ColumnType.String => value.StrValue!,
        ColumnType.Bytes => Convert.ToHexString(value.BytesValue ?? []).ToLowerInvariant(),
        _ => throw new ArgumentException(value.Type.ToString()),
    };

    private static async Task<string> Run(
        DatabaseDescriptor database,
        CommandExecutor executor,
        string dbname,
        string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, parameters));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            return string.Join(" ; ", rows.Select(row => string.Join("|", row.Row.Values.Select(Render))));
        }
        catch (CamusDBException e)
        {
            return e.Code;
        }
        finally
        {
            await database.Transactions.RollbackAsync(tx);
        }
    }

    [TestCase("('abcde', 2)", "bcde")]
    [TestCase("('abcde', 2, 2)", "bc")]
    [TestCase("(NULL, 1)", "")]
    [TestCase("('abc', NULL)", "")]
    [TestCase("('abc', 1, NULL)", "")]
    [TestCase("(NULL, -1, -1)", "")]
    [TestCase("('abc', NULL, -1)", "")]
    [TestCase("('abcde', 0)", "abcde")]
    [TestCase("('abcde', -1)", "abcde")]
    [TestCase("('abcde', 0, 2)", "a")]
    [TestCase("('abcde', -1, 3)", "a")]
    [TestCase("('abcde', -5, 3)", "")]
    [TestCase("('abcde', -2, 0)", "")]
    [TestCase("('abcde', 0, 0)", "")]
    [TestCase("('abcde', 1, 0)", "")]
    [TestCase("('abcde', 6)", "")]
    [TestCase("('abcde', 6, 1)", "")]
    [TestCase("('abcde', 100, 100)", "")]
    [TestCase("('abcde', 2147483647, 2147483647)", "")]
    [TestCase("('abcde', -2147483647, 2147483647)", "")]
    [TestCase("('abcde', 1, 2147483647)", "abcde")]
    [TestCase("('', 0, 2)", "")]
    [TestCase("('x🙂y', 0, 3)", "x🙂")]
    [TestCase("('x🙂y', -1, 4)", "x🙂")]
    [TestCase("('x🙂y', 2, 1)", "🙂")]
    [TestCase("('abcde', 2, -1)", "CADB0400")]
    [TestCase("('abcde', 100, -1)", "CADB0400")]
    [TestCase("('abcde', -1, -1)", "CADB0400")]
    [TestCase("(X'0102030405', 2)", "02030405")]
    [TestCase("(X'0102030405', 2, 2)", "0203")]
    [TestCase("(X'0102030405', 0, 2)", "01")]
    [TestCase("(X'0102030405', -1, 3)", "01")]
    [TestCase("(X'0102030405', -5, 3)", "")]
    [TestCase("(X'0102030405', 0)", "0102030405")]
    [TestCase("(X'0102030405', -1)", "0102030405")]
    [TestCase("(X'0102030405', 6)", "")]
    [TestCase("(X'0102030405', 1, 0)", "")]
    [TestCase("(X'0102030405', 2, -1)", "CADB0400")]
    public async Task BothNames_GiveTheSameResult(string arguments, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        Assert.That(await Run(database, executor, dbname, "SELECT substr" + arguments), Is.EqualTo(expected), "substr");
        Assert.That(await Run(database, executor, dbname, "SELECT substring" + arguments), Is.EqualTo(expected), "substring");
    }

    /// <summary>PostgreSQL takes a 32-bit integer here and CamusDB takes 64 bits, so the sum of the two arguments must not wrap.</summary>
    [TestCase(long.MaxValue, long.MaxValue, "")]
    [TestCase(1L, long.MaxValue, "abcde")]
    [TestCase(2L, long.MaxValue, "bcde")]
    [TestCase(long.MinValue, long.MaxValue, "")]
    [TestCase(long.MinValue, 0L, "")]
    [TestCase(long.MinValue + 1, 1L, "")]
    [TestCase(-5L, 5L, "")]
    [TestCase(-5L, 7L, "a")]
    [TestCase(-1L, long.MaxValue, "abcde")]
    public async Task LargeArguments_DoNotWrap(long position, long count, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        Dictionary<string, ColumnValue> parameters = new()
        {
            ["@p"] = new ColumnValue(ColumnType.Integer64, position),
            ["@c"] = new ColumnValue(ColumnType.Integer64, count),
        };

        Assert.That(await Run(database, executor, dbname, "SELECT substr('abcde', @p, @c)", parameters), Is.EqualTo(expected));
    }

    [Test]
    public async Task LargePosition_WithNoCount_IsTheWholeValue()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        Dictionary<string, ColumnValue> parameters = new() { ["@p"] = new ColumnValue(ColumnType.Integer64, long.MinValue) };

        Assert.That(await Run(database, executor, dbname, "SELECT substring('abcde', @p)", parameters), Is.EqualTo("abcde"));
    }

    /// <summary>A wrong number of arguments is a statement mistake, so an empty table cannot hide it.</summary>
    [TestCase("SELECT substr('abc') FROM em", "CADB0400")]
    [TestCase("SELECT substr('abc', 1, 2, 3) FROM em", "CADB0400")]
    public async Task ArgumentCount_IsCheckedWithNoRows(string sql, string code)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction ddl = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddl, dbname, "CREATE TABLE em (id INT64 PRIMARY KEY)", null));
        await database.Transactions.CommitAsync(ddl);

        Assert.That(await Run(database, executor, dbname, sql), Is.EqualTo(code));
    }

    /// <summary>The evaluator names the function that the statement called, so the error reads as the statement does.</summary>
    [TestCase("SELECT substr(1, 1)", "Function 'substr' expects argument 1")]
    [TestCase("SELECT substr('abc', 'x')", "Function 'substr' expects argument 2")]
    [TestCase("SELECT substr('abc', 1, 'x')", "Function 'substr' expects argument 3")]
    [TestCase("SELECT substr('abc', 1, -1)", "Function 'substr' expects argument 3 to be a non-negative length")]
    [TestCase("SELECT SUBSTR('abc')", "Function 'SUBSTR' expects 2 to 3")]
    public async Task Error_NamesTheCalledFunction(string sql, string message)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        KvTransaction tx = await database.Transactions.BeginAsync();

        CamusDBException? error = Assert.CatchAsync<CamusDBException>(async () =>
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
            await cursor.ToListAsync();
        });

        await database.Transactions.RollbackAsync(tx);
        Assert.That(error!.Message, Does.Contain(message).IgnoreCase);
    }

    /// <summary>The result type is known with no row, because the planner reads it from the first argument.</summary>
    [Test]
    public void ReturnType_FollowsTheFirstArgument_ForBothNames()
    {
        foreach (string name in new[] { "substr", "substring" })
        {
            Assert.That(ScalarFunctionEvaluator.InferReturnType(name, [ColumnType.Bytes, ColumnType.Integer64]), Is.EqualTo(ColumnType.Bytes), name);
            Assert.That(ScalarFunctionEvaluator.InferReturnType(name, [ColumnType.String, ColumnType.Integer64]), Is.EqualTo(ColumnType.String), name);
        }
    }
}
