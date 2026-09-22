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
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// An <c>IN</c> list must return every matching row whatever share of the table it names.
///
/// <para>The planner seeks the index for each list item while the list names less than about half
/// of the table, and scans the table with the list as a filter from that point on. The two paths must
/// agree for every pair of types that <c>=</c> accepts. A uuid or object id column filtered by values
/// bound as strings (what a client binds for a <c>Guid</c> or a string key) is the pair where they
/// did not: the seek parsed the strings, the filter compared them raw, and a list over half the table
/// returned no rows at all. Each test therefore walks the list size across that threshold and checks
/// the count, the rows, and <c>NOT IN</c>, since a filter that matches nothing makes <c>NOT IN</c>
/// return every row.</para>
/// </summary>
[NonParallelizable]
public sealed class TestInListHighSelectivity : BaseTest
{
    private static readonly int[] Percentages = [1, 2, 10, 25, 49, 50, 51, 75, 100];

    [Test]
    public async Task UuidKey_StringParameters_ReturnsEveryMatchAcrossTheThreshold([Values(100, 400)] int tableRows)
    {
        (string db, CommandExecutor executor, List<string> ids) = await Seed(tableRows, "uuid", UuidKeys);

        foreach (int percent in Percentages)
        {
            int count = tableRows * percent / 100;
            await AssertInList(executor, db, tableRows, StringParameters(ids.Take(count)), count);
        }
    }

    [Test]
    public async Task UuidKey_StringLiterals_ReturnsEveryMatchAcrossTheThreshold()
    {
        (string db, CommandExecutor executor, List<string> ids) = await Seed(100, "uuid", UuidKeys);

        foreach (int count in Percentages)
        {
            string list = "(" + string.Join(", ", ids.Take(count).Select(id => $"'{id}'")) + ")";

            Assert.AreEqual(count, await Count(executor, db, $"WHERE id IN {list}", null), $"IN, {count} of 100");
            Assert.AreEqual(100 - count, await Count(executor, db, $"WHERE id NOT IN {list}", null), $"NOT IN, {count} of 100");
        }
    }

    [Test]
    public async Task UuidKey_PaddedParameterList_ReturnsEveryDistinctMatch()
    {
        (string db, CommandExecutor executor, List<string> ids) = await Seed(400, "uuid", UuidKeys);

        // 151 distinct ids sent as 200 placeholders, repeating the last id, as EF Core pads a list.
        List<string> padded = [.. ids.Take(151), .. Enumerable.Repeat(ids[150], 49)];

        await AssertInList(executor, db, 400, StringParameters(padded), 151);
    }

    [Test]
    public async Task UuidKey_OtherSpellings_MatchOnBothPaths()
    {
        (string db, CommandExecutor executor, List<string> ids) = await Seed(100, "uuid", UuidKeys);

        // Upper case and the 32-hex form parse to the same UUID, as they do for `=`.
        foreach (int count in new[] { 5, 60 })
        {
            List<string> spelled = ids.Take(count)
                .Select((id, i) => i % 2 == 0 ? id.ToUpperInvariant() : id.Replace("-", ""))
                .ToList();

            await AssertInList(executor, db, 100, StringParameters(spelled), count);
        }
    }

    [Test]
    public async Task UuidKey_MalformedStringItem_MatchesNothingAndDoesNotThrow()
    {
        (string db, CommandExecutor executor, List<string> ids) = await Seed(100, "uuid", UuidKeys);

        foreach (int count in new[] { 5, 60 })
        {
            Dictionary<string, ColumnValue> parameters = StringParameters(ids.Take(count));
            parameters["@bad"] = new ColumnValue(ColumnType.String, "not-a-uuid");

            await AssertInList(executor, db, 100, parameters, count);
        }
    }

    [Test]
    public async Task ObjectIdKey_StringParameters_ReturnsEveryMatchAcrossTheThreshold()
    {
        (string db, CommandExecutor executor, List<string> ids) = await Seed(100, "object_id", ObjectIdKeys);

        foreach (int count in Percentages)
            await AssertInList(executor, db, 100, StringParameters(ids.Take(count)), count);
    }

    [Test]
    public async Task UuidKey_Projection_AgreesWithEquality()
    {
        (string db, CommandExecutor executor, List<string> ids) = await Seed(20, "uuid", UuidKeys);

        // Eleven items passes the prepared set's hash threshold; the projection takes the AST path.
        Dictionary<string, ColumnValue> parameters = StringParameters(ids.Take(11));
        string list = "(" + string.Join(", ", parameters.Keys) + ")";

        List<IReadOnlyDictionary<string, ColumnValue>> rows = await Query(
            executor, db, $"SELECT id IN {list} AS m, id = @p0 AS e FROM users", parameters);

        Assert.AreEqual(20, rows.Count);
        Assert.AreEqual(11, rows.Count(r => r["m"].BoolValue));
        Assert.AreEqual(1, rows.Count(r => r["e"].BoolValue));
    }

    [Test]
    public async Task StringKey_UuidParameters_IndexAndScanAgree()
    {
        (string db, CommandExecutor executor, List<string> ids) = await Seed(100, "string", UuidKeys);

        // `string_col = uuid` parses the stored string, so a Uuid value matches its row. Before, the
        // index seek built a Uuid key that no String entry has, and only the scan found the row.
        foreach (int count in new[] { 1, 10, 50, 100 })
        {
            Dictionary<string, ColumnValue> parameters = [];
            int i = 0;
            foreach (string id in ids.Take(count))
                parameters[$"@p{i++}"] = ColumnValue.FromUuid(Guid.Parse(id));

            await AssertInList(executor, db, 100, parameters, count);
        }

        Assert.AreEqual(1, await Count(executor, db, "WHERE id = @p", new() { ["@p"] = ColumnValue.FromUuid(Guid.Parse(ids[0])) }));
    }

    [Test]
    public async Task StringKey_StringParameters_ReturnsEveryMatchAcrossTheThreshold()
    {
        (string db, CommandExecutor executor, List<string> ids) = await Seed(100, "string", UuidKeys);

        foreach (int count in Percentages)
            await AssertInList(executor, db, 100, StringParameters(ids.Take(count)), count);
    }

    private static async Task AssertInList(
        CommandExecutor executor, string db, int tableRows, Dictionary<string, ColumnValue> parameters, int expected)
    {
        string list = "(" + string.Join(", ", parameters.Keys) + ")";
        string label = $"{parameters.Count} items, {expected} matches of {tableRows}";

        Assert.AreEqual(expected, await Count(executor, db, $"WHERE id IN {list}", parameters), $"COUNT IN, {label}");
        Assert.AreEqual(expected, await Count(executor, db, $"AS u WHERE u.id IN {list}", parameters), $"aliased COUNT IN, {label}");
        Assert.AreEqual(expected, (await Query(executor, db, $"SELECT id FROM users WHERE id IN {list}", parameters)).Count, $"rows IN, {label}");
        Assert.AreEqual(tableRows - expected, await Count(executor, db, $"WHERE id NOT IN {list}", parameters), $"COUNT NOT IN, {label}");
    }

    private static Dictionary<string, ColumnValue> StringParameters(IEnumerable<string> values)
    {
        Dictionary<string, ColumnValue> parameters = [];
        int i = 0;
        foreach (string value in values)
            parameters[$"@p{i++}"] = new ColumnValue(ColumnType.String, value);
        return parameters;
    }

    private static string UuidKeys(int i) => Guid.NewGuid().ToString();

    private static string ObjectIdKeys(int i) => (0x100000 + i).ToString("x24");

    private async Task<(string Db, CommandExecutor Executor, List<string> Ids)> Seed(int rows, string idType, Func<int, string> key)
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(executor, db, $"CREATE TABLE users (id {idType} PRIMARY KEY NOT NULL, name string NULL)");

        List<string> ids = [];
        List<string> values = [];
        for (int i = 0; i < rows; i++)
        {
            string id = key(i);
            ids.Add(id);
            values.Add($"('{id}', 'user{i}')");
        }

        await ExecuteNonQuery(executor, db, "INSERT INTO users (id, name) VALUES " + string.Join(", ", values));

        // With statistics the planner takes the cost-based path, which a long-running server does.
        await Query(executor, db, "ANALYZE users", null);

        return (db, executor, ids);
    }

    private static async Task ExecuteDdl(CommandExecutor executor, string db, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, sql, null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task ExecuteNonQuery(CommandExecutor executor, string db, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<long> Count(CommandExecutor executor, string db, string fromTail, Dictionary<string, ColumnValue>? parameters)
    {
        List<IReadOnlyDictionary<string, ColumnValue>> rows =
            await Query(executor, db, $"SELECT COUNT(*) AS c FROM users {fromTail}", parameters);
        Assert.AreEqual(1, rows.Count);
        return rows[0]["c"].LongValue;
    }

    private static async Task<List<IReadOnlyDictionary<string, ColumnValue>>> Query(
        CommandExecutor executor, string db, string sql, Dictionary<string, ColumnValue>? parameters)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> rows) =
                await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, parameters));

            List<IReadOnlyDictionary<string, ColumnValue>> result = [];
            await foreach (QueryResultRow row in rows)
                result.Add(row.Row);

            await database.Transactions.CommitAsync(tx);
            return result;
        }
        catch
        {
            await database.Transactions.RollbackAsync(tx);
            throw;
        }
    }
}
