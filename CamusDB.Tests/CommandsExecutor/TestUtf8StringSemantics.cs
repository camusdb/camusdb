
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

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Locks the string collation contract now that rows store UTF-8: ordering and range comparisons must
/// still follow <see cref="string.CompareOrdinal(string,string)"/> (UTF-16 code-unit order), NOT the
/// UTF-8 byte / Unicode code-point order the storage now uses. The two disagree for supplementary
/// characters — e.g. "😀" (U+1F600, UTF-16 lead unit 0xD83D) sorts <b>before</b> "豈" (U+F900) under
/// CompareOrdinal but <b>after</b> it in code-point order — so ORDER BY over exactly that pair proves the
/// storage change did not leak UTF-8 ordering into the query layer. Rows are inserted through the ticket
/// API rather than SQL literals so the data (a surrogate-pair emoji and an empty string) does not depend
/// on the SQL lexer accepting those literal forms.
/// </summary>
[NonParallelizable]
public sealed class TestUtf8StringSemantics : SharedNodeBaseTest
{
    // Includes a supplementary char (😀 = U+1F600, UTF-16 lead unit 0xD83D) and a high BMP char
    // (U+F900, single unit 0xF900) that order OPPOSITELY under CompareOrdinal vs UTF-8/code-point order:
    // 0xD83D < 0xF900 so 😀 < U+F900 by CompareOrdinal, but 😀's UTF-8 starts 0xF0 > U+F900's 0xEF, and
    // U+1F600 > U+F900 by code point — so a byte/code-point sort would flip them. Explicit escapes avoid
    // pasting a glyph that is actually the (non-discriminating) CJK-unified codepoint. Plus an empty string.
    private static readonly string[] Values = { "a", "z", "é", "中", "\U0001F600", "\uF900", "" };

    private static async Task<List<QueryResultRow>> QueryAsync(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    private async Task<(string, DatabaseDescriptor, CommandExecutor)> SeedAsync()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction ddl = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddl, dbname,
            "CREATE TABLE t (id INT64 NOT NULL, s STRING NOT NULL, PRIMARY KEY (id))", null));
        await database.Transactions.CommitAsync(ddl);

        KvTransaction ins = await database.Transactions.BeginAsync();
        List<Dictionary<string, ColumnValue>> rows = new();
        for (int i = 0; i < Values.Length; i++)
            rows.Add(new()
            {
                ["id"] = new ColumnValue(ColumnType.Integer64, i),
                ["s"] = new ColumnValue(ColumnType.String, Values[i]),
            });
        await executor.Insert(new InsertTicket(txnState: ins, databaseName: dbname, tableName: "t", values: rows));
        await database.Transactions.CommitAsync(ins);

        return (dbname, database, executor);
    }

    [Test]
    public async Task OrderBy_NonAscii_FollowsCompareOrdinalNotUtf8()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SeedAsync();

        List<QueryResultRow> rows = await QueryAsync(executor, database, dbname, "SELECT s FROM t ORDER BY s");
        List<string> got = rows.Select(r => r.Row["s"].StrValue!).ToList();

        List<string> expectedOrdinal = Values.OrderBy(v => v, StringComparer.Ordinal).ToList();
        CollectionAssert.AreEqual(expectedOrdinal, got, "ORDER BY must match string.CompareOrdinal");

        // Sanity: the stored representation is UTF-8, whose byte order differs from CompareOrdinal for the
        // 😀/豈 pair — so the result must NOT equal the UTF-8/code-point ordering. This proves the
        // assertion above is meaningful rather than accidentally true.
        List<string> utf8Order = Values
            .OrderBy(v => System.Text.Encoding.UTF8.GetBytes(v), ByteArrayComparer.Instance)
            .ToList();
        Assert.AreNotEqual(utf8Order, got, "result matched UTF-8 byte order — collation leaked into the query layer");
    }

    [Test]
    public async Task Equality_MultiByte_MatchesExactly()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SeedAsync();

        // Filtered, non-retaining scan → borrowed decode + byte-native equality fast path, on a multi-byte
        // (3-byte UTF-8) CJK literal.
        List<QueryResultRow> cjk = await QueryAsync(executor, database, dbname, "SELECT id FROM t WHERE s = \"中\"");
        Assert.AreEqual(1, cjk.Count);
        Assert.AreEqual(3L, cjk[0].Row["id"].LongValue); // 中 was inserted at id 3

        // A non-matching multi-byte literal returns nothing (byte-native compare must not false-positive).
        List<QueryResultRow> none = await QueryAsync(executor, database, dbname, "SELECT id FROM t WHERE s = \"漢\"");
        Assert.AreEqual(0, none.Count);
    }

    [Test]
    public async Task RangeComparison_NonAscii_FollowsCompareOrdinal()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SeedAsync();

        // Borrowed-path range predicate (`<`) must order by CompareOrdinal, not UTF-8 bytes.
        List<QueryResultRow> rows = await QueryAsync(executor, database, dbname, "SELECT s FROM t WHERE s < \"中\"");
        HashSet<string> got = rows.Select(r => r.Row["s"].StrValue!).ToHashSet();

        HashSet<string> expected = Values.Where(v => string.CompareOrdinal(v, "中") < 0).ToHashSet();
        Assert.That(got, Is.EquivalentTo(expected));
        // 😀 lead unit 0xD83D > 中 0x4E2D, so 😀 is NOT below 中 under CompareOrdinal (it would be, wrongly,
        // under code-point order).
        Assert.IsFalse(got.Contains("😀"));
    }

    private sealed class ByteArrayComparer : IComparer<byte[]>
    {
        public static readonly ByteArrayComparer Instance = new();
        public int Compare(byte[]? x, byte[]? y) => ((ReadOnlySpan<byte>)(x ?? [])).SequenceCompareTo(y ?? []);
    }
}
