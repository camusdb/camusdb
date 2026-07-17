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

using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Verifies MySQL/SQL-standard quote doubling in string literals: <c>''</c> inside a single-quoted
/// string and <c>""</c> inside a double-quoted string decode to one literal quote, and the opposite
/// quote character is taken verbatim (no doubling needed). Both single- and double-quoted literals
/// are accepted as strings.
/// </summary>
[TestFixture]
public sealed class TestStringLiteralQuoteDoubling : BaseTest
{
    private async Task<(string dbname, DatabaseDescriptor db, CommandExecutor executor)> SetupTableAsync()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname,
            "CREATE TABLE t (id oid NOT NULL DEFAULT(gen_id()), v string, PRIMARY KEY (id))", null));
        await db.Transactions.CommitAsync(tx);
        return (dbname, db, executor);
    }

    private static async Task InsertAsync(CommandExecutor executor, DatabaseDescriptor db, string dbname, string valuesLiteral)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname,
            $"INSERT INTO t (v) VALUES ({valuesLiteral})", null));
        await db.Transactions.CommitAsync(tx);
    }

    private static async Task<List<string?>> SelectVAsync(CommandExecutor executor, DatabaseDescriptor db, string dbname, string? where = null)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        string sql = "SELECT v FROM t" + (where is null ? "" : " WHERE " + where);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(tx, dbname, sql, null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await db.Transactions.CommitAsync(tx);
        return rows.Select(r => r.Row["v"].StrValue).ToList();
    }

    [Test]
    public async Task SingleQuoteDoubling_DecodesToOneQuote()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTableAsync();
        await InsertAsync(executor, db, dbname, "'O''Brien'");

        List<string?> vals = await SelectVAsync(executor, db, dbname);
        Assert.AreEqual(1, vals.Count);
        Assert.AreEqual("O'Brien", vals[0]);
    }

    [Test]
    public async Task DoubleQuoteDoubling_DecodesToOneQuote()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTableAsync();
        await InsertAsync(executor, db, dbname, "\"a\"\"b\"");

        List<string?> vals = await SelectVAsync(executor, db, dbname);
        Assert.AreEqual(1, vals.Count);
        Assert.AreEqual("a\"b", vals[0]);
    }

    [Test]
    public async Task OppositeQuoteIsLiteral_NoDoublingNeeded()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTableAsync();
        // Single quote inside a double-quoted string, and vice versa.
        await InsertAsync(executor, db, dbname, "\"it's\"");
        await InsertAsync(executor, db, dbname, "'a\"b'");

        List<string?> vals = await SelectVAsync(executor, db, dbname);
        Assert.That(vals, Does.Contain("it's"));
        Assert.That(vals, Does.Contain("a\"b"));
    }

    [Test]
    public async Task DoubledQuotesInBothPositions_Decode()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTableAsync();
        // Leading and trailing doubled quotes plus adjacent pairs.
        await InsertAsync(executor, db, dbname, "'''x'''");   // '' x '' → 'x'

        List<string?> vals = await SelectVAsync(executor, db, dbname);
        Assert.AreEqual(1, vals.Count);
        Assert.AreEqual("'x'", vals[0]);
    }

    [Test]
    public async Task WhereClause_DoubledQuoteMatchesStoredValue()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupTableAsync();
        await InsertAsync(executor, db, dbname, "'O''Brien'");
        await InsertAsync(executor, db, dbname, "'Smith'");

        List<string?> matched = await SelectVAsync(executor, db, dbname, "v = 'O''Brien'");
        Assert.AreEqual(1, matched.Count);
        Assert.AreEqual("O'Brien", matched[0]);
    }
}
