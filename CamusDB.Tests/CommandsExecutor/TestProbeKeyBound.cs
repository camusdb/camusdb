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
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The distinct keys of an index-lookup step are at most the rows that the key's table, or a
/// table with a column equal to the key, can have in the step's left input: the smallest estimate
/// of any subset of the left tables that contains such a table. A join after that table
/// multiplies its rows but adds none of them, and the join's memo answers the repeated keys. The
/// data: one keyword names 100 of 1,500 movies; each movie has 6 companies and 4 cast rows; 4,000
/// people. The chain keyword → movie_keywords → movie_companies → casts → people multiplies the
/// 100 movies to 600 company rows and 2,400 cast rows, but the casts still have 100 movies and at
/// most 400 people.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestProbeKeyBound : BaseTest
{
    private sealed record Db(string Name, DatabaseDescriptor Database, CommandExecutor Executor);

    private const int Movies = 1500;
    private const int KeywordMovies = 100;
    private const int CompaniesPerMovie = 6;
    private const int CastPerMovie = 4;
    private const int People = 4000;

    private const string Chain =
        "SELECT MIN(p.name) FROM keywords k, movie_keywords mk, movie_companies mc, casts ci, people p " +
        "WHERE k.keyword = 'keyword 1' AND mk.keyword_id = k.id AND mc.movie_id = mk.movie_id " +
        "AND ci.movie_id = mc.movie_id AND p.id = ci.person_id";

    [Test]
    public async Task AMultipliedLeftInput_ProbesOnceForEachKeyItStillHas()
    {
        Db db = await CreateTables(Options with { PlanCacheEnabled = false });

        // 600 company rows carry 100 movies: 100 probes into the casts, not 600. 2,400 cast rows
        // carry at most 400 people (the 100 movies' casts): 400 probes into the people, where a
        // hash join reads all 4,000. Counted per row, both steps would be hash joins.
        List<QueryResultRow> plan = await Query(db, "EXPLAIN (ANALYZE) " + Chain);
        string text = PlanText(plan);

        Assert.That(text, Does.Not.Contain("hash-join"), "every step must probe:\n" + text);
        Assert.That(text, Does.Contain("index=~pk, left=ci.person_id"), "the people join must probe the primary key:\n" + text);
        Assert.That(text, Does.Contain("index=movie_id_casts, left=mc.movie_id"), "the casts join must probe the movie index:\n" + text);

        // What the probes did: the memo answered the repeats.
        Assert.That(Lookups(plan, "index=~pk, left=ci.person_id"), Is.LessThanOrEqualTo(KeywordMovies * CastPerMovie));
    }

    [Test]
    public async Task APlanCacheHit_PricesTheBoundOfTheMiss()
    {
        // A replay estimates only the prefixes of the cached order, so it cannot read the bound
        // from the subsets; the entry carries it, and the hit builds the probes of the miss.
        CamusDBOptions options = Options with { CostBasedJoinOrderEnabled = true, PlanCacheEnabled = true };
        Db db = await CreateTables(options);
        PlanCache cache = db.Executor.PlanCache;
        cache.Clear();

        QueryPlan miss = await Plan(db, Chain, options, cache);
        long hits = cache.Hits;
        long failures = cache.ReplayFailures;
        QueryPlan hit = await Plan(db, Chain, options, cache);

        Assert.That(cache.Hits, Is.EqualTo(hits + 1), "the second plan must be a cache hit");
        Assert.That(cache.ReplayFailures, Is.EqualTo(failures), "the hit must replay the cached order");
        Assert.That(hit.JoinOrderSearch?.Path, Is.EqualTo("cached"));

        string rendered = PlanRenderer.Render(hit);
        Assert.That(rendered, Is.EqualTo(PlanRenderer.Render(miss)), "the hit must build the tree of the miss");
        Assert.That(rendered, Does.Not.Contain("hash-join"), "every step must probe on the hit as on the miss:\n" + rendered);
    }

    [Test]
    public void MinRowsContainingTable_IsTheSmallestSubsetWithTheLeaf()
    {
        // Three leaves a, b, c: masks 1, 2, 4.
        double[] rows = new double[8];
        rows[0b001] = 10; rows[0b010] = 20; rows[0b100] = 5;
        rows[0b011] = 100; rows[0b101] = 50; rows[0b110] = 2;
        rows[0b111] = 1000;

        double[] withA = JoinEnumerator.MinRowsContainingTable(rows, 3, leaf: 0);
        double[] withC = JoinEnumerator.MinRowsContainingTable(rows, 3, leaf: 2);

        Assert.That(withA[0b111], Is.EqualTo(10), "a alone is the smallest subset with a");
        Assert.That(withA[0b011], Is.EqualTo(10));
        Assert.That(withA[0b001], Is.EqualTo(10));
        Assert.That(withA[0b110], Is.EqualTo(double.PositiveInfinity), "no subset of {b, c} has a");
        Assert.That(withC[0b111], Is.EqualTo(2), "{b, c} is the smallest subset with c");
        Assert.That(withC[0b101], Is.EqualTo(5));
        Assert.That(withC[0b011], Is.EqualTo(double.PositiveInfinity));
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private async Task<Db> CreateTables(CamusDBOptions options)
    {
        (string name, DatabaseDescriptor database, CommandExecutor executor) =
            await CreateDatabase(options with { CostBasedJoinOrderEnabled = true });
        Db db = new(name, database, executor);

        await Ddl(db, "CREATE TABLE keywords (id int64 primary key, keyword string)");
        await Ddl(db, "CREATE TABLE movie_keywords (id int64 primary key, movie_id int64, keyword_id int64)");
        await Ddl(db, "CREATE INDEX keyword_id_movie_keywords ON movie_keywords (keyword_id)");
        await Ddl(db, "CREATE INDEX movie_id_movie_keywords ON movie_keywords (movie_id)");
        await Ddl(db, "CREATE TABLE movie_companies (id int64 primary key, movie_id int64, company_id int64)");
        await Ddl(db, "CREATE INDEX movie_id_movie_companies ON movie_companies (movie_id)");
        await Ddl(db, "CREATE TABLE casts (id int64 primary key, movie_id int64, person_id int64)");
        await Ddl(db, "CREATE INDEX movie_id_casts ON casts (movie_id)");
        await Ddl(db, "CREATE INDEX person_id_casts ON casts (person_id)");
        await Ddl(db, "CREATE TABLE people (id int64 primary key, name string)");

        await Insert(db, "keywords (id, keyword)", Enumerable.Range(1, 50).Select(i => $"({i}, 'keyword {i}')"));

        // Movies 1 to 100 carry keyword 1; the others spread over keywords 2 to 50.
        await Insert(db, "movie_keywords (id, movie_id, keyword_id)",
            Enumerable.Range(1, Movies).Select(m => $"({m}, {m}, {(m <= KeywordMovies ? 1 : 2 + m % 49)})"));

        List<string> companies = new(Movies * CompaniesPerMovie);
        List<string> casts = new(Movies * CastPerMovie);
        for (int m = 1; m <= Movies; m++)
        {
            for (int j = 0; j < CompaniesPerMovie; j++)
                companies.Add($"({(m - 1) * CompaniesPerMovie + j + 1}, {m}, {1 + ((m - 1) * CompaniesPerMovie + j) % 300})");

            for (int j = 0; j < CastPerMovie; j++)
                casts.Add($"({(m - 1) * CastPerMovie + j + 1}, {m}, {1 + ((m - 1) * CastPerMovie + j) % People})");
        }

        await Insert(db, "movie_companies (id, movie_id, company_id)", companies);
        await Insert(db, "casts (id, movie_id, person_id)", casts);
        await Insert(db, "people (id, name)", Enumerable.Range(1, People).Select(i => $"({i}, 'person {i}')"));

        foreach (string table in new[] { "keywords", "movie_keywords", "movie_companies", "casts", "people" })
            await Query(db, "ANALYZE " + table);

        return db;
    }

    private static async Task Insert(Db db, string target, IEnumerable<string> rows)
    {
        List<string> all = rows.ToList();
        for (int start = 0; start < all.Count; start += 500)
            await Exec(db, $"INSERT INTO {target} VALUES " + string.Join(", ", all.Skip(start).Take(500)));
    }

    private static async Task Ddl(Db db, string sql)
    {
        KvTransaction txn = await db.Database.Transactions.BeginAsync();
        await db.Executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: txn, database: db.Name, sql: sql, parameters: null));
        await db.Database.Transactions.CommitAsync(txn);
    }

    private static async Task Exec(Db db, string sql)
    {
        KvTransaction txn = await db.Database.Transactions.BeginAsync();
        await db.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(txnState: txn, database: db.Name, sql: sql, parameters: null));
        await db.Database.Transactions.CommitAsync(txn);
    }

    /// <summary>The plan of <paramref name="sql"/> from a planner over <paramref name="cache"/>, as the executor plans it.</summary>
    private async Task<QueryPlan> Plan(Db db, string sql, CamusDBOptions options, PlanCache cache)
    {
        ExecuteSQLTicket executeTicket = new(txnState: await db.Database.Transactions.BeginAsync(), database: db.Name, sql: sql, parameters: null);
        SelectQuery selectQuery = new SelectQueryCreator().CreateSelectQuery(SQLParserProcessor.Parse(sql));
        BoundSelectQuery bound = await new QueryBinder(new TableOpener(db.Executor.Catalogs, logger)).BindAsync(db.Database, selectQuery);
        QueryTicket ticket = QueryTicketAdapter.ToQueryTicket(bound, executeTicket);

        return new JoinQueryPlanner(db.Executor.Statistics, cache, options).GetPlan(db.Database, bound, ticket);
    }

    private static string PlanText(List<QueryResultRow> plan) =>
        string.Join("\n", plan.Select(r => r.Row["node"].StrValue + " " + r.Row["detail"].StrValue));

    /// <summary>The KV lookups of the plan node whose detail contains <paramref name="marker"/>.</summary>
    private static long Lookups(List<QueryResultRow> plan, string marker)
    {
        foreach (QueryResultRow row in plan)
        {
            if ((row.Row["detail"].StrValue ?? "").Contains(marker))
                return row.Row["kv_lookups"].LongValue;
        }

        Assert.Fail("no plan node with " + marker + ":\n" + PlanText(plan));
        return 0;
    }

    private static async Task<List<QueryResultRow>> Query(Db db, string sql)
    {
        KvTransaction txn = await db.Database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await db.Executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: txn, database: db.Name, sql: sql, parameters: null));

        List<QueryResultRow> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add(row);

        await db.Database.Transactions.CommitAsync(txn);
        return rows;
    }
}
