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
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Expression indexes (<c>CREATE INDEX i ON t ((lower(email)))</c>) through SQL on a standalone node:
/// the index is built, kept by every write, used by the planner for a WHERE term that spells the same
/// expression, and gives the same rows as a table scan. Each refused expression class reports
/// <see cref="CamusDBErrorCodes.InvalidIndexExpression"/>.
/// </summary>
[NonParallelizable]
internal sealed class TestExpressionIndexes : BaseTest
{
    private static async Task ExecDDL(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        await db.Transactions.CommitAsync(tx);
    }

    private static async Task ExecNonQuery(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction tx = await db.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
            await db.Transactions.CommitAsync(tx);
        }
        catch
        {
            await db.Transactions.RollbackIfNotCompletedAsync(tx);
            throw;
        }
    }

    private static async Task<List<QueryResultRow>> ExecSelect(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction tx = await db.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await db.Transactions.CommitAsync(tx);
        return rows;
    }

    /// <summary>The scan node of the plan and its detail text.</summary>
    private static async Task<(string Node, string Detail)> ScanOf(CommandExecutor executor, string dbname, string select)
    {
        foreach (QueryResultRow row in await ExecSelect(executor, dbname, "EXPLAIN " + select))
        {
            string node = row.Row["node"].StrValue ?? "";
            if (node is "table-scan" or "index-range-scan" or "index-lookup" or "index-in-list-scan" or "index-in-list")
                return (node, row.Row["detail"].StrValue ?? "");
        }

        return ("(no scan node)", "");
    }

    private static async Task<List<string>> Emails(CommandExecutor executor, string dbname, string where)
    {
        List<QueryResultRow> rows = await ExecSelect(executor, dbname, "SELECT email FROM users WHERE " + where);
        return [.. rows.Select(r => r.Row["email"].StrValue ?? "<null>").OrderBy(e => e, System.StringComparer.Ordinal)];
    }

    private async Task<(string dbname, CommandExecutor executor)> CreateUsers()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname, "CREATE TABLE users (id oid primary key, email string, doc jsonb, score int64)");
        return (dbname, executor);
    }

    private static Task Insert(CommandExecutor executor, string dbname, string email, string doc, long score) =>
        ExecNonQuery(executor, dbname,
            $"INSERT INTO users (id, email, doc, score) VALUES (gen_id(), '{email}', '{doc}', {score})");

    [Test]
    public async Task UniqueLowerEmail_IsUsedByTheMatchingPredicate()
    {
        (string dbname, CommandExecutor executor) = await CreateUsers();
        await ExecDDL(executor, dbname, "CREATE UNIQUE INDEX users_lower_email ON users ((lower(email)))");

        await Insert(executor, dbname, "Ann@Example.com", "{\"email\": \"ann@example.com\"}", 1);
        await Insert(executor, dbname, "bob@example.com", "{\"email\": \"bob@example.com\"}", 2);

        Assert.That(await Emails(executor, dbname, "lower(email) = 'ann@example.com'"), Is.EqualTo(new[] { "Ann@Example.com" }));
        Assert.That(await Emails(executor, dbname, "LOWER( email ) = 'ann@example.com'"), Is.EqualTo(new[] { "Ann@Example.com" }));

        (string node, string detail) = await ScanOf(executor, dbname, "SELECT email FROM users WHERE lower(email) = 'ann@example.com'");
        Assert.That(node, Is.EqualTo("index-lookup"));
        Assert.That(detail, Does.Contain("users_lower_email"));
        Assert.That(detail, Does.Contain("lower(`email`)"));

        // An alias does not change the expression.
        (string aliased, _) = await ScanOf(executor, dbname, "SELECT u.email FROM users u WHERE lower(u.email) = 'ann@example.com'");
        Assert.That(aliased, Is.EqualTo("index-lookup"));
    }

    [Test]
    public async Task UniqueExpressionIndex_RefusesADuplicateValue()
    {
        (string dbname, CommandExecutor executor) = await CreateUsers();
        await ExecDDL(executor, dbname, "CREATE UNIQUE INDEX users_lower_email ON users ((lower(email)))");

        await Insert(executor, dbname, "ann@example.com", "{}", 1);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(() => Insert(executor, dbname, "ANN@EXAMPLE.COM", "{}", 2));
        Assert.That(ex!.Code, Is.EqualTo(CamusDBErrorCodes.DuplicateUniqueKeyValue));

        // NULL results are distinct, as for a plain unique index.
        await ExecNonQuery(executor, dbname, "INSERT INTO users (id, score) VALUES (gen_id(), 3)");
        await ExecNonQuery(executor, dbname, "INSERT INTO users (id, score) VALUES (gen_id(), 4)");
        Assert.That(await Emails(executor, dbname, "score >= 3"), Has.Count.EqualTo(2));
    }

    [Test]
    public async Task JsonbFieldIndex_IsUsedByTheArrowPredicate()
    {
        (string dbname, CommandExecutor executor) = await CreateUsers();
        await ExecDDL(executor, dbname, "CREATE INDEX users_email ON users ((doc->>'email'))");

        await Insert(executor, dbname, "a@x", "{\"email\": \"a@example.com\"}", 1);
        await Insert(executor, dbname, "b@x", "{\"email\": \"b@example.com\"}", 2);
        await Insert(executor, dbname, "c@x", "{\"name\": \"no email\"}", 3);

        Assert.That(await Emails(executor, dbname, "doc->>'email' = 'a@example.com'"), Is.EqualTo(new[] { "a@x" }));

        (string node, string detail) = await ScanOf(executor, dbname, "SELECT email FROM users WHERE doc->>'email' = 'a@example.com'");
        Assert.That(node, Is.EqualTo("index-range-scan"));
        Assert.That(detail, Does.Contain("users_email"));

        // A literal with another case is another expression: it must not use this index.
        (string other, _) = await ScanOf(executor, dbname, "SELECT email FROM users WHERE doc->>'EMAIL' = 'a@example.com'");
        Assert.That(other, Is.EqualTo("table-scan"));
    }

    [Test]
    public async Task IndexScan_MatchesTableScan_ForEachPredicateShape()
    {
        (string dbname, CommandExecutor executor) = await CreateUsers();

        string[] emails = ["Ann@x.io", "bob@x.io", "CAROL@x.io", "dave@x.io", "Eve@x.io"];
        for (int i = 0; i < emails.Length; i++)
            await Insert(executor, dbname, emails[i], "{}", i);

        string[] predicates =
        [
            "lower(email) = 'carol@x.io'",
            "'carol@x.io' = lower(email)",
            "lower(email) > 'bob@x.io'",
            "lower(email) >= 'bob@x.io' AND lower(email) < 'eve@x.io'",
            "lower(email) BETWEEN 'bob@x.io' AND 'dave@x.io'",
            "lower(email) IN ('ann@x.io', 'eve@x.io', 'zed@x.io')",
            "lower(email) = 'ann@x.io' OR lower(email) = 'dave@x.io'",
            "lower(email) = 'ann@x.io' AND score = 0",
        ];

        Dictionary<string, List<string>> before = [];
        foreach (string predicate in predicates)
            before[predicate] = await Emails(executor, dbname, predicate);

        await ExecDDL(executor, dbname, "CREATE INDEX users_lower_email ON users ((lower(email)))");

        foreach (string predicate in predicates)
        {
            Assert.That(await Emails(executor, dbname, predicate), Is.EqualTo(before[predicate]), predicate);

            (string node, _) = await ScanOf(executor, dbname, "SELECT email FROM users WHERE " + predicate);
            Assert.That(node, Is.Not.EqualTo("table-scan"), predicate);
        }
    }

    [Test]
    public async Task OrderByTheExpression_ReadsTheIndexOrder_WithTheSameRows()
    {
        (string dbname, CommandExecutor executor) = await CreateUsers();

        string[] emails = ["delta@x", "Alpha@x", "charlie@x", "BRAVO@x", "echo@x"];
        for (int i = 0; i < emails.Length; i++)
            await Insert(executor, dbname, emails[i], "{}", i);
        await ExecNonQuery(executor, dbname, "INSERT INTO users (id, score) VALUES (gen_id(), 99)");

        List<string?> sorted = [.. (await ExecSelect(executor, dbname, "SELECT email FROM users ORDER BY lower(email)")).Select(r => r.Row["email"].StrValue)];

        await ExecDDL(executor, dbname, "CREATE INDEX users_lower_email ON users ((lower(email)), score)");

        const string Ordered = "SELECT email FROM users ORDER BY lower(email)";
        List<string?> viaIndex = [.. (await ExecSelect(executor, dbname, Ordered)).Select(r => r.Row["email"].StrValue)];
        Assert.That(viaIndex, Is.EqualTo(sorted));

        bool sortNode = false;
        bool indexScan = false;
        foreach (QueryResultRow row in await ExecSelect(executor, dbname, "EXPLAIN " + Ordered))
        {
            sortNode |= row.Row["node"].StrValue == "sort";
            indexScan |= (row.Row["detail"].StrValue ?? "").Contains("users_lower_email");
        }

        Assert.That(indexScan, Is.True, "ORDER BY the indexed expression reads the index");
        Assert.That(sortNode, Is.False, "the index order makes the sort unnecessary");

        // An equality on the expression fixes the first key, so the index orders by the second one.
        List<QueryResultRow> fixedFirst = await ExecSelect(executor, dbname, "SELECT score FROM users WHERE lower(email) = 'charlie@x' ORDER BY score");
        Assert.That(fixedFirst.Select(r => r.Row["score"].LongValue), Is.EqualTo(new long[] { 2 }));

        // Another expression keeps its sort.
        bool otherSorted = (await ExecSelect(executor, dbname, "EXPLAIN SELECT email FROM users ORDER BY upper(email)"))
            .Exists(r => r.Row["node"].StrValue == "sort");
        Assert.That(otherSorted, Is.True);
    }

    [Test]
    public async Task UpdateAndDelete_KeepTheIndexInStep()
    {
        (string dbname, CommandExecutor executor) = await CreateUsers();
        await ExecDDL(executor, dbname, "CREATE UNIQUE INDEX users_lower_email ON users ((lower(email)))");

        await Insert(executor, dbname, "ann@x.io", "{}", 1);
        await Insert(executor, dbname, "bob@x.io", "{}", 2);

        // A change of case only: the key is the same, and the unique check must not fire.
        await ExecNonQuery(executor, dbname, "UPDATE users SET email = 'ANN@x.io' WHERE score = 1");
        Assert.That(await Emails(executor, dbname, "lower(email) = 'ann@x.io'"), Is.EqualTo(new[] { "ANN@x.io" }));

        // A change of value moves the key.
        await ExecNonQuery(executor, dbname, "UPDATE users SET email = 'zed@x.io' WHERE score = 1");
        Assert.That(await Emails(executor, dbname, "lower(email) = 'ann@x.io'"), Is.Empty);
        Assert.That(await Emails(executor, dbname, "lower(email) = 'zed@x.io'"), Is.EqualTo(new[] { "zed@x.io" }));

        // The old key is free again.
        await Insert(executor, dbname, "ann@x.io", "{}", 3);

        // An update of a column the expression does not read leaves the index alone.
        await ExecNonQuery(executor, dbname, "UPDATE users SET score = 10 WHERE score = 2");
        Assert.That(await Emails(executor, dbname, "lower(email) = 'bob@x.io'"), Is.EqualTo(new[] { "bob@x.io" }));

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(() =>
            ExecNonQuery(executor, dbname, "UPDATE users SET email = 'BOB@X.IO' WHERE score = 3"));
        Assert.That(ex!.Code, Is.EqualTo(CamusDBErrorCodes.DuplicateUniqueKeyValue));

        await ExecNonQuery(executor, dbname, "DELETE FROM users WHERE score = 10");
        Assert.That(await Emails(executor, dbname, "lower(email) = 'bob@x.io'"), Is.Empty);

        // The deleted key is free again.
        await Insert(executor, dbname, "Bob@x.io", "{}", 4);
        Assert.That(await Emails(executor, dbname, "lower(email) = 'bob@x.io'"), Is.EqualTo(new[] { "Bob@x.io" }));
    }

    [Test]
    public async Task Backfill_IndexesTheRowsThatExist()
    {
        (string dbname, CommandExecutor executor) = await CreateUsers();

        for (int i = 0; i < 25; i++)
            await Insert(executor, dbname, $"User{i}@X.io", $"{{\"n\": {i}}}", i);

        await ExecDDL(executor, dbname, "CREATE INDEX users_lower_email ON users ((lower(email)))");

        Assert.That(await Emails(executor, dbname, "lower(email) = 'user7@x.io'"), Is.EqualTo(new[] { "User7@X.io" }));
        Assert.That(await Emails(executor, dbname, "lower(email) IN ('user0@x.io', 'user24@x.io')"), Is.EqualTo(new[] { "User0@X.io", "User24@X.io" }));
    }

    [Test]
    public async Task ExpressionIndex_SurvivesReopenAndRename()
    {
        (string dbname, CommandExecutor executor) = await CreateUsers();
        await ExecDDL(executor, dbname, "CREATE UNIQUE INDEX users_lower_email ON users ((lower(email)))");
        await Insert(executor, dbname, "Ann@x.io", "{}", 1);

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);

        TableIndexSchema persisted = reopened.Schema.Tables["users"].Indexes!.First(ix => ix.Name == "users_lower_email");
        Assert.That(persisted.KeyExpressions, Has.Length.EqualTo(1));
        Assert.That(persisted.ColumnIds![0], Is.EqualTo(IndexKeyExpression.ColumnIdMarker));
        Assert.That(persisted.KeyExpressions![0].Sql, Does.Contain(StoredColumnRef.Prefix));
        Assert.That(persisted.KeyExpressions[0].Type, Is.EqualTo(ColumnType.String));

        Assert.That(await Emails(executor, dbname, "lower(email) = 'ann@x.io'"), Is.EqualTo(new[] { "Ann@x.io" }));

        // The stored text names the column by id, so a rename keeps the index working.
        await ExecDDL(executor, dbname, "ALTER TABLE users RENAME COLUMN email TO mail");
        List<QueryResultRow> rows = await ExecSelect(executor, dbname, "SELECT mail FROM users WHERE lower(mail) = 'ann@x.io'");
        Assert.That(rows.Select(r => r.Row["mail"].StrValue), Is.EqualTo(new[] { "Ann@x.io" }));

        (string node, string detail) = await ScanOf(executor, dbname, "SELECT mail FROM users WHERE lower(mail) = 'ann@x.io'");
        Assert.That(node, Is.EqualTo("index-lookup"));
        Assert.That(detail, Does.Contain("lower(`mail`)"));

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            await ExecNonQuery(executor, dbname, "INSERT INTO users (id, mail) VALUES (gen_id(), 'ANN@X.IO')");
        });
        Assert.That(ex!.Code, Is.EqualTo(CamusDBErrorCodes.DuplicateUniqueKeyValue));
    }

    [Test]
    public async Task DropColumn_ReadByAnExpression_IsRefused()
    {
        (string dbname, CommandExecutor executor) = await CreateUsers();
        await ExecDDL(executor, dbname, "CREATE INDEX users_lower_email ON users ((lower(email)))");

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(() => ExecDDL(executor, dbname, "ALTER TABLE users DROP COLUMN email"));
        Assert.That(ex!.Message, Does.Contain("users_lower_email"));

        await ExecDDL(executor, dbname, "DROP INDEX users_lower_email ON users");
        await ExecDDL(executor, dbname, "ALTER TABLE users DROP COLUMN email");
    }

    [Test]
    public async Task ShowCreateTable_And_ShowIndexes_RoundTrip()
    {
        (string dbname, CommandExecutor executor) = await CreateUsers();
        await ExecDDL(executor, dbname, "CREATE UNIQUE INDEX users_lower_email ON users ((lower(email)))");
        await ExecDDL(executor, dbname, "CREATE INDEX users_mixed ON users (score DESC, (doc->>'email'))");

        string ddl = (await ExecSelect(executor, dbname, "SHOW CREATE TABLE users"))[0].Row["Create Table"].StrValue!;
        Assert.That(ddl, Does.Contain("UNIQUE KEY `users_lower_email` ((lower(`email`)))"));
        Assert.That(ddl, Does.Contain("KEY `users_mixed` (`score` DESC, (jsonb_object_field_text(`doc`, 'email')))"));

        await ExecDDL(executor, dbname, ddl.Replace("`users`", "`users_copy`"));
        TableDescriptor copy = await executor.OpenTable(new OpenTableTicket(dbname, "users_copy"));
        Assert.That(copy.Indexes["users_lower_email"].ExpressionAt(0)!.Sql, Is.EqualTo("lower(`email`)"));
        Assert.That(copy.Indexes["users_mixed"].ExpressionAt(1)!.Sql, Is.EqualTo("jsonb_object_field_text(`doc`, 'email')"));
        Assert.That(copy.Indexes["users_mixed"].DirectionAt(0), Is.EqualTo(OrderType.Descending));

        QueryResultRow shown = (await ExecSelect(executor, dbname, "SHOW INDEXES FROM users")).Find(r => r.Row["Key_name"].StrValue == "users_mixed")!;
        Assert.That(shown.Row["Key"].StrValue, Is.EqualTo("`score` DESC, (jsonb_object_field_text(`doc`, 'email'))"));
        Assert.That(shown.Row["Directions"].StrValue, Is.EqualTo("DESC,ASC"));

        // The Key field is what a dump writes; it creates the same index.
        await ExecDDL(executor, dbname, "CREATE INDEX users_mixed2 ON users (" + shown.Row["Key"].StrValue + ")");
        TableDescriptor users = await executor.OpenTable(new OpenTableTicket(dbname, "users"));
        Assert.That(users.Indexes["users_mixed2"].ExpressionAt(1)!.KeyName, Is.EqualTo(users.Indexes["users_mixed"].ExpressionAt(1)!.KeyName));
    }

    [TestCase("now()", Description = "a clock function")]
    [TestCase("current_date()", Description = "a clock function")]
    [TestCase("random()", Description = "a random function")]
    [TestCase("gen_id()", Description = "a generator")]
    [TestCase("nextval('s')", Description = "a sequence")]
    [TestCase("current_user()", Description = "a session function")]
    [TestCase("count(email)", Description = "an aggregate")]
    [TestCase("(SELECT 1)", Description = "a subquery")]
    [TestCase("row_number() OVER ()", Description = "a window function")]
    [TestCase("lower(@p)", Description = "a parameter")]
    [TestCase("lower(missing)", Description = "an unknown column")]
    [TestCase("doc->'email'", Description = "a jsonb result")]
    [TestCase("json_value(doc, '$.a')", Description = "a type only the value knows")]
    [TestCase("CAST(email AS string(10))", Description = "a sized cast")]
    [TestCase("lower(other.email)", Description = "a column of another relation")]
    public async Task RefusedExpression_ReportsInvalidIndexExpression(string expression)
    {
        (string dbname, CommandExecutor executor) = await CreateUsers();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(() =>
            ExecDDL(executor, dbname, $"CREATE INDEX bad ON users (({expression}))"));
        Assert.That(ex!.Code, Is.EqualTo(CamusDBErrorCodes.InvalidIndexExpression), ex.Message);

        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(dbname, "users"));
        Assert.That(table.AllIndexes.ContainsKey("bad"), Is.False);
    }

    [Test]
    public async Task DescendingStringExpression_IsRefused_AndIntegerExpression_IsAccepted()
    {
        (string dbname, CommandExecutor executor) = await CreateUsers();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(() =>
            ExecDDL(executor, dbname, "CREATE INDEX bad ON users ((lower(email)) DESC)"));
        Assert.That(ex!.Message, Does.Contain("Descending"));

        await ExecDDL(executor, dbname, "CREATE INDEX users_len ON users ((length(email)) DESC)");
        await Insert(executor, dbname, "a@x", "{}", 1);
        await Insert(executor, dbname, "bbbb@x", "{}", 2);

        Assert.That(await Emails(executor, dbname, "length(email) > 4"), Is.EqualTo(new[] { "bbbb@x" }));
        Assert.That(await Emails(executor, dbname, "length(email) = 6"), Is.EqualTo(new[] { "bbbb@x" }));

        (string node, string detail) = await ScanOf(executor, dbname, "SELECT email FROM users WHERE length(email) = 6");
        Assert.That(node, Is.EqualTo("index-range-scan"));
        Assert.That(detail, Does.Contain("users_len"));
    }

    [Test]
    public async Task FunctionCallWithoutExtraParentheses_IsAnExpression()
    {
        (string dbname, CommandExecutor executor) = await CreateUsers();
        await ExecDDL(executor, dbname, "CREATE INDEX users_upper ON users (upper(email))");
        await ExecDDL(executor, dbname, "CREATE INDEX users_plain ON users ((score))");

        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(dbname, "users"));
        Assert.That(table.Indexes["users_upper"].ExpressionAt(0)!.Sql, Is.EqualTo("upper(`email`)"));
        Assert.That(table.Indexes["users_plain"].HasExpressions, Is.False);
        Assert.That(table.Indexes["users_plain"].Columns, Is.EqualTo(new[] { "score" }));
    }

    [Test]
    public async Task OnConflictDoNothing_SeesTheExpressionIndex()
    {
        (string dbname, CommandExecutor executor) = await CreateUsers();
        await ExecDDL(executor, dbname, "CREATE UNIQUE INDEX users_lower_email ON users ((lower(email)))");
        await Insert(executor, dbname, "ann@x.io", "{}", 1);

        await ExecNonQuery(executor, dbname, "INSERT INTO users (id, email, score) VALUES (gen_id(), 'ANN@x.io', 2) ON CONFLICT DO NOTHING");
        await ExecNonQuery(executor, dbname, "INSERT INTO users (id, email, score) VALUES (gen_id(), 'ANN@x.io', 2) ON CONFLICT ON CONSTRAINT users_lower_email DO NOTHING");

        Assert.That(await Emails(executor, dbname, "score >= 0"), Is.EqualTo(new[] { "ann@x.io" }));
    }

    [Test]
    public async Task RolledBackInsert_LeavesNoEntry()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname, "CREATE TABLE users (id oid primary key, email string)");
        await ExecDDL(executor, dbname, "CREATE UNIQUE INDEX users_lower_email ON users ((lower(email)))");

        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, "INSERT INTO users (id, email) VALUES (gen_id(), 'Ann@x.io')", null));
        await db.Transactions.RollbackAsync(tx);

        Assert.That(await Emails(executor, dbname, "lower(email) = 'ann@x.io'"), Is.Empty);
        await ExecNonQuery(executor, dbname, "INSERT INTO users (id, email) VALUES (gen_id(), 'ANN@x.io')");
        Assert.That(await Emails(executor, dbname, "lower(email) = 'ann@x.io'"), Is.EqualTo(new[] { "ANN@x.io" }));
    }

    [Test]
    public async Task ForeignKey_DoesNotTakeAnExpressionIndexAsItsBackingIndex()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname, "CREATE TABLE parents (id int64 primary key)");
        await ExecDDL(executor, dbname, "CREATE TABLE kids (id int64 primary key, parent_id int64, name string)");

        // Leads with the referencing column, so a plain index of this shape would back the key.
        await ExecDDL(executor, dbname, "CREATE INDEX kids_parent_name ON kids (parent_id, (lower(name)))");
        await ExecDDL(executor, dbname, "ALTER TABLE kids ADD CONSTRAINT kids_parent FOREIGN KEY (parent_id) REFERENCES parents (id)");

        await ExecNonQuery(executor, dbname, "INSERT INTO parents (id) VALUES (1)");
        await ExecNonQuery(executor, dbname, "INSERT INTO kids (id, parent_id, name) VALUES (1, 1, 'Ann')");

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(() =>
            ExecNonQuery(executor, dbname, "INSERT INTO kids (id, parent_id, name) VALUES (2, 9, 'Bob')"));
        Assert.That(ex!.Code, Is.Not.EqualTo(CamusDBErrorCodes.InvalidInternalOperation), ex.Message);

        CamusDBException? delete = Assert.ThrowsAsync<CamusDBException>(() => ExecNonQuery(executor, dbname, "DELETE FROM parents WHERE id = 1"));
        Assert.That(delete!.Code, Is.Not.EqualTo(CamusDBErrorCodes.InvalidInternalOperation), delete.Message);

        TableDescriptor kids = await executor.OpenTable(new OpenTableTicket(dbname, "kids"));
        ForeignKeySchema foreignKey = kids.Schema.ForeignKeys!.Single();
        Assert.That(foreignKey.BackingIndexId, Is.Not.EqualTo(kids.Indexes["kids_parent_name"].KvId));
    }

    [Test]
    public async Task MaterializedViewIndex_SurvivesARefresh()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname, "CREATE TABLE people (id int64 primary key, email string)");
        await ExecNonQuery(executor, dbname, "INSERT INTO people (id, email) VALUES (1, 'Ann@x.io'), (2, 'bob@x.io')");

        await ExecDDL(executor, dbname, "CREATE MATERIALIZED VIEW mv_people AS SELECT id, email FROM people");
        await ExecDDL(executor, dbname, "CREATE INDEX mv_people_lower ON mv_people ((lower(email)))");

        await ExecNonQuery(executor, dbname, "INSERT INTO people (id, email) VALUES (3, 'CAROL@x.io')");
        await ExecNonQuery(executor, dbname, "REFRESH MATERIALIZED VIEW mv_people");

        List<QueryResultRow> rows = await ExecSelect(executor, dbname, "SELECT id FROM mv_people WHERE lower(email) = 'carol@x.io'");
        Assert.That(rows.Select(r => r.Row["id"].LongValue), Is.EqualTo(new long[] { 3 }));

        (string node, string detail) = await ScanOf(executor, dbname, "SELECT id FROM mv_people WHERE lower(email) = 'carol@x.io'");
        Assert.That(node, Is.Not.EqualTo("table-scan"));
        Assert.That(detail, Does.Contain("mv_people_lower"));
    }

    [Test]
    public async Task CoveringExpressionIndex_AnswersFromTheIndex()
    {
        (string dbname, CommandExecutor executor) = await CreateUsers();
        await ExecDDL(executor, dbname, "CREATE INDEX users_lower_cover ON users ((lower(email))) INCLUDE (email, score)");
        await Insert(executor, dbname, "Ann@x.io", "{}", 7);
        await Insert(executor, dbname, "bob@x.io", "{}", 8);

        List<QueryResultRow> rows = await ExecSelect(executor, dbname, "SELECT email, score FROM users WHERE lower(email) = 'ann@x.io'");
        Assert.That(rows, Has.Count.EqualTo(1));
        Assert.That(rows[0].Row["email"].StrValue, Is.EqualTo("Ann@x.io"));
        Assert.That(rows[0].Row["score"].LongValue, Is.EqualTo(7));

        long? rowsRead = null;
        foreach (QueryResultRow row in await ExecSelect(executor, dbname, "EXPLAIN (ANALYZE) SELECT email, score FROM users WHERE lower(email) = 'ann@x.io'"))
        {
            if (row.Row["node"].StrValue is "index-range-scan" or "index-lookup")
                rowsRead = row.Row["rows_read"].LongValue;
        }

        Assert.That(rowsRead, Is.EqualTo(0L), "a covered read fetches no primary row");
    }

    [Test]
    public async Task KeywordColumnName_IsQuotedInEveryText()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname, "CREATE TABLE orders (id oid primary key, `order` string)");
        await ExecDDL(executor, dbname, "CREATE INDEX orders_lower ON orders ((lower(`order`)))");
        await ExecNonQuery(executor, dbname, "INSERT INTO orders (id, `order`) VALUES (gen_id(), 'First')");

        List<QueryResultRow> rows = await ExecSelect(executor, dbname, "SELECT `order` FROM orders WHERE lower(`order`) = 'first'");
        Assert.That(rows.Select(r => r.Row["order"].StrValue), Is.EqualTo(new[] { "First" }));

        (string node, string detail) = await ScanOf(executor, dbname, "SELECT `order` FROM orders WHERE lower(`order`) = 'first'");
        Assert.That(node, Is.Not.EqualTo("table-scan"));
        Assert.That(detail, Does.Contain("lower(`order`)"));

        string ddl = (await ExecSelect(executor, dbname, "SHOW CREATE TABLE orders"))[0].Row["Create Table"].StrValue!;
        Assert.That(ddl, Does.Contain("KEY `orders_lower` ((lower(`order`)))"));

        await ExecDDL(executor, dbname, ddl.Replace("`orders`", "`orders_copy`"));
        TableDescriptor copy = await executor.OpenTable(new OpenTableTicket(dbname, "orders_copy"));
        Assert.That(copy.Indexes["orders_lower"].ExpressionAt(0)!.Sql, Is.EqualTo("lower(`order`)"));
    }

    [Test]
    public async Task InlineKeyOfCreateTable_AcceptsAnExpression()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname,
            "CREATE TABLE users (id oid primary key, email string, UNIQUE KEY users_lower_email ((lower(email))))");
        await ExecNonQuery(executor, dbname, "INSERT INTO users (id, email) VALUES (gen_id(), 'Ann@x.io')");

        Assert.That(await Emails(executor, dbname, "lower(email) = 'ann@x.io'"), Is.EqualTo(new[] { "Ann@x.io" }));
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(() =>
            ExecNonQuery(executor, dbname, "INSERT INTO users (id, email) VALUES (gen_id(), 'ann@X.IO')"));
        Assert.That(ex!.Code, Is.EqualTo(CamusDBErrorCodes.DuplicateUniqueKeyValue));
    }
}
