/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Extensions;
using CamusDB.Core.Extensions.Inline;
using CamusDB.Extensions;

namespace CamusDB.Tests.InlineFunctions;

/// <summary>
/// The pipeline of an inline trigger body with no database: compile, checks, load and fire through
/// <see cref="InlineFunctionRuntime.AcquireTrigger"/> and <see cref="InlineFunctionRuntime.GuardTrigger"/>,
/// against a fake <see cref="ITriggerContext"/> and a fake session. What the body may do, what it may
/// not do, and the budgets with <c>await</c> in the body. Each fire runs on a thread with a 1.5 MB
/// stack, the smallest stack a pool thread of the server has, so a recursion must end as a budget
/// error there and not end the process.
/// </summary>
[TestFixture]
public sealed class TestInlineTriggerBodies
{
    private const int TimeoutMs = 200;

    private static readonly TableColumnSchema[] Columns =
    [
        new("id", "id", ColumnType.Integer64, notNull: false, defaultValue: null),
        new("title", "title", ColumnType.String, notNull: false, defaultValue: null),
        new("slug", "slug", ColumnType.String, notNull: false, defaultValue: null),
        new("price", "price", ColumnType.Integer64, notNull: false, defaultValue: null),
    ];

    private InlineFunctionRuntime runtime = null!;

    [OneTimeSetUp]
    public void CreateRuntime() => runtime = new InlineFunctionRuntime(CamusDBOptions.Default with
    {
        InlineTriggersEnabled = true,
        InlineFunctionCallTimeoutMs = TimeoutMs,
        InlineFunctionAllocationLimitBytes = 4 * 1024 * 1024,
        InlineFunctionArrayLimit = 10_000,
    }, logger: null);

    [OneTimeTearDown]
    public void DisposeRuntime() => runtime.Dispose();

    /// <summary>A trigger source on (id, title, slug, price), with one function <c>slugify(s STRING) RETURNS STRING</c>.</summary>
    private static InlineTriggerSource Source(string body) => new()
    {
        Name = "tg",
        TableName = "posts",
        Source = body,
        Columns = Columns.Select(c => new InlineTriggerColumn(c.Name, c.Type, null)).ToArray(),
        Functions =
        [
            new InlineFunctionDefinition
            {
                Name = "slugify",
                Source = "return s;",
                SourceSha256 = InlineFunctionRuntime.SourceHash("return s;"),
                ReturnType = ColumnType.String,
                Parameters = [new InlineFunctionParameter { Name = "s", Type = ColumnType.String }],
                LanguageVersion = InlineFunctionRuntime.LanguageVersion,
                RuleRevision = InlineFunctionAllowList.Revision,
            },
        ],
    };

    private static Dictionary<string, ColumnValue> Row(long id, string? title, long? price = null) => new(StringComparer.OrdinalIgnoreCase)
    {
        ["id"] = new ColumnValue(ColumnType.Integer64, id),
        ["title"] = title is null ? ColumnValue.Null : new ColumnValue(ColumnType.String, title),
        ["slug"] = ColumnValue.Null,
        ["price"] = price is null ? ColumnValue.Null : new ColumnValue(ColumnType.Integer64, price.Value),
    };

    /// <summary>The error <c>CREATE TRIGGER</c> gives for <paramref name="body"/>.</summary>
    private CamusDBException Refusal(string body) =>
        Assert.Catch<CamusDBException>(() => runtime.ValidateTrigger(Source(body), default))!;

    /// <summary>
    /// Fires <paramref name="body"/> once on a 1.5 MB thread, as the engine does after the bind: the
    /// unit, the guard, and a stub for <c>slugify</c> that prefixes "slug:". Throws what the fire throws.
    /// </summary>
    private void Fire(string body, TriggerTiming timing, TriggerOperation operation,
        Dictionary<string, ColumnValue>? oldRow, Dictionary<string, ColumnValue>? newRow, FakeSession? session = null)
    {
        InlineFunctionUnit unit = runtime.AcquireTrigger(Source(body), "probe.posts.tg");

        try
        {
            if (unit.Trigger is null)
                throw unit.Failure!;

            Func<object?[], object?>[] stubs = [args => "slug:" + (string?)args[0]];
            Func<ITriggerContext, ISqlSession, Task> fire = runtime.GuardTrigger(unit.Trigger, stubs, "tg");

            FakeSession sql = session ?? new FakeSession();
            TriggerRow? oldAccessor = oldRow is null ? null : new TriggerRow(oldRow);
            TriggerRow? newAccessor = newRow is null ? null
                : timing == TriggerTiming.Before ? new TriggerRow(newRow, Columns) : new TriggerRow(newRow);

            FakeContext context = new(timing, operation, oldAccessor, newAccessor, sql);
            Exception? failure = null;

            Thread thread = new(() =>
            {
                try
                {
                    fire(context, sql).GetAwaiter().GetResult();
                }
                catch (Exception ex)
                {
                    failure = ex;
                }
            }, maxStackSize: 1536 * 1024);

            thread.Start();
            thread.Join();

            if (failure is not null)
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(failure).Throw();
        }
        finally
        {
            runtime.Release(unit, "probe.posts.tg");
        }
    }

    private CamusDBException Stopped(string body, string reason, FakeSession? session = null)
    {
        CamusDBException stopped = Assert.Throws<CamusDBException>(
            () => Fire(body, TriggerTiming.Before, TriggerOperation.Insert, null, Row(1, "a"), session))!;

        Assert.AreEqual(CamusDBErrorCodes.InlineFunctionBudgetExceeded, stopped.Code, stopped.Message);
        StringAssert.Contains("Trigger 'tg'", stopped.Message);
        StringAssert.Contains(reason, stopped.Message);
        return stopped;
    }

    // ── accepted bodies ───────────────────────────────────────────────────────

    [Test]
    public void New_IsReadAndWritten_InBefore_AndTheFunctionStubIsAMethod()
    {
        Dictionary<string, ColumnValue> row = Row(1, "Hello", 5);
        Fire("if (NEW!.title is { } t) NEW.slug = slugify(t); NEW.price = NEW.price * 2;", TriggerTiming.Before, TriggerOperation.Insert, null, row);

        Assert.AreEqual("slug:Hello", row["slug"].StrValue);
        Assert.AreEqual(10, row["price"].LongValue);

        Dictionary<string, ColumnValue> cleared = Row(2, "x", 3);
        Fire("NEW!.price = null; NEW.title = null;", TriggerTiming.Before, TriggerOperation.Insert, null, cleared);
        Assert.AreEqual(ColumnType.Null, cleared["price"].Type);
        Assert.AreEqual(ColumnType.Null, cleared["title"].Type);
    }

    [Test]
    public void Old_IsRead_AndDeleteHasNoNew()
    {
        FakeSession sql = new();
        Fire("await Sql.Set(\"t\", OLD!.title).Set(\"isnull\", NEW is null).ExecuteAsync(\"INSERT INTO a (t, n) VALUES (@t, @isnull)\");",
            TriggerTiming.After, TriggerOperation.Delete, Row(1, "gone"), null, sql);

        Assert.AreEqual("INSERT INTO a (t, n) VALUES (@t, @isnull)", sql.Statements.Single());
        Assert.AreEqual("gone", sql.Parameters.Single()["t"]);
        Assert.AreEqual(true, sql.Parameters.Single()["isnull"]);
    }

    [Test]
    public void AThrow_Vetoes()
    {
        ArgumentException vetoed = Assert.Throws<ArgumentException>(() => Fire(
            "if (NEW!.price is < 0) throw new ArgumentException(\"negative\");",
            TriggerTiming.Before, TriggerOperation.Insert, null, Row(1, "a", -1)))!;

        Assert.AreEqual("negative", vetoed.Message);
    }

    [Test]
    public void ASetter_InAfter_Throws()
    {
        InvalidOperationException refused = Assert.Throws<InvalidOperationException>(() => Fire(
            "NEW!.slug = \"x\";", TriggerTiming.After, TriggerOperation.Insert, null, Row(1, "a")))!;

        StringAssert.Contains("only NEW in a BEFORE trigger can be changed", refused.Message);

        Assert.Throws<InvalidOperationException>(() => Fire(
            "OLD!.slug = \"x\";", TriggerTiming.Before, TriggerOperation.Update, Row(1, "a"), Row(1, "b")));
    }

    [Test]
    public void TheSession_IsAwaited_WithTypedParameters_AndQueryRowsAreRead()
    {
        FakeSession sql = new() { QueryRows = [new TriggerRow(Row(9, "found"))] };

        Fire("var rows = await Sql.Set(\"id\", NEW!.id).QueryAsync(\"SELECT title FROM posts WHERE id = @id\");\n" +
             "NEW.slug = rows.Count + \":\" + rows[0].GetString(\"title\");\n" +
             "await Sql.Set(\"when\", DateTime.UtcNow).Set(\"g\", Guid.NewGuid()).Set(\"xs\", new List<long?> { 1, null }).ExecuteAsync(\"UPDATE x SET y = 1\");",
            TriggerTiming.Before, TriggerOperation.Insert, null, Row(1, "a"), sql);

        Assert.AreEqual(2, sql.Statements.Count);
        Assert.AreEqual(1L, sql.Parameters[0]["id"]);
        Assert.IsInstanceOf<DateTime>(sql.Parameters[1]["when"]);
        Assert.IsInstanceOf<Guid>(sql.Parameters[1]["g"]);
        Assert.AreEqual(0, sql.Parameters.Skip(2).Count());
    }

    [Test]
    public void AnAwaitOfALocal_ParsesAsAnAwait()
    {
        FakeSession sql = new() { Yield = true };
        Dictionary<string, ColumnValue> row = Row(1, "a");

        Fire("var pending = Sql.ExecuteAsync(\"DELETE FROM x\");\nvar changed = await pending;\nawait pending;\nNEW!.price = changed;",
            TriggerTiming.Before, TriggerOperation.Insert, null, row, sql);

        Assert.AreEqual(1, row["price"].LongValue);
        Assert.AreEqual(1, sql.Statements.Count);
    }

    [Test]
    public void TheContextApi_AndTheNames()
    {
        Dictionary<string, ColumnValue> row = Row(1, "a");
        Fire("NEW!.slug = $\"{Operation}/{Timing}/{TableName}/{TriggerName}/{Context.New!.GetString(\"title\")}\";",
            TriggerTiming.Before, TriggerOperation.Update, Row(1, "old"), row);

        Assert.AreEqual("Update/Before/posts/tg/a", row["slug"].StrValue);
    }

    [Test]
    public void LocalFunctions_AndLinq_AndArrays()
    {
        Dictionary<string, ColumnValue> row = Row(1, "b a c");
        Fire("string Sorted(string s) => string.Join(\" \", s.Split(' ').OrderBy(x => x));\n" +
             "var parts = new long[3]; parts[0] = 7;\n" +
             "NEW!.slug = Sorted(NEW.title!) + parts.Sum();",
            TriggerTiming.Before, TriggerOperation.Insert, null, row);

        Assert.AreEqual("a b c7", row["slug"].StrValue);
    }

    // ── refusals ──────────────────────────────────────────────────────────────

    [TestCase("await Task.Delay(1);")]
    [TestCase("await Task.Run(() => 1);")]
    [TestCase("var t = Task.CompletedTask; await t;")]
    [TestCase("var t = Sql.ExecuteAsync(\"DELETE FROM x\"); var u = Sql.ExecuteAsync(\"DELETE FROM y\"); await Task.WhenAll(t, u);")]
    [TestCase("Func<Task> f = async () => await Sql.ExecuteAsync(\"DELETE FROM x\"); await f();")]
    [TestCase("var d = new Dictionary<string, object?>();")]
    [TestCase("System.IO.File.Delete(\"x\");")]
    [TestCase("var h = NEW!.GetHashCode();")]
    [TestCase("Environment.Exit(1);")]
    [TestCase("__camus_frame.Suspend();")]
    [TestCase("__camus_functions[0](new object?[] { \"x\" });")]
    [TestCase("var x = Context.Sql;")]
    public void ARefusedBody_IsRefused(string body)
    {
        CamusDBException refused = Refusal(body);
        TestContext.Out.WriteLine(refused.Message);
        Assert.That(refused.Code, Is.EqualTo(CamusDBErrorCodes.InlineFunctionRefused).Or.EqualTo(CamusDBErrorCodes.InlineFunctionCompileFailed), refused.Message);
    }

    [Test]
    public void ABodyThatDoesNotCompile_NamesTheTrigger()
    {
        CamusDBException failed = Refusal("NEW!.no_such_column = 1;");
        Assert.AreEqual(CamusDBErrorCodes.InlineFunctionCompileFailed, failed.Code);
    }

    // ── budgets ───────────────────────────────────────────────────────────────

    [TestCase("while (true) { }")]
    [TestCase("long i = 0; for (;;) i++;")]
    [TestCase("while (true) { try { long.Parse(\"x\"); } catch { } }")]
    public void ALoop_IsStopped(string body) => Stopped(body, "was stopped: one call");

    [TestCase("long Deeper(long x) => Deeper(x + 1) + 1; NEW!.price = Deeper(1);")]
    [TestCase("Func<long, long> deeper = null!; deeper = x => deeper(x + 1) + 1; NEW!.price = deeper(1);")]
    public void ARecursion_IsStopped_OnASmallStack(string body) => Stopped(body, "nested too deeply");

    [Test]
    public void AnAllocationLoop_IsStopped() =>
        Stopped("var l = new List<string>(); while (true) l.Add(new string(new char[1000]));", "inline_function_allocation_limit_bytes");

    [Test]
    public void AnAllocationAfterAnAwait_Counts()
    {
        // The session continues on another thread; the bytes allocated after it still count.
        Stopped("await Sql.ExecuteAsync(\"DELETE FROM x\"); var l = new List<string>(); while (true) l.Add(new string(new char[1000]));",
            "inline_function_allocation_limit_bytes", new FakeSession { Yield = true });
    }

    [Test]
    public void TheTimeOfASessionCall_IsNotCharged()
    {
        // The statement takes longer than the whole budget of the body; the body itself is short.
        FakeSession slow = new() { DelayMs = TimeoutMs * 3 };
        Fire("await Sql.ExecuteAsync(\"DELETE FROM x\"); await Sql.ExecuteAsync(\"DELETE FROM y\");",
            TriggerTiming.After, TriggerOperation.Insert, null, Row(1, "a"), slow);

        Assert.AreEqual(2, slow.Statements.Count);
    }

    [Test]
    public void ALargeArray_IsCheckedBeforeItIsAllocated() =>
        Stopped("var a = new byte[NEW!.id!.Value * 100000];", "inline_function_array_limit");

    // ── fakes ─────────────────────────────────────────────────────────────────

    private sealed class FakeContext(TriggerTiming timing, TriggerOperation operation, IRow? old, IWritableRow? @new, ISqlSession sql) : ITriggerContext
    {
        public string TriggerName => "tg";

        public string TableName => "posts";

        public TriggerTiming Timing { get; } = timing;

        public TriggerOperation Operation { get; } = operation;

        public IRow? Old { get; } = old;

        public IWritableRow? New { get; } = @new;

        public ISqlSession Sql { get; } = sql;

        public CancellationToken CancellationToken => CancellationToken.None;
    }

    private sealed class FakeSession : ISqlSession
    {
        public List<string> Statements { get; } = [];

        public List<IReadOnlyDictionary<string, object?>> Parameters { get; } = [];

        public IReadOnlyList<IRow> QueryRows { get; init; } = [];

        /// <summary>Continue on a pool thread, as a statement that waits for storage does.</summary>
        public bool Yield { get; init; }

        public int DelayMs { get; init; }

        public async Task<long> ExecuteAsync(string sql, IReadOnlyDictionary<string, object?>? parameters = null)
        {
            await Run(sql, parameters);
            return 1;
        }

        public async Task<IReadOnlyList<IRow>> QueryAsync(string sql, IReadOnlyDictionary<string, object?>? parameters = null)
        {
            await Run(sql, parameters);
            return QueryRows;
        }

        private async Task Run(string sql, IReadOnlyDictionary<string, object?>? parameters)
        {
            Statements.Add(sql);

            if (parameters is not null)
                Parameters.Add(parameters);

            if (DelayMs > 0)
                await Task.Delay(DelayMs);
            else if (Yield)
                await Task.Run(() => { });
        }
    }
}
