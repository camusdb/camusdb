/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.SqlFuzz.Tests.Engine;
using CamusDB.SqlFuzz.Tests.Generator;
using CamusDB.SqlFuzz.Tests.Oracles;
using CamusDB.SqlFuzz.Tests.Runner;

namespace CamusDB.SqlFuzz.Tests.Unit;

/// <summary>
/// The DML-integrity oracle against planted faults. A write check changes the database, so each test
/// runs it twice on two new databases with the same rows: once on the real engine, where it must pass,
/// and once through a <see cref="FaultyExecutor"/>, where it must report the fault. The table is
/// <c>tf(c0 INT64 key, c1 INT64 with an index, c2 STRING)</c>.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestDmlEffectDetection
{
    private const string Rows = "(1, 1, 'a'), (2, 2, 'b'), (3, NULL, 'c')";

    private FuzzEngine engine = null!;

    [OneTimeSetUp]
    public async Task SetUp() => engine = await FuzzEngine.StartAsync();

    [OneTimeTearDown]
    public async Task TearDown() => await engine.DisposeAsync();

    private static LiteralExpr I(long value) => new(FuzzValue.Of(value));

    private static LiteralExpr S(string value) => new(FuzzValue.String(value));

    private static readonly ColumnExpr C1 = new(null, "c1", FuzzType.Int64);

    private static SchemaState Schema()
    {
        StatementGenerator generator = new(new FuzzRandom(1), GeneratorProfile.CamusDB);
        generator.State.Tables.Add(new TableDef("tf", [new ColumnDef("c0", FuzzType.Int64, true, true), new ColumnDef("c1", FuzzType.Int64, false, false), new ColumnDef("c2", FuzzType.String, false, false)]));
        return generator.State;
    }

    private async Task<(FuzzArm Db, SqlRunner Runner)> FreshAsync(string rows)
    {
        FuzzArm db = await engine.CreateDatabaseAsync();
        SqlRunner runner = new(db);
        Assert.That((await runner.ExecuteAsync(StatementRoute.Ddl, "CREATE TABLE tf (c0 int64 PRIMARY KEY, c1 int64, c2 string)")).IsSuccess);
        Assert.That((await runner.ExecuteAsync(StatementRoute.Ddl, "CREATE INDEX tf_i0 ON tf (c1)")).IsSuccess);
        if (rows.Length > 0)
            Assert.That((await runner.ExecuteAsync(StatementRoute.NonQuery, "INSERT INTO tf (c0, c1, c2) VALUES " + rows)).IsSuccess);
        return (db, runner);
    }

    /// <summary>The check passes on the real engine and gives <paramref name="signature"/> with the fault. A new fault per run, so a fault may keep state.</summary>
    private async Task AssertDetects(Func<OracleCheck> check, Func<Func<string, StatementOutcome, StatementOutcome?>> fault, string signature, string rows = Rows)
    {
        (FuzzArm clean, SqlRunner cleanRunner) = await FreshAsync(rows);
        CheckResult pass = await RoundContext.RunGuardedAsync(check(), new CheckContext(cleanRunner));
        Assert.That(pass.Status, Is.EqualTo(CheckStatus.Pass), "the real engine: " + pass.Detail);
        await engine.DropDatabaseAsync(clean.DatabaseName);

        (FuzzArm faultyDb, SqlRunner faultyRunner) = await FreshAsync(rows);
        FaultyExecutor faulty = new(faultyRunner, fault());
        CheckResult result = await RoundContext.RunGuardedAsync(check(), new CheckContext(faulty));
        Assert.That(faulty.Applied, Is.GreaterThan(0), "the fault never applied, so the test proves nothing");
        Assert.That(result.Status, Is.EqualTo(CheckStatus.Fail), "the planted fault was not reported: " + result.Detail);
        Assert.That(result.Signature, Is.EqualTo(signature), result.Detail);
        await engine.DropDatabaseAsync(faultyDb.DatabaseName);
    }

    /// <summary>A copy of a read where the row with key <paramref name="key"/> gets <paramref name="value"/> in column <paramref name="column"/>.</summary>
    private static StatementOutcome WithCell(StatementOutcome outcome, long key, int column, ColumnValue value) =>
        StatementOutcome.FromRows(outcome.Sql, outcome.Columns, outcome.Rows.Select(r =>
        {
            if (r[0].Type != ColumnType.Integer64 || r[0].LongValue != key)
                return r;
            ColumnValue[] copy = r.ToArray();
            copy[column] = value;
            return copy;
        }).ToList());

    private const string Snapshot = "SELECT * FROM tf";

    /// <summary>A fault that changes the first snapshot taken after the write that <paramref name="isWrite"/> picks.</summary>
    private static Func<Func<string, StatementOutcome, StatementOutcome?>> AfterWrite(Func<string, bool> isWrite, Func<StatementOutcome, StatementOutcome?> change) => () =>
    {
        bool written = false;
        bool done = false;
        return (sql, outcome) =>
        {
            if (isWrite(sql))
            {
                written = true;
                return null;
            }
            if (!written || done || sql != Snapshot || outcome.Kind != OutcomeKind.Rows)
                return null;
            done = true;
            return change(outcome);
        };
    };

    /// <summary>
    /// <c>substring</c> counted UTF-16 units, so these writes stored U+FFFD where the expression in a SELECT
    /// gave half of a pair. The effect rule reads the same expression in a SELECT, so it reported them. A
    /// write now stores the value of that SELECT.
    /// </summary>
    [TestCase("c2", TestName = "ASubstringOfAColumnWithAPairPasses")]
    [TestCase(null, TestName = "ASubstringOfALiteralWithAPairPasses")]
    public async Task ASubstringOverAPairStoresTheValueOfTheSelect(string? column)
    {
        (FuzzArm db, SqlRunner runner) = await FreshAsync("(1, 1, '\U0001F642x'), (2, 2, 'b')");
        Expr subject = column is null ? S("\U0001F642x") : new ColumnExpr(null, column, FuzzType.String);
        Expr call = new FunctionExpr("substring", [subject, I(1), I(1)], FuzzType.String);

        foreach (OracleCheck check in new OracleCheck[]
        {
            new WriteCheck(new UpdateStmt("tf", [new Assignment("c2", call)], new BinaryExpr(BinaryOp.Eq, C1, I(1), FuzzType.Bool)), Schema()),
            new WriteCheck(new InsertStmt("tf", ["c0", "c1", "c2"], [[I(10), I(1), new FunctionExpr("substring", [S("\U0001F642x"), I(1), I(1)], FuzzType.String)]]), Schema()),
        })
        {
            CheckResult result = await RoundContext.RunGuardedAsync(check, new CheckContext(runner));
            Assert.That(result.Status, Is.EqualTo(CheckStatus.Pass), result.Detail);
        }

        StatementOutcome stored = await runner.ExecuteAsync(StatementRoute.Query, "SELECT c2 FROM tf WHERE c0 = 10");
        Assert.That(stored.Rows[0][0].StrValue, Is.EqualTo("\U0001F642"));

        await engine.DropDatabaseAsync(db.DatabaseName);
    }

    [Test]
    public Task AFailedInsertThatKeptARowIsAFinding() =>
        // From an empty table: before the fix, a failed write was checked only against the keys that
        // existed before it, so a row it left behind passed.
        AssertDetects(
            () => new WriteCheck(new InsertStmt("tf", ["c0", "c1", "c2"], [[I(10), I(1), S("x")], [I(11), I(2), S("y")]]), Schema()),
            () => (sql, o) => sql.StartsWith("INSERT", StringComparison.Ordinal) && o.IsSuccess
                ? StatementOutcome.FromException(sql, new CamusDBException(CamusDBErrorCodes.DuplicatePrimaryKey, "planted: the second row failed"))
                : null,
            "dml:effect:insert",
            rows: "");

    [Test]
    public Task AnInsertThatStoredAWrongValueIsAFinding() =>
        AssertDetects(
            () => new WriteCheck(new InsertStmt("tf", ["c0", "c1", "c2"], [[I(10), I(1), S("x")]]), Schema()),
            AfterWrite(sql => sql.StartsWith("INSERT", StringComparison.Ordinal), o => WithCell(o, 10, 2, new ColumnValue(ColumnType.String, "wrong"))),
            "dml:effect:insert");

    [Test]
    public Task AnInsertSelectThatStoredAWrongValueIsAFinding() =>
        AssertDetects(
            () => new WriteCheck(new InsertSelectStmt("tf", ["c0", "c1", "c2"], new SelectStmt(
                [
                    new SelectItem(new BinaryExpr(BinaryOp.Add, new ColumnExpr("s", "c0", FuzzType.Int64), I(100), FuzzType.Int64)),
                    new SelectItem(new ColumnExpr("s", "c1", FuzzType.Int64)),
                    new SelectItem(new ColumnExpr("s", "c2", FuzzType.String)),
                ],
                new TableRef("tf", "s"), new BinaryExpr(BinaryOp.Eq, new ColumnExpr("s", "c0", FuzzType.Int64), I(1), FuzzType.Bool))), Schema()),
            AfterWrite(sql => sql.StartsWith("INSERT", StringComparison.Ordinal), o => WithCell(o, 101, 1, new ColumnValue(ColumnType.Integer64, 999L))),
            "dml:effect:insert-select");

    [Test]
    public Task AnUpsertThatDidNothingForAnAbsentKeyIsAFinding() =>
        AssertDetects(
            () => new WriteCheck(new InsertStmt("tf", ["c0", "c1", "c2"], [[I(1), I(7), S("u")], [I(20), I(8), S("v")]], new OnConflictClause(["c0"], null)), Schema()),
            () =>
            {
                StatementOutcome? before = null;
                bool written = false;
                return (sql, o) =>
                {
                    if (sql == Snapshot && !written && o.Kind == OutcomeKind.Rows)
                    {
                        before ??= o;
                        return null;
                    }
                    if (sql.StartsWith("INSERT", StringComparison.Ordinal))
                    {
                        written = true;
                        return null;
                    }
                    // The table after the UPSERT is the table before it: the statement did nothing.
                    return written && sql == Snapshot && before is not null ? StatementOutcome.FromRows(sql, before.Columns, before.Rows) : null;
                };
            },
            "dml:effect:upsert");

    [Test]
    public Task AnUpsertWithAWrongAffectedCountIsAFinding() =>
        AssertDetects(
            () => new WriteCheck(new InsertStmt("tf", ["c0", "c1", "c2"], [[I(1), I(7), S("u")], [I(20), I(8), S("v")]], new OnConflictClause(["c0"], null)), Schema()),
            () => (sql, o) => sql.StartsWith("INSERT", StringComparison.Ordinal) && o.Kind == OutcomeKind.Affected ? StatementOutcome.FromAffected(sql, 999) : null,
            "dml:effect:upsert");

    [Test]
    public Task AnUpsertThatDidNotApplyItsUpdateIsAFinding() =>
        AssertDetects(
            () => new WriteCheck(new InsertStmt("tf", ["c0", "c1", "c2"], [[I(1), I(7), S("u")]],
                new OnConflictClause(["c0"], [new Assignment("c2", new ColumnExpr("excluded", "c2", FuzzType.String))])), Schema()),
            AfterWrite(sql => sql.StartsWith("INSERT", StringComparison.Ordinal), o => WithCell(o, 1, 2, new ColumnValue(ColumnType.String, "a"))),
            "dml:effect:upsert");

    [Test]
    public Task ADeleteThatAddedARowIsAFinding() =>
        AssertDetects(
            () => new WriteCheck(new DeleteStmt("tf", new BinaryExpr(BinaryOp.Gt, C1, I(1), FuzzType.Bool)), Schema()),
            AfterWrite(sql => sql.StartsWith("DELETE", StringComparison.Ordinal), o => StatementOutcome.FromRows(o.Sql, o.Columns,
                o.Rows.Append([new ColumnValue(ColumnType.Integer64, 99L), new ColumnValue(ColumnType.Integer64, 9L), new ColumnValue(ColumnType.String, "z")]).ToList())),
            "dml:effect:delete");

    [Test]
    public Task AWrongEffectInsideACommittedTransactionIsAFinding() =>
        AssertDetects(
            () => new TransactionCheck(
                [new UpdateStmt("tf", [new Assignment("c2", S("q"))], new BinaryExpr(BinaryOp.Gt, C1, I(1), FuzzType.Bool))],
                null, DeliberateFailure.None, new SelectStmt([], new TableRef("tf", "a0")), ["tf"], Schema()),
            () => (sql, o) => sql.StartsWith("UPDATE", StringComparison.Ordinal) && o.Kind == OutcomeKind.Affected ? StatementOutcome.FromAffected(sql, o.AffectedRows + 1) : null,
            "dml:effect-in-transaction:update");

    [Test]
    public async Task ACommittedTransactionJoinsTheHistoryAndAnAbortedOneDoesNot()
    {
        (FuzzArm db, SqlRunner runner) = await FreshAsync(Rows);
        TransactionCheck committed = new([new InsertStmt("tf", ["c0", "c1", "c2"], [[I(50), I(5), S("t")]])], null, DeliberateFailure.None,
            new SelectStmt([], new TableRef("tf", "a0")), ["tf"], Schema());
        Assert.That((await committed.RunAsync(new CheckContext(runner))).Status, Is.EqualTo(CheckStatus.Pass));
        Assert.That(committed.Committed, Is.True);

        // A generated write that fails by itself aborts the transaction; nothing of it is history.
        TransactionCheck aborted = new([new InsertStmt("tf", ["c0", "c1", "c2"], [[I(1), I(5), S("t")]])], null, DeliberateFailure.None,
            new SelectStmt([], new TableRef("tf", "a0")), ["tf"], Schema());
        CheckResult result = await aborted.RunAsync(new CheckContext(runner));
        Assert.That(result.Status, Is.EqualTo(CheckStatus.Pass), result.Detail);
        Assert.That(aborted.Committed, Is.False);
        await engine.DropDatabaseAsync(db.DatabaseName);
    }

    /// <summary>A corrupt-storage code from an integrity scan is a finding. Before the fix, every <see cref="CamusDBException"/> of a scan was a skip.</summary>
    [TestCase(CamusDBErrorCodes.TableCorrupt)]
    [TestCase(CamusDBErrorCodes.LargeValueCorrupt)]
    [TestCase(CamusDBErrorCodes.InvalidIndexLayout)]
    public async Task ACorruptionCodeFromAnIntegrityScanIsAFinding(string code)
    {
        (FuzzArm db, SqlRunner runner) = await FreshAsync(Rows);
        FaultyExecutor faulty = new(runner, (_, _) => null) { IntegrityFault = _ => new CamusDBException(code, "planted corruption") };
        CheckResult result = await WriteCheck.IntegrityAsync(faulty, ["tf"]);
        Assert.That(result.Status, Is.EqualTo(CheckStatus.Fail), result.Detail);
        Assert.That(result.Signature, Does.StartWith("error:integrity-scan:" + code));

        // A table a shrink step removed is no finding, and no comparison either.
        CheckResult missing = await WriteCheck.IntegrityAsync(runner, ["no_such_table"]);
        Assert.That(missing.Status, Is.EqualTo(CheckStatus.Skip), missing.Detail);
        await engine.DropDatabaseAsync(db.DatabaseName);
    }

    /// <summary>
    /// An index entry whose INCLUDE payload no longer equals its row (an UPDATE that kept the key and
    /// forgot the payload) is an integrity problem. The fault is planted in storage: the entry is
    /// rewritten with the payload of another row, and its key and row id stay right.
    /// </summary>
    [Test]
    public async Task TheAuditReportsAStaleCoveringPayload()
    {
        FuzzArm db = await engine.CreateDatabaseAsync();
        SqlRunner runner = new(db);
        Assert.That((await runner.ExecuteAsync(StatementRoute.Ddl, "CREATE TABLE tc (c0 int64 PRIMARY KEY, c1 int64, c2 string)")).IsSuccess);
        Assert.That((await runner.ExecuteAsync(StatementRoute.Ddl, "CREATE INDEX tc_i0 ON tc (c1) INCLUDE (c2)")).IsSuccess);
        Assert.That((await runner.ExecuteAsync(StatementRoute.NonQuery, "INSERT INTO tc (c0, c1, c2) VALUES (1, 10, 'fresh'), (2, 20, 'other')")).IsSuccess);
        Assert.That(await runner.CheckIntegrityAsync("tc"), Is.Empty, "a table written through SQL is consistent");

        TableDescriptor table = await db.Executor.OpenTableWithDescriptor(db.Database, new OpenTableTicket(db.DatabaseName, "tc"));
        TableIndexSchema index = table.AllIndexes.Values.First(i => i.Name == "tc_i0");
        KvTransaction tx = await db.Database.Transactions.BeginAsync();
        CompositeColumnValue? key = null;
        Core.Util.ObjectIds.ObjectIdValue rowId = default;
        await foreach ((CompositeColumnValue k, Core.Util.ObjectIds.ObjectIdValue id, _) in table.Store.ScanIndex(tx, index.KvId, [ColumnType.Integer64], from: null, to: null, unique: false))
        {
            (key, rowId) = (k, id);
            break;
        }
        Assert.That(key, Is.Not.Null);
        byte[] stale = IndexIncludeValueCodec.EncodeTuple(["c2"], new Dictionary<string, ColumnValue> { ["c2"] = new(ColumnType.String, "stale") });
        await table.Store.PutIndexEntry(tx, index.KvId, key!, rowId, unique: false, includeTuple: stale);
        await db.Database.Transactions.CommitAsync(tx);

        IReadOnlyList<string> problems = await runner.CheckIntegrityAsync("tc");
        TestContext.Progress.WriteLine(string.Join("\n", problems));
        Assert.That(problems, Has.Some.Contains("stale covering payload"));
        await engine.DropDatabaseAsync(db.DatabaseName);
    }

    /// <summary>
    /// A large-value cell of a live row that the stored row no longer points to (an UPDATE from a large
    /// value to a short one that kept the old cell) is an integrity problem. Before the fix the audit
    /// accepted any cell whose row was live. The fault is planted in storage: the row's bytes are
    /// rewritten with a short value, and the old cell stays.
    /// </summary>
    [Test]
    public async Task TheAuditReportsAStaleLargeValueCellOfALiveRow()
    {
        FuzzArm db = await engine.CreateDatabaseAsync();
        SqlRunner runner = new(db);
        Assert.That((await runner.ExecuteAsync(StatementRoute.Ddl, "CREATE TABLE tl (c0 int64 PRIMARY KEY, c1 string)")).IsSuccess);
        Random random = new(7);
        string large = new(Enumerable.Range(0, 8000).Select(_ => (char)('a' + random.Next(26))).ToArray());
        Assert.That((await runner.ExecuteAsync(StatementRoute.NonQuery, $"INSERT INTO tl (c0, c1) VALUES (1, '{large}'), (2, 'short')")).IsSuccess);
        Assert.That(await runner.CheckIntegrityAsync("tl"), Is.Empty, "a table written through SQL is consistent");

        TableDescriptor table = await db.Executor.OpenTableWithDescriptor(db.Database, new OpenTableTicket(db.DatabaseName, "tl"));
        List<QueryResultRow> rows = new();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await db.Executor.ExecuteSQLQuery(new ExecuteSQLTicket(KvTransaction.CreateReadOnly(), db.DatabaseName, "SELECT * FROM tl", null));
        await foreach (QueryResultRow row in cursor)
            rows.Add(new QueryResultRow(row.RowId, row.Row.ToDictionary(kv => kv.Key, kv => kv.Value)));
        QueryResultRow largeRow = rows.First(r => r.Row["c0"].LongValue == 1);
        QueryResultRow shortRow = rows.First(r => r.Row["c0"].LongValue == 2);

        KvTransaction tx = await db.Database.Transactions.BeginAsync();
        Dictionary<string, ColumnValue> rewritten = new(largeRow.Row) { ["c1"] = shortRow.Row["c1"] };
        await table.Store.UpdateRow(tx, largeRow.RowId, RowEncoder.Encode(table.Schema, rewritten, largeRow.RowId));
        await db.Database.Transactions.CommitAsync(tx);

        IReadOnlyList<string> problems = await runner.CheckIntegrityAsync("tl");
        TestContext.Progress.WriteLine(string.Join("\n", problems));
        Assert.That(problems, Has.Some.Contains("stale cell"));
        await engine.DropDatabaseAsync(db.DatabaseName);
    }
}
