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
using CamusDB.Core.Util.ObjectIds;
using CamusDB.SqlFuzz.Tests.Engine;
using CamusDB.SqlFuzz.Tests.Generator;
using CamusDB.SqlFuzz.Tests.Oracles;
using CamusDB.SqlFuzz.Tests.Runner;
using CamusDB.Tests.SqlFuzz;

namespace CamusDB.SqlFuzz.Tests.Unit;

/// <summary>
/// Each oracle passes on the real engine and reports a finding when a planted fault makes two results
/// disagree. Without the second half, an oracle that compares nothing would pass every campaign.
///
/// <para>The table: <c>t0(c0 PK, c1 INT64 with an index, c2 STRING)</c> with the rows (1, 1, 'a'),
/// (2, 2, 'b'), (3, NULL, 'c'), (4, 5, 'b'). The predicate <c>a0.c1 &gt; 1</c> is TRUE for rows 2 and 4,
/// FALSE for row 1 and NULL for row 3, so each TLP part holds at least one row.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestOracleDetection
{
    private FuzzEngine engine = null!;
    private FuzzArm database = null!;
    private SqlRunner runner = null!;

    private static readonly TableRef T0 = new("t0", "a0");
    private static readonly ColumnExpr C0 = new("a0", "c0", FuzzType.Int64);
    private static readonly ColumnExpr C1 = new("a0", "c1", FuzzType.Int64);
    private static readonly ColumnExpr C2 = new("a0", "c2", FuzzType.String);
    private static readonly Expr P = new BinaryExpr(BinaryOp.Gt, C1, new LiteralExpr(FuzzValue.Of(1L)), FuzzType.Bool);

    [OneTimeSetUp]
    public async Task SetUp()
    {
        engine = await FuzzEngine.StartAsync();
        database = await engine.CreateDatabaseAsync();
        runner = new SqlRunner(database);
        await Run(StatementRoute.Ddl, "CREATE TABLE t0 (c0 int64 PRIMARY KEY, c1 int64, c2 string)");
        await Run(StatementRoute.Ddl, "CREATE INDEX t0_i0 ON t0 (c1)");
        await Run(StatementRoute.NonQuery, "INSERT INTO t0 (c0, c1, c2) VALUES (1, 1, 'a'), (2, 2, 'b'), (3, NULL, 'c'), (4, 5, 'b')");
    }

    [OneTimeTearDown]
    public async Task TearDown() => await engine.DisposeAsync();

    private async Task Run(StatementRoute route, string sql)
    {
        StatementOutcome outcome = await runner.ExecuteAsync(route, sql);
        Assert.That(outcome.IsSuccess, Is.True, $"{sql}: {outcome}");
    }

    private static async Task AssertDetects(OracleCheck check, IStatementExecutor real, Func<string, StatementOutcome, StatementOutcome?> fault, string signature)
    {
        CheckResult clean = await check.RunAsync(new CheckContext(real));
        Assert.That(clean.Status, Is.EqualTo(CheckStatus.Pass), $"the real engine: {clean.Detail}");

        FaultyExecutor faulty = new(real, fault);
        CheckResult result = await check.RunAsync(new CheckContext(faulty));
        Assert.That(faulty.Applied, Is.GreaterThan(0), "the fault never applied, so the test proves nothing");
        Assert.That(result.Status, Is.EqualTo(CheckStatus.Fail), "the planted fault was not reported");
        Assert.That(result.Signature, Is.EqualTo(signature));
    }

    // -- TLP ------------------------------------------------------------------------------------

    private static StatementOutcome? LoseRowOfNotPart(string sql, StatementOutcome o) => sql.Contains("(NOT ") ? FaultyExecutor.DropLastRow(o) : null;

    [Test]
    public Task TlpWhereReportsAPartThatLosesARow() =>
        AssertDetects(new TlpCheck(TlpVariant.Where, T0, [new SelectItem(C0), new SelectItem(C2)], P), runner, LoseRowOfNotPart, "tlp:where:mismatch");

    [Test]
    public Task TlpDistinctReportsAPartThatLosesARow() =>
        AssertDetects(new TlpCheck(TlpVariant.Distinct, T0, [new SelectItem(C2)], P), runner, LoseRowOfNotPart, "tlp:distinct:mismatch");

    [Test]
    public Task TlpAggregateCountReportsAPartThatLosesARow() =>
        AssertDetects(new TlpCheck(TlpVariant.Aggregate, T0, [], P, aggregates: [new AggregateExpr(AggregateFunc.CountStar, null, FuzzType.Int64)]),
            runner, LoseRowOfNotPart, "tlp:aggregate:count:mismatch");

    [Test]
    public Task TlpAggregateSumReportsAPartThatLosesARow() =>
        AssertDetects(new TlpCheck(TlpVariant.Aggregate, T0, [], P, aggregates: [new AggregateExpr(AggregateFunc.Sum, C1, FuzzType.Int64)]),
            runner, LoseRowOfNotPart, "tlp:aggregate:sum:mismatch");

    [Test]
    public Task TlpAggregateMinReportsAPartThatLosesARow() =>
        AssertDetects(new TlpCheck(TlpVariant.Aggregate, T0, [], P, aggregates: [new AggregateExpr(AggregateFunc.Min, C1, FuzzType.Int64)]),
            runner, LoseRowOfNotPart, "tlp:aggregate:min:mismatch");

    [Test]
    public Task TlpGroupByReportsAPartThatLosesARow() =>
        AssertDetects(new TlpCheck(TlpVariant.GroupBy, T0, [], P, [C2],
                [new AggregateExpr(AggregateFunc.CountStar, null, FuzzType.Int64), new AggregateExpr(AggregateFunc.Sum, C1, FuzzType.Int64)]),
            runner, LoseRowOfNotPart, "tlp:groupby:mismatch");

    [Test]
    public Task TlpHavingReportsAPartThatLosesARow()
    {
        AggregateExpr count = new(AggregateFunc.CountStar, null, FuzzType.Int64);
        Expr having = new BinaryExpr(BinaryOp.Gt, count, new LiteralExpr(FuzzValue.Of(1L)), FuzzType.Bool);
        return AssertDetects(new TlpCheck(TlpVariant.Having, T0, [], having, [C2], [count]), runner, LoseRowOfNotPart, "tlp:having:mismatch");
    }

    [Test]
    public Task TlpJoinOnReportsAPartThatLosesARow()
    {
        JoinFrom join = new(T0, JoinKind.Inner, new TableRef("t0", "a1"),
            new BinaryExpr(BinaryOp.Eq, C0, new ColumnExpr("a1", "c0", FuzzType.Int64), FuzzType.Bool));
        return AssertDetects(new TlpCheck(TlpVariant.JoinOn, join, [new SelectItem(C0), new SelectItem(new ColumnExpr("a1", "c2", FuzzType.String))], P),
            runner, (sql, o) => sql.Contains("AND (NOT ") ? FaultyExecutor.DropLastRow(o) : null, "tlp:joinon:mismatch");
    }

    /// <summary>
    /// The scale of the float-sum tolerance is <c>SUM(ABS(x))</c>, so it must stay small for ordinary
    /// inputs: a FLOAT64 sum still reports a part that loses a row.
    /// </summary>
    [Test]
    public async Task TlpAggregateFloatSumReportsAPartThatLosesARow()
    {
        FuzzArm db = await engine.CreateDatabaseAsync();
        SqlRunner local = new(db);
        Assert.That((await local.ExecuteAsync(StatementRoute.Ddl, "CREATE TABLE tf (c0 int64 PRIMARY KEY, c1 float64)")).IsSuccess);
        Assert.That((await local.ExecuteAsync(StatementRoute.NonQuery, "INSERT INTO tf (c0, c1) VALUES (1, 1.5), (2, 2.5), (3, NULL), (4, 4.0)")).IsSuccess);

        ColumnExpr c1 = new("a0", "c1", FuzzType.Float64);
        Expr p = new BinaryExpr(BinaryOp.Gt, c1, new LiteralExpr(FuzzValue.Of(2.0)), FuzzType.Bool);
        await AssertDetects(new TlpCheck(TlpVariant.Aggregate, new TableRef("tf", "a0"), [], p, aggregates: [new AggregateExpr(AggregateFunc.Sum, c1, FuzzType.Float64)]),
            local, LoseRowOfNotPart, "tlp:aggregate:sum:mismatch");

        await engine.DropDatabaseAsync(db.DatabaseName);
    }

    /// <summary>
    /// A FLOAT64 sum depends on the order of its additions. Over 1e300, -1e300 and 1.0, one order gives 1
    /// and another gives 0, so the whole and the parts of a partition can differ by far more than a
    /// relative tolerance of the result. The comparison scales the tolerance with <c>SUM(ABS(x))</c>,
    /// and this grouped sum over a cross join (the shrunk form of a campaign finding) passes.
    /// </summary>
    [Test]
    public async Task TlpGroupByFloatSumAcceptsAnotherAdditionOrder()
    {
        FuzzArm db = await engine.CreateDatabaseAsync();
        SqlRunner local = new(db);
        Assert.That((await local.ExecuteAsync(StatementRoute.Ddl, "CREATE TABLE t0 (c0 int64 PRIMARY KEY, c4 float64)")).IsSuccess);
        Assert.That((await local.ExecuteAsync(StatementRoute.Ddl, "CREATE TABLE t1 (c0 int64 PRIMARY KEY, c1 numeric, c2 bool)")).IsSuccess);
        Assert.That((await local.ExecuteAsync(StatementRoute.NonQuery, "INSERT INTO t0 (c0, c4) VALUES (30, 1e300), (33, -1e300), (38, 1.0)")).IsSuccess);
        Assert.That((await local.ExecuteAsync(StatementRoute.NonQuery, "INSERT INTO t1 (c0, c1, c2) VALUES (1, NULL, NULL), (2, NULL, TRUE)")).IsSuccess);

        JoinFrom from = new(new TableRef("t1", "a0"), JoinKind.Cross, new TableRef("t0", "a1"), null);
        TlpCheck check = new(TlpVariant.GroupBy, from, [], new ColumnExpr("a0", "c2", FuzzType.Bool),
            [new ColumnExpr("a0", "c1", FuzzType.Numeric)], [new AggregateExpr(AggregateFunc.Sum, new ColumnExpr("a1", "c4", FuzzType.Float64), FuzzType.Float64)]);
        CheckResult result = await check.RunAsync(new CheckContext(local));
        Assert.That(result.Status, Is.EqualTo(CheckStatus.Pass), result.Detail);

        await engine.DropDatabaseAsync(db.DatabaseName);
    }

    [Test]
    public void TheFloatSumToleranceScalesWithTheAbsoluteSumOfTheInputs()
    {
        // Without the scale, 1 and 2 are different sums.
        Assert.That(SqlFuzzValues.FloatsClose(1.0, 2.0), Is.False);
        // With inputs of size 1e300, an addition order can lose the 1.0 of a part.
        Assert.That(SqlFuzzValues.FloatsClose(1.0, 2.0, 6e300), Is.True);
        // A scale of the same size as the values does not hide a lost row.
        Assert.That(SqlFuzzValues.FloatsClose(8.0, 6.5, 8.0), Is.False);
        // Inputs in the overflow range: the order decides if a partial sum overflows.
        Assert.That(SqlFuzzValues.FloatsClose(double.PositiveInfinity, 1.0, double.PositiveInfinity), Is.True);
        Assert.That(SqlFuzzValues.FloatsClose(double.NaN, 1.0), Is.False);
    }

    /// <summary>
    /// A DISTINCT read that returns a row twice is a finding. Before the fix the whole and the parts
    /// were both folded into sets, so a duplicate disappeared before the comparison.
    /// </summary>
    [Test]
    public Task TlpDistinctReportsAWholeThatReturnsARowTwice() =>
        AssertDetects(new TlpCheck(TlpVariant.Distinct, T0, [new SelectItem(C2)], P), runner,
            (sql, o) => sql.StartsWith("SELECT DISTINCT", StringComparison.Ordinal) && !sql.Contains(" WHERE ", StringComparison.Ordinal) && o.Kind == OutcomeKind.Rows && o.Rows.Count > 0
                ? StatementOutcome.FromRows(sql, o.Columns, o.Rows.Append(o.Rows[0]).ToList())
                : null,
            "tlp:distinct:duplicate");

    // -- NoREC ----------------------------------------------------------------------------------

    [Test]
    public Task NoRecReportsDifferentCounts() =>
        AssertDetects(new NoRecCheck(false, T0, [], P), runner,
            (sql, o) => sql.Contains("COUNT(*)") && sql.Contains(" WHERE ") ? FaultyExecutor.DropLastRow(o) : null, "norec:count:mismatch");

    [Test]
    public Task NoRecReportsDifferentRows() =>
        AssertDetects(new NoRecCheck(true, T0, [new SelectItem(C0), new SelectItem(C2)], P), runner,
            (sql, o) => sql.Contains(" WHERE ") ? FaultyExecutor.DropLastRow(o) : null, "norec:rows:mismatch");

    // -- Plan equivalence -----------------------------------------------------------------------

    /// <summary>
    /// Two plans with equal rows but another output shape (a column type) disagree. Before the fix only
    /// the rows were compared, so two empty results of different shapes were equal.
    /// </summary>
    [Test]
    public Task PlanEquivalenceReportsAHintThatChangesTheOutputShape() =>
        AssertDetects(new PlanEquivalenceCheck(new SelectStmt([new SelectItem(C0)], T0, P), "a0", "t0_i0", null), runner,
            (sql, o) => sql.Contains("FORCE_INDEX", StringComparison.Ordinal) && o.Kind == OutcomeKind.Rows
                ? StatementOutcome.FromRows(sql, o.Columns.Select(c => c with { Type = ColumnType.Float64 }).ToList(), o.Rows)
                : null,
            "plan:hint:index:shape");

    /// <summary>The full-scan baseline runs as a full scan (it is not skipped) and reports a row its flagged form loses.</summary>
    [Test]
    public Task PlanEquivalenceReportsAFullScanBaselineThatDisagrees() =>
        AssertDetects(new PlanEquivalenceCheck(new SelectStmt([new SelectItem(C0), new SelectItem(C2)], T0, P), null, null, null, fullScan: true), runner,
            (sql, o) => sql.Contains("pe_flag", StringComparison.Ordinal) && !sql.StartsWith("EXPLAIN", StringComparison.Ordinal)
                ? StatementOutcome.FromRows(sql, o.Columns, o.Rows.Where(r => !(r[^1].Type == ColumnType.Integer64 && r[^1].LongValue == 1 && r[0].LongValue == 4)).ToList())
                : null,
            "plan:full-scan:mismatch");

    /// <summary>
    /// Each forced-operator arm sets its seam on its own executor: the self-join on the indexed column
    /// takes the forced operator on the arm, and the check of each arm passes against the default plan.
    /// </summary>
    [TestCase("force-merge-join", "merge-join")]
    [TestCase("force-hash-join", "hash-join")]
    [TestCase("force-nested-loop", "nested-loop")]
    [TestCase("force-index-nested-loop", null)]
    public async Task AForcedOperatorArmTakesItsOperator(string armName, string? op)
    {
        ArmSpec spec = PlanEquivalenceOracle.ArmByName(armName)!;
        List<FuzzArm> arms = new();
        CheckContext context = new(runner, async s =>
        {
            FuzzArm arm = await engine.CreateArmAsync(database.DatabaseName, s.Name, s.Configure, s.Prepare);
            arms.Add(arm);
            return new SqlRunner(arm);
        });
        try
        {
            JoinFrom join = new(T0, JoinKind.Inner, new TableRef("t0", "a1"),
                new BinaryExpr(BinaryOp.Eq, C1, new ColumnExpr("a1", "c1", FuzzType.Int64), FuzzType.Bool));
            SelectStmt query = new([new SelectItem(C0), new SelectItem(new ColumnExpr("a1", "c0", FuzzType.Int64))], join);

            string plan = await context.ExplainAsync(await context.ArmAsync(spec), query);
            TestContext.Progress.WriteLine(armName + ":\n" + plan);
            if (op is not null)
                Assert.That(plan, Does.Contain(op));

            CheckResult result = await new PlanEquivalenceCheck(query, null, null, spec).RunAsync(context);
            Assert.That(result.Status, Is.EqualTo(CheckStatus.Pass), result.Detail);
        }
        finally
        {
            foreach (FuzzArm arm in arms)
                await FuzzEngine.ReleaseArmAsync(arm);
        }
    }

    [Test]
    public Task PlanEquivalenceReportsAHintThatChangesTheRows() =>
        AssertDetects(new PlanEquivalenceCheck(new SelectStmt([new SelectItem(C0)], T0, P), "a0", "t0_i0", null), runner,
            (sql, o) => sql.Contains("FORCE_INDEX") ? FaultyExecutor.DropLastRow(o) : null, "plan:hint:index:mismatch");

    [Test]
    public async Task PlanEquivalenceReportsAnArmThatChangesTheRows()
    {
        ArmSpec spec = PlanEquivalenceOracle.Arms[0];
        PlanEquivalenceCheck check = new(new SelectStmt([new SelectItem(C0)], T0, P), null, null, spec);
        List<FuzzArm> arms = new();
        try
        {
            async Task<IStatementExecutor> Factory(ArmSpec s, bool faulty)
            {
                FuzzArm arm = await engine.CreateArmAsync(database.DatabaseName, s.Name, s.Configure);
                arms.Add(arm);
                SqlRunner armRunner = new(arm);
                return faulty ? new FaultyExecutor(armRunner, (_, o) => FaultyExecutor.DropLastRow(o)) : armRunner;
            }

            CheckResult clean = await check.RunAsync(new CheckContext(runner, s => Factory(s, false)));
            Assert.That(clean.Status, Is.EqualTo(CheckStatus.Pass), clean.Detail);

            CheckResult result = await check.RunAsync(new CheckContext(runner, s => Factory(s, true)));
            Assert.That(result.Status, Is.EqualTo(CheckStatus.Fail));
            Assert.That(result.Signature, Is.EqualTo("plan:arm:" + spec.Name + ":mismatch"));
        }
        finally
        {
            foreach (FuzzArm arm in arms)
                await FuzzEngine.ReleaseArmAsync(arm);
        }
    }

    // -- Error classification -------------------------------------------------------------------

    [Test]
    public Task ErrorOracleReportsARawException() =>
        AssertDetects(new ErrorCheck(new SelectStmt([new SelectItem(C0)], T0, P)), runner,
            (sql, _) => StatementOutcome.FromException(sql, new OverflowException("Arithmetic operation resulted in an overflow.")),
            "error:unclassified:OverflowException:Arithmetic operation resulted in an overflow.");

    [Test]
    public Task ErrorOracleReportsAHang() =>
        AssertDetects(new ErrorCheck(new SelectStmt([new SelectItem(C0)], T0, P)), runner,
            (sql, _) => StatementOutcome.FromHang(sql, TimeSpan.FromSeconds(10)), "error:hang");

    [Test]
    public Task ErrorOracleReportsAnInternalErrorCode() =>
        AssertDetects(new ErrorCheck(new SelectStmt([new SelectItem(C0)], T0, P)), runner,
            (sql, _) => StatementOutcome.FromException(sql, new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "planted")),
            "error:code:" + CamusDBErrorCodes.InvalidInternalOperation + ":planted");

    [Test]
    public void ErrorOracleAcceptsAnExpectedError()
    {
        StatementOutcome divide = StatementOutcome.FromException("SELECT 1 / 0", new CamusDBException(CamusDBErrorCodes.InvalidInput, "Division by zero"));
        Assert.That(ErrorClassifier.Classify(divide), Is.EqualTo(ErrorVerdict.Accepted));
        StatementOutcome unlisted = StatementOutcome.FromException("SELECT x", new CamusDBException(CamusDBErrorCodes.UnknownColumn, "Unknown column: x"));
        Assert.That(ErrorClassifier.Classify(unlisted), Is.EqualTo(ErrorVerdict.UnlistedCode));
    }

    [Test]
    public async Task TheRunnerTurnsAStatementPastItsTimeLimitIntoAHang()
    {
        // A three-way cross join of the table reads 64 rows; a time limit of one tick passes before the
        // statement can finish, so the runner must report a hang, whether or not the cancellation stops it.
        SqlRunner strict = new(database, TimeSpan.FromTicks(1)) { CancelGrace = TimeSpan.FromSeconds(5) };
        StatementOutcome outcome = await strict.ExecuteAsync(StatementRoute.Query,
            "SELECT a0.c0, a1.c0, a2.c0 FROM t0 AS a0 CROSS JOIN t0 AS a1 CROSS JOIN t0 AS a2");
        Assert.That(outcome.Kind, Is.EqualTo(OutcomeKind.Hang));
        Assert.That(ErrorClassifier.Classify(outcome), Is.EqualTo(ErrorVerdict.Finding));
    }

    // -- DML integrity --------------------------------------------------------------------------

    /// <summary>The schema state of the fixture's table <c>t0</c>, for the checks that need its key and column types.</summary>
    private static SchemaState FixtureSchema()
    {
        StatementGenerator generator = new(new FuzzRandom(1), GeneratorProfile.CamusDB);
        generator.State.Tables.Add(new TableDef("t0", [new ColumnDef("c0", FuzzType.Int64, true, true), new ColumnDef("c1", FuzzType.Int64, false, false), new ColumnDef("c2", FuzzType.String, false, false)]));
        return generator.State;
    }

    [Test]
    public async Task DmlIntegrityReportsAWrongAffectedRowCount()
    {
        SchemaState schema = FixtureSchema();
        UpdateStmt update = new("t0", [new Assignment("c2", new LiteralExpr(FuzzValue.String("b")))],
            new BinaryExpr(BinaryOp.Eq, new ColumnExpr(null, "c2", FuzzType.String), new LiteralExpr(FuzzValue.String("b")), FuzzType.Bool));

        await AssertDetects(new WriteCheck(update, schema), runner,
            (sql, o) => sql.StartsWith("UPDATE", StringComparison.Ordinal) && o.Kind == OutcomeKind.Affected ? StatementOutcome.FromAffected(sql, o.AffectedRows + 1) : null,
            "dml:effect:update");
    }

    [Test]
    public async Task DmlIntegrityReportsAStatementThatAFailedTransactionDidNotRefuse()
    {
        InsertStmt insert = new("t0", ["c0", "c1", "c2"], [[new LiteralExpr(FuzzValue.Of(900L)), new LiteralExpr(FuzzValue.Of(1L)), new LiteralExpr(FuzzValue.String("z"))]]);
        SelectStmt after = new([], T0);
        TransactionCheck check = new([insert], insert, DeliberateFailure.DuplicateKey, after, ["t0"], FixtureSchema());

        await AssertDetects(check, runner,
            (sql, o) => sql.StartsWith("SELECT * FROM t0 AS a0", StringComparison.Ordinal) && o.Kind == OutcomeKind.Error
                ? StatementOutcome.FromRows(sql, [], [])
                : null,
            "dml:after-failure-not-refused");
    }

    [Test]
    public async Task TheIntegrityCheckerReportsATwinRowAndAnOrphanIndexEntry()
    {
        FuzzArm db = await engine.CreateDatabaseAsync();
        SqlRunner local = new(db);
        Assert.That((await local.ExecuteAsync(StatementRoute.Ddl, "CREATE TABLE tw (c0 int64 PRIMARY KEY, c1 int64)")).IsSuccess);
        Assert.That((await local.ExecuteAsync(StatementRoute.Ddl, "CREATE INDEX tw_i0 ON tw (c1)")).IsSuccess);
        Assert.That((await local.ExecuteAsync(StatementRoute.NonQuery, "INSERT INTO tw (c0, c1) VALUES (1, 10), (2, 20)")).IsSuccess);
        Assert.That(await local.CheckIntegrityAsync("tw"), Is.Empty, "a table written through SQL is consistent");

        TableDescriptor table = await db.Executor.OpenTableWithDescriptor(db.Database, new OpenTableTicket(db.DatabaseName, "tw"));
        List<QueryResultRow> rows = new();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await db.Executor.ExecuteSQLQuery(new ExecuteSQLTicket(KvTransaction.CreateReadOnly(), db.DatabaseName, "SELECT * FROM tw", null));
        await foreach (QueryResultRow row in cursor)
            rows.Add(new QueryResultRow(row.RowId, row.Row.ToDictionary(kv => kv.Key, kv => kv.Value)));

        KvTransaction tx = await db.Database.Transactions.BeginAsync();
        // A twin of row 1: a second stored row with the same key and no index entry.
        ObjectIdValue twin = ObjectIdGenerator.Generate();
        await table.Store.InsertRow(tx, twin, RowEncoder.Encode(table.Schema, rows[0].Row, twin));
        // Row 2 loses its stored row but keeps its index entries: they become orphans.
        await table.Store.DeleteRow(tx, rows[1].RowId);
        await db.Database.Transactions.CommitAsync(tx);

        IReadOnlyList<string> problems = await local.CheckIntegrityAsync("tw");
        TestContext.Progress.WriteLine(string.Join("\n", problems));
        Assert.That(problems, Has.Some.Contains("twin"));
        Assert.That(problems, Has.Some.Contains("orphan entry"));
        Assert.That(problems, Has.Some.Contains("has 0 entries"));

        await engine.DropDatabaseAsync(db.DatabaseName);
    }
}
