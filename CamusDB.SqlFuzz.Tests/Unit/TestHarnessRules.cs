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
using CamusDB.SqlFuzz.Tests.Campaign;
using CamusDB.SqlFuzz.Tests.Engine;
using CamusDB.SqlFuzz.Tests.Generator;
using CamusDB.SqlFuzz.Tests.Oracles;
using CamusDB.SqlFuzz.Tests.Printing;
using CamusDB.SqlFuzz.Tests.Runner;
using CamusDB.SqlFuzz.Tests.Shrinking;
using CamusDB.Tests.SqlFuzz;

namespace CamusDB.SqlFuzz.Tests.Unit;

/// <summary>
/// The rules of the harness around the oracles: how values and rows compare, how a setup outcome is
/// classified, the time limit of every engine operation, a shrink that leaves an operation running,
/// the replay of read history, and the helpers the printed regression tests call.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestHarnessRules
{
    private FuzzEngine engine = null!;

    [OneTimeSetUp]
    public async Task SetUp() => engine = await FuzzEngine.StartAsync();

    [OneTimeTearDown]
    public async Task TearDown() => await engine.DisposeAsync();

    private static async Task Ok(IStatementExecutor runner, StatementRoute route, string sql)
    {
        StatementOutcome outcome = await runner.ExecuteAsync(route, sql);
        Assert.That(outcome.IsSuccess, Is.True, $"{sql}: {outcome}");
    }

    // -- Values and rows ------------------------------------------------------------------------

    [Test]
    public void DistinctTypesAndDistinctRowsHaveDistinctTexts()
    {
        Assert.That(SqlFuzzValues.Render(new ColumnValue(ColumnType.Float32, 1.0)), Is.Not.EqualTo(SqlFuzzValues.Render(new ColumnValue(ColumnType.Float64, 1.0))));
        Assert.That(SqlFuzzValues.Render(new ColumnValue(ColumnType.Id, "6849f3a1c2e7d50b4f8a91d3")), Is.Not.EqualTo(SqlFuzzValues.Render(new ColumnValue(ColumnType.String, "6849f3a1c2e7d50b4f8a91d3"))));

        // Cell text that holds a separator cannot imitate a cell boundary.
        ColumnValue[] a = [new(ColumnType.String, "a␟s:b"), new(ColumnType.String, "c")];
        ColumnValue[] b = [new(ColumnType.String, "a"), new(ColumnType.String, "b␟s:c")];
        Assert.That(SqlFuzzValues.RenderRow(a), Is.Not.EqualTo(SqlFuzzValues.RenderRow(b)));

        // The documented equivalences stay: -0 equals 0, and NaN equals NaN.
        Assert.That(SqlFuzzValues.Render(new ColumnValue(ColumnType.Float64, -0.0)), Is.EqualTo(SqlFuzzValues.Render(new ColumnValue(ColumnType.Float64, 0.0))));
        Assert.That(SqlFuzzValues.Render(new ColumnValue(ColumnType.Float64, double.NaN)), Is.EqualTo(SqlFuzzValues.Render(new ColumnValue(ColumnType.Float64, -double.NaN))));
    }

    [Test]
    public void TwoReadsWithEqualRowsAndAnotherShapeDiffer()
    {
        Assert.That(SqlFuzzValues.ShapeDifference([("a", ColumnType.Integer64)], [("a", ColumnType.Integer64), ("b", ColumnType.String)]), Is.Not.Null);
        Assert.That(SqlFuzzValues.ShapeDifference([("a", ColumnType.Integer64)], [("x", ColumnType.Float64)]), Is.Not.Null);
        Assert.That(SqlFuzzValues.ShapeDifference([("a", ColumnType.Integer64)], [("x", ColumnType.Integer64)]), Is.Null);
    }

    // -- Setup ----------------------------------------------------------------------------------

    /// <summary>
    /// Every setup outcome is classified. Before the fix the campaign ignored them, so a defect in a
    /// CREATE, a data load or ANALYZE was never a finding, and a failed load left an empty table on
    /// which every relation passes.
    /// </summary>
    [Test]
    public void EverySetupOutcomeIsClassified()
    {
        TableDef table = new("t0", [new ColumnDef("c0", FuzzType.Int64, true, true), new ColumnDef("c1", FuzzType.Int64, false, false)]);
        SetupStatement create = SetupStatement.Of(new CreateTableStmt(table));
        SetupStatement index = SetupStatement.Of(new CreateIndexStmt("t0", new IndexDef("t0_i0", ["c1"], false, [])));
        SetupStatement load = SetupStatement.Of(new InsertStmt("t0", ["c0", "c1"], [[new LiteralExpr(FuzzValue.Of(1L)), new LiteralExpr(FuzzValue.Of(1L))]]));
        List<SetupStatement> setup = [create, index, load, load, load];
        CamusDBException refusal = new(CamusDBErrorCodes.DuplicatePrimaryKey, "a refused load");
        CamusDBException defect = new(CamusDBErrorCodes.InvalidInternalOperation, "an internal error");

        List<SetupOutcome> outcomes =
        [
            new(0, create, StatementOutcome.FromAffected("CREATE TABLE", 0)),
            new(1, index, StatementOutcome.FromException("CREATE INDEX", new CamusDBException(CamusDBErrorCodes.InvalidInput, "refused"))),
            new(2, load, StatementOutcome.FromException("INSERT", refusal)),
            new(3, load, StatementOutcome.FromException("INSERT", defect)),
            new(4, load, StatementOutcome.FromHang("INSERT", TimeSpan.FromSeconds(10))),
        ];

        OracleStats stats = new();
        List<CheckFailure> failures = new();
        bool usable = FuzzCampaign.ClassifySetup(outcomes, setup, stats, failures);

        Assert.That(usable, Is.False, "a refused CREATE INDEX leaves the round without its schema");
        Assert.That(failures.Select(f => f.Result.Signature), Is.EqualTo(new[]
        {
            "setup:ddl-refused:" + CamusDBErrorCodes.InvalidInput,
            $"error:code:{CamusDBErrorCodes.InvalidInternalOperation}:an internal error",
            "error:hang",
        }));
        Assert.That(failures[0].Check, Is.InstanceOf<SetupDdlCheck>());
        Assert.That(failures[0].Setup, Has.Count.EqualTo(1), "a setup finding replays the setup before it");
        Assert.That(stats.SetupRefusals.Values.Sum(), Is.EqualTo(1), "a refused data load is counted");
    }

    // -- Time limits ----------------------------------------------------------------------------

    /// <summary>
    /// Every engine operation has the time limit, not only a statement: BEGIN and an integrity scan
    /// that pass it are hang findings. Before the fix they were awaited without a limit, so one stuck
    /// operation blocked the campaign.
    /// </summary>
    [Test]
    public async Task BeginAndAnIntegrityScanPastTheLimitAreHangFindings()
    {
        FuzzArm db = await engine.CreateDatabaseAsync();
        await Ok(new SqlRunner(db), StatementRoute.Ddl, "CREATE TABLE th (c0 int64 PRIMARY KEY, c1 int64)");

        SqlRunner strict = new(db, TimeSpan.FromTicks(1)) { CancelGrace = TimeSpan.FromSeconds(5) };
        Assert.ThrowsAsync<StatementHangException>(() => strict.CheckIntegrityAsync("th"));
        CheckResult result = await RoundContext.RunGuardedAsync(new IntegrityOnlyCheck(), new CheckContext(new SqlRunner(db, TimeSpan.FromTicks(1)) { CancelGrace = TimeSpan.FromSeconds(5) }));
        Assert.That(result.Signature, Is.EqualTo("error:hang"), result.Detail);

        SqlRunner strictBegin = new(db, TimeSpan.FromTicks(1)) { CancelGrace = TimeSpan.FromSeconds(5) };
        try
        {
            KvTransaction tx = await strictBegin.BeginAsync();
            // A BEGIN can finish before a one-tick limit is checked; then it must roll back cleanly.
            await strictBegin.RollbackAsync(tx);
        }
        catch (StatementHangException hang)
        {
            Assert.That(hang.Operation, Is.EqualTo("BEGIN"));
        }

        await engine.DropDatabaseAsync(db.DatabaseName);
    }

    /// <summary>A check that only runs the integrity scan of <c>th</c>.</summary>
    private sealed class IntegrityOnlyCheck : OracleCheck
    {
        public override OracleKind Oracle => OracleKind.DmlIntegrity;

        public override string Variant => "test:integrity";

        public override Task<CheckResult> RunAsync(CheckContext context) => WriteCheck.IntegrityAsync(context.Engine, ["th"]);

        public override IEnumerable<string> Describe(ISqlPrinter printer) => [];

        public override void WriteTest(TestWriter writer, ISqlPrinter printer) { }
    }

    /// <summary>
    /// A replay that leaves an operation running poisons the engine: the shrinker stops at once, says
    /// so, and does not use the engine again, not even to drop the replay database.
    /// </summary>
    [Test]
    public async Task AShrinkThatLeavesAnOperationRunningStops()
    {
        await using FuzzEngine own = await FuzzEngine.StartAsync();
        Shrinker shrinker = new(own, TimeSpan.FromSeconds(10), executorFactory: arm => new SqlRunner(arm, TimeSpan.FromTicks(1)) { CancelGrace = TimeSpan.Zero });

        TableDef table = new("t0", [new ColumnDef("c0", FuzzType.Int64, true, true), new ColumnDef("c1", FuzzType.Int64, false, false)]);
        Finding finding = new(OracleKind.Tlp, "tlp:where:mismatch", "planted", 1, 0, 1,
            [SetupStatement.Of(new CreateTableStmt(table))],
            new TlpCheck(TlpVariant.Where, new TableRef("t0", "a0"), [new SelectItem(new ColumnExpr("a0", "c0", FuzzType.Int64))],
                new BinaryExpr(BinaryOp.Gt, new ColumnExpr("a0", "c1", FuzzType.Int64), new LiteralExpr(FuzzValue.Of(1L)), FuzzType.Bool)),
            "CamusDB");

        Finding result = await shrinker.ShrinkAsync(finding, TimeSpan.FromSeconds(60));
        Assert.That(shrinker.Poisoned, Is.True);
        Assert.That(shrinker.Attempts, Is.EqualTo(1), "the shrinker stopped after the replay that hung");
        Assert.That(result.Reproduced, Is.False);
        Assert.That(result.Setup, Has.Count.EqualTo(1));
    }

    // -- Read history ---------------------------------------------------------------------------

    /// <summary>
    /// A fault that appears only after an earlier read (a cache that a warm-up query fills) reproduces
    /// from the setup history, because the history holds the read, on the arm it ran on. Without the
    /// read it does not reproduce. Before the fix no read was history, so such a finding could not be
    /// replayed.
    /// </summary>
    [Test]
    public async Task AFaultThatNeedsAWarmUpReadReplaysFromTheHistory()
    {
        TableDef table = new("t0", [new ColumnDef("c0", FuzzType.Int64, true, true), new ColumnDef("c1", FuzzType.Int64, false, false)]);
        ColumnExpr c0 = new("a0", "c0", FuzzType.Int64);
        ColumnExpr c1 = new("a0", "c1", FuzzType.Int64);
        SelectStmt warmUp = new([new SelectItem(c0, "o0")], new TableRef("t0", "a0"), new BinaryExpr(BinaryOp.Eq, c1, new LiteralExpr(FuzzValue.Of(7L)), FuzzType.Bool));
        string warmUpSql = CamusSqlPrinter.Instance.Print(warmUp);

        List<SetupStatement> withoutRead =
        [
            SetupStatement.Of(new CreateTableStmt(table)),
            SetupStatement.Of(new InsertStmt("t0", ["c0", "c1"], [[new LiteralExpr(FuzzValue.Of(1L)), new LiteralExpr(FuzzValue.Of(1L))], [new LiteralExpr(FuzzValue.Of(2L)), new LiteralExpr(FuzzValue.Of(5L))]])),
        ];
        List<SetupStatement> withRead = [.. withoutRead, new SetupStatement(warmUp, StatementRoute.Query) { Arm = "force-merge-join" }];

        // The fault lives on the force-merge-join arm and needs the warm-up read on that arm first.
        List<string> armsThatRanTheRead = new();
        Shrinker shrinker = new(engine, TimeSpan.FromSeconds(10), executorFactory: arm =>
        {
            bool warm = false;
            SqlRunner real = new(arm);
            return new FaultyExecutor(real, (sql, o) =>
            {
                if (sql == warmUpSql)
                {
                    warm = arm.Name == "force-merge-join";
                    armsThatRanTheRead.Add(arm.Name);
                    return null;
                }
                return warm && o.Kind == OutcomeKind.Rows ? FaultyExecutor.DropLastRow(o) : null;
            });
        });

        PlanEquivalenceCheck check = new(new SelectStmt([new SelectItem(c0, "o0")], new TableRef("t0", "a0"), new BinaryExpr(BinaryOp.Gt, c1, new LiteralExpr(FuzzValue.Of(0L)), FuzzType.Bool)),
            null, null, PlanEquivalenceOracle.ArmByName("force-merge-join"));

        CheckResult without = await shrinker.ReplayAsync(withoutRead, check);
        Assert.That(without.Status, Is.EqualTo(CheckStatus.Pass), without.Detail);

        CheckResult with = await shrinker.ReplayAsync(withRead, check);
        Assert.That(with.Status, Is.EqualTo(CheckStatus.Fail), with.Detail);
        Assert.That(armsThatRanTheRead, Is.EqualTo(new[] { "force-merge-join" }), "the recorded read ran on its arm");
    }

    /// <summary>The reads of a check that did not fail join the round's setup; the reads of a failed check do not.</summary>
    [Test]
    public async Task TheReadsOfAPassedCheckJoinTheSetup()
    {
        FuzzArm db = await engine.CreateDatabaseAsync();
        SqlRunner runner = new(db);
        await Ok(runner, StatementRoute.Ddl, "CREATE TABLE t0 (c0 int64 PRIMARY KEY, c1 int64)");
        await Ok(runner, StatementRoute.NonQuery, "INSERT INTO t0 (c0, c1) VALUES (1, 1), (2, 5)");

        StatementGenerator generator = new(new FuzzRandom(1), GeneratorProfile.CamusDB);
        List<SetupStatement> setup = new();
        RoundContext round = new(generator, new CheckContext(runner), setup, new OracleStats(), 1, () => false);
        TlpCheck check = new(TlpVariant.Where, new TableRef("t0", "a0"), [new SelectItem(new ColumnExpr("a0", "c0", FuzzType.Int64))],
            new BinaryExpr(BinaryOp.Gt, new ColumnExpr("a0", "c1", FuzzType.Int64), new LiteralExpr(FuzzValue.Of(1L)), FuzzType.Bool));

        Assert.That((await round.RunAsync(check)).Status, Is.EqualTo(CheckStatus.Pass));
        Assert.That(setup, Has.Count.EqualTo(4), "the whole and the three parts");
        Assert.That(setup.All(s => s.Route == StatementRoute.Query && s.Arm is null));
        await engine.DropDatabaseAsync(db.DatabaseName);
    }

    // -- The helpers of the printed tests -------------------------------------------------------

    /// <summary>
    /// A printed error-classification test keeps the campaign's rule on every route. Before the fix a
    /// write ran through a helper that swallowed every refusal, so an internal code of a write printed a
    /// test that passed. The fault is planted in storage: a stored row with bytes that do not decode.
    /// </summary>
    [Test]
    public async Task APrintedClassificationTestFailsOnADefectOfAWriteAndOfARead()
    {
        FuzzArm db = await engine.CreateDatabaseAsync();
        SqlRunner runner = new(db);
        await Ok(runner, StatementRoute.Ddl, "CREATE TABLE tg (c0 int64 PRIMARY KEY, c1 int64)");
        await Ok(runner, StatementRoute.NonQuery, "INSERT INTO tg (c0, c1) VALUES (1, 1)");

        TableDescriptor table = await db.Executor.OpenTableWithDescriptor(db.Database, new OpenTableTicket(db.DatabaseName, "tg"));
        KvTransaction tx = await db.Database.Transactions.BeginAsync();
        await table.Store.InsertRow(tx, ObjectIdGenerator.Generate(), [0x01, 0x00, 0x00, 0x00, 0xFF, 0xFE, 0xFD]);
        await db.Database.Transactions.CommitAsync(tx);

        SqlFuzzReplay replay = new(db.Executor, db.Database, db.DatabaseName);
        foreach ((string sql, string route, Statement statement) in new (string, string, Statement)[]
        {
            ("SELECT * FROM tg", "query", new SelectStmt([], new TableRef("tg", "a0"))),
            ("UPDATE tg SET c1 = 2 WHERE TRUE", "write", new UpdateStmt("tg", [new Assignment("c1", new LiteralExpr(FuzzValue.Of(2L)))], new LiteralExpr(FuzzValue.Of(true)))),
        })
        {
            CheckResult campaign = await new ErrorCheck(statement).RunAsync(new CheckContext(runner));
            TestContext.Progress.WriteLine($"{route}: {campaign.Signature} {campaign.Detail}");
            Assert.That(campaign.Status, Is.EqualTo(CheckStatus.Fail), $"the campaign must report the {route}");
            string? printed = await CatchToText(() => replay.ClassifyAsync(sql, route));
            Assert.That(printed, Is.Not.Null, $"the printed test must fail on the {route}");
        }
        await engine.DropDatabaseAsync(db.DatabaseName);
    }

    /// <summary>The problem the printed rule reports: its text, or the exception that is not a refusal.</summary>
    private static async Task<string?> CatchToText(Func<Task<string?>> classify)
    {
        try { return await classify(); }
        catch (Exception ex) { return ex.GetType().Name + ": " + ex.Message; }
    }

    /// <summary>
    /// The printed helpers run each statement as the campaign did: ANALYZE as a read in a read-write
    /// transaction, and a setup transaction that goes on after a refused statement, as SetupRunner ran
    /// it. Before the fix ANALYZE went through the write entry point, and a setup transaction threw at the
    /// first refusal.
    /// </summary>
    [Test]
    public async Task ThePrintedHelpersReplayAsTheCampaignRan()
    {
        FuzzArm db = await engine.CreateDatabaseAsync();
        SqlRunner runner = new(db);
        await Ok(runner, StatementRoute.Ddl, "CREATE TABLE tr (c0 int64 PRIMARY KEY, c1 int64, c2 string)");
        await Ok(runner, StatementRoute.NonQuery, "INSERT INTO tr (c0, c1, c2) VALUES (1, 1, 'a'), (2, 2, 'b')");
        SqlFuzzReplay replay = new(db.Executor, db.Database, db.DatabaseName);

        await replay.AssertClassified("ANALYZE tr", "analyze");
        await replay.Transaction(true, "INSERT INTO tr (c0, c1, c2) VALUES (1, 9, 'dup')", "INSERT INTO tr (c0, c1, c2) VALUES (3, 3, 'c')");
        Assert.That(await replay.Rows("SELECT * FROM tr"), Has.Count.EqualTo(2), "the refused statement aborted the transaction");

        // The effect rule of a printed test runs the predictions the campaign ran: the one-row SELECT of
        // each VALUES row, and the UNION ALL of the DO UPDATE values.
        StatementGenerator generator = new(new FuzzRandom(1), GeneratorProfile.CamusDB);
        generator.State.Tables.Add(new TableDef("tr", [new ColumnDef("c0", FuzzType.Int64, true, true), new ColumnDef("c1", FuzzType.Int64, false, false), new ColumnDef("c2", FuzzType.String, false, false)]));
        InsertStmt upsert = new("tr", ["c0", "c1", "c2"],
            [[new LiteralExpr(FuzzValue.Of(2L)), new LiteralExpr(FuzzValue.Of(20L)), new LiteralExpr(FuzzValue.String("x"))],
             [new LiteralExpr(FuzzValue.Of(5L)), new LiteralExpr(FuzzValue.Of(50L)), new LiteralExpr(FuzzValue.String("y"))]],
            new OnConflictClause(["c0"], [new Assignment("c1", new BinaryExpr(BinaryOp.Add, new ColumnExpr(null, "c1", FuzzType.Int64), new ColumnExpr("excluded", "c1", FuzzType.Int64), FuzzType.Int64))]));
        WriteSpec spec = new WriteCheck(upsert, generator.State).Spec(CamusSqlPrinter.Instance);
        TestContext.Progress.WriteLine(TestWriter.WriteSpec(spec));
        await replay.AssertWriteEffect(spec);
        Assert.That(await replay.Rows("SELECT c1 FROM tr WHERE c0 = 2"), Is.EqualTo(new[] { SqlFuzzValues.RenderRow([new ColumnValue(ColumnType.Integer64, 22L)]) }));

        await engine.DropDatabaseAsync(db.DatabaseName);
    }

    /// <summary>A finding is written at once with its own class name, so the original and the reduced test can sit in one folder.</summary>
    [Test]
    public void AnOriginalFindingHasItsOwnClassName()
    {
        TableDef table = new("t0", [new ColumnDef("c0", FuzzType.Int64, true, true)]);
        Finding finding = new(OracleKind.ErrorClassification, "error:hang", "hang", 1, 2, 3,
            [SetupStatement.Of(new CreateTableStmt(table))], new ErrorCheck(new SelectStmt([], new TableRef("t0", "a0"))), "CamusDB")
        { Reproduced = false };
        string original = RegressionTestPrinter.ClassName(finding, original: true);
        Assert.That(original, Is.EqualTo(RegressionTestPrinter.ClassName(finding) + "Original"));
        string printed = RegressionTestPrinter.Print(finding, original: true);
        Assert.That(printed, Does.Contain("class " + original));
        Assert.That(printed, Does.Contain("did not reproduce"));
        Assert.That(printed, Does.Contain("AssertClassified(\"SELECT * FROM t0 AS a0\", \"query\")"));
    }
}
