/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using CamusDB.SqlFuzz.Tests.Engine;
using CamusDB.SqlFuzz.Tests.Generator;
using CamusDB.SqlFuzz.Tests.Oracles;
using CamusDB.SqlFuzz.Tests.Printing;
using CamusDB.SqlFuzz.Tests.Runner;
using CamusDB.SqlFuzz.Tests.Shrinking;

namespace CamusDB.SqlFuzz.Tests.Unit;

/// <summary>
/// The shrinker reduces a finding to a short statement list and prints a regression test for it. The
/// finding is planted: an executor drops every row that holds the value 777 from the <c>NOT p</c>
/// part of a TLP check, the way a broken access path would. Only the statement that inserts that row,
/// and the table it needs, can keep the finding alive.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestShrinker
{
    [Test]
    public async Task APlantedFindingShrinksToAShortStatementListAndPrintsATest()
    {
        await using FuzzEngine engine = await FuzzEngine.StartAsync();

        IStatementExecutor Faulty(FuzzArm arm) => new FaultyExecutor(new SqlRunner(arm),
            (sql, outcome) => sql.Contains("(NOT ") ? FaultyExecutor.DropRowsWith(outcome, "i:777") : null);
        Shrinker shrinker = new(engine, TimeSpan.FromSeconds(10), executorFactory: Faulty);

        // The first seed whose full history reproduces the planted finding: the planted INSERT comes after
        // the generated writes, so none of them can change its row, but it can still meet a unique key a
        // generated row took. That is a precondition of the test, not what it proves.
        List<SetupStatement> setup = new();
        TlpCheck? check = null;
        TableDef? table = null;
        for (ulong seed = 1; check is null; seed++)
        {
            Assert.That(seed, Is.LessThan(200), "no seed gave a history that reproduces the planted finding");
            StatementGenerator generator = new(new FuzzRandom(seed), GeneratorProfile.CamusDB);
            List<Statement> statements = generator.GenerateSchema();
            statements.AddRange(generator.GenerateData());
            TableDef candidate = generator.State.Tables[0];
            ColumnDef? target = candidate.Columns.FirstOrDefault(c => !c.PrimaryKey && c.Type == FuzzType.Int64);
            if (target is null || statements.Count < 6)
                continue;

            for (int i = 0; i < 20; i++)
                statements.Add(generator.GenerateWrite());
            List<Expr> planted = generator.GenerateRows(candidate, 1, boundaries: false)[0].ToList();
            planted[candidate.Columns.ToList().IndexOf(target)] = new LiteralExpr(FuzzValue.Of(777L));
            statements.Add(new InsertStmt(candidate.Name, candidate.Columns.Select(c => c.Name).ToList(), [planted]));

            List<SetupStatement> candidateSetup = statements.Select(SetupStatement.Of).ToList();
            TlpCheck candidateCheck = new(TlpVariant.Where, new TableRef(candidate.Name, "a0"),
                candidate.Columns.Select(c => new SelectItem(new ColumnExpr("a0", c.Name, c.Type))).ToList(),
                new BinaryExpr(BinaryOp.Lt, new ColumnExpr("a0", target.Name, FuzzType.Int64), new LiteralExpr(FuzzValue.Of(0L)), FuzzType.Bool));
            CheckResult full = await shrinker.ReplayAsync(candidateSetup, candidateCheck);
            if (full.Status != CheckStatus.Fail)
                continue;
            (setup, check, table) = (candidateSetup, candidateCheck, candidate);
        }

        Finding finding = new(OracleKind.Tlp, "tlp:where:mismatch", "planted", 0, 0, 0, setup, check, "CamusDB") { OriginalSetupCount = setup.Count };
        Finding shrunk = await shrinker.ShrinkAsync(finding, TimeSpan.FromSeconds(120));

        TestContext.Progress.WriteLine($"{setup.Count} -> {shrunk.Setup.Count} setup statements in {shrinker.Attempts} replays");
        string printed = RegressionTestPrinter.Print(shrunk);
        TestContext.Progress.WriteLine(printed);

        Assert.That(shrinker.Reproduced, Is.True, "the planted finding did not reproduce");
        Assert.That(shrunk.Setup.Count, Is.LessThanOrEqualTo(10), "the common case shrinks to at most 10 statements");
        Assert.That(shrunk.Setup.Count, Is.LessThan(setup.Count));
        Assert.That(shrunk.Setup.Select(s => CamusSqlPrinter.Instance.Print(s.Statement)), Has.Some.Contains("777"));

        CheckResult replay = await shrinker.ReplayAsync(shrunk.Setup, shrunk.Check);
        Assert.That(replay.Status, Is.EqualTo(CheckStatus.Fail), "the shrunk form must still fail");
        Assert.That(replay.Signature, Is.EqualTo("tlp:where:mismatch"));

        Assert.That(printed, Does.Contain(": BaseTest"));
        Assert.That(printed, Does.Contain("await db.Ddl(\"CREATE TABLE " + table!.Name));
        Assert.That(printed, Does.Contain("await db.AssertPartitionRows("));
    }
}
