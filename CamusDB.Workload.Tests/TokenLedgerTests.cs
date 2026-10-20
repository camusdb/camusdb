/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Workload.Metrics;
using CamusDB.Workload.Operations;
using NUnit.Framework;

namespace CamusDB.Workload.Tests;

/// <summary>
/// The token verdict: one row per slot at the generation the journal accounts for, no double spend, the
/// slot index equal to the primary key, and every read-only lookup finding exactly one row. Each failure
/// class must be named on its own, because each points at a different engine path.
/// </summary>
[TestFixture]
public sealed class TokenLedgerTests
{
    private static TokenRow Row(long slot, long gen, string? id = null) => new(id ?? $"s{slot}.g{gen}", slot, gen, "");

    private static IReadOnlyList<string> Ids(IEnumerable<TokenRow> rows) => rows.Select(r => r.Id).ToList();

    private static TokenLedger LedgerWith(long tokens, params (long Slot, long FromGen, TokenMoveOutcome Outcome)[] moves)
    {
        TokenLedger ledger = new(tokens);
        foreach ((long slot, long fromGen, TokenMoveOutcome outcome) in moves)
            ledger.RecordMove(slot, fromGen, TokenMoveKind.Fresh, 1, outcome, null);
        return ledger;
    }

    [Test]
    public void ExactState_Passes()
    {
        TokenLedger ledger = LedgerWith(3,
            (0, 0, TokenMoveOutcome.Committed), (0, 1, TokenMoveOutcome.Committed), (1, 0, TokenMoveOutcome.Committed));
        List<TokenRow> rows = [Row(0, 2), Row(1, 1), Row(2, 0)];

        TokenResult r = ledger.Judge(rows, Ids(rows));

        Assert.That(r.Passed, Is.True, string.Join("\n", r.Failures));
        Assert.That(r.Committed, Is.EqualTo(3));
        Assert.That(r.Rows, Is.EqualTo(3));
    }

    [Test]
    public void IndeterminateMove_MayOrMayNotHaveApplied()
    {
        TokenLedger ledger = LedgerWith(2, (0, 0, TokenMoveOutcome.Indeterminate), (1, 0, TokenMoveOutcome.Indeterminate));
        List<TokenRow> rows = [Row(0, 0), Row(1, 1)];

        Assert.That(ledger.Judge(rows, Ids(rows)).Passed, Is.True);
    }

    [Test]
    public void ResurrectedDelete_ShowsAsADuplicateSlotAndAnExtraRow()
    {
        TokenLedger ledger = LedgerWith(2, (0, 0, TokenMoveOutcome.Committed));
        // The move of slot 0 committed, but the row it deleted is back.
        List<TokenRow> rows = [Row(0, 0), Row(0, 1), Row(1, 0)];

        TokenResult r = ledger.Judge(rows, Ids(rows));

        Assert.That(r.Passed, Is.False);
        Assert.That(r.DuplicateSlots, Is.EqualTo(1));
        Assert.That(r.Failures, Has.Some.Contains("holds 3 row(s), expected exactly 2"));
        Assert.That(r.Failures, Has.Some.Contains("more than one row").And.Contains("s0.g0 gen 0, s0.g1 gen 1"));
    }

    [Test]
    public void LostInsert_ShowsAsAMissingSlot()
    {
        TokenLedger ledger = LedgerWith(2, (1, 0, TokenMoveOutcome.Committed));
        List<TokenRow> rows = [Row(0, 0)];

        TokenResult r = ledger.Judge(rows, Ids(rows));

        Assert.That(r.MissingSlots, Is.EqualTo(1));
        Assert.That(r.Failures, Has.Some.Contains("slot 1 (committed 1, indeterminate 0)"));
    }

    [Test]
    public void LostCommittedMove_IsAGenerationBelowTheJournal()
    {
        TokenLedger ledger = LedgerWith(1, (0, 0, TokenMoveOutcome.Committed), (0, 1, TokenMoveOutcome.Committed));
        List<TokenRow> rows = [Row(0, 1)];

        TokenResult r = ledger.Judge(rows, Ids(rows));

        Assert.That(r.GenBelow, Is.EqualTo(1));
        Assert.That(r.Failures, Has.Some.Contains("gen 1, but 2 move(s) committed (1 lost)"));
    }

    [Test]
    public void FailedMoveThatApplied_IsAGenerationAboveTheCeiling()
    {
        TokenLedger ledger = LedgerWith(1, (0, 0, TokenMoveOutcome.Error));
        List<TokenRow> rows = [Row(0, 1)];

        TokenResult r = ledger.Judge(rows, Ids(rows));

        Assert.That(r.GenAbove, Is.EqualTo(1));
        Assert.That(r.Errors, Is.EqualTo(1));
    }

    [Test]
    public void TwoCommitsFromOneGeneration_AreADoubleSpendEvenWhenTheTableLooksRight()
    {
        TokenLedger ledger = LedgerWith(1, (0, 0, TokenMoveOutcome.Committed), (0, 0, TokenMoveOutcome.Committed));
        List<TokenRow> rows = [Row(0, 2)];

        TokenResult r = ledger.Judge(rows, Ids(rows));

        Assert.That(r.DoubleCommits, Is.EqualTo(1));
        Assert.That(r.Failures, Has.Some.Contains("double spend"));
    }

    [Test]
    public void IndexThatDisagreesWithThePrimaryKey_IsNamed()
    {
        TokenLedger ledger = LedgerWith(2);
        List<TokenRow> rows = [Row(0, 0), Row(1, 0)];

        TokenResult r = ledger.Judge(rows, ["s0.g0", "s0.g0", "s9.g3"]);

        Assert.That(r.IndexMissing, Is.EqualTo(1));
        Assert.That(r.IndexExtra, Is.EqualTo(2));
        Assert.That(r.Failures, Has.Some.Contains("missing from the slot index").And.Contains("s1.g0"));
        Assert.That(r.Failures, Has.Some.Contains("rows the primary key does not hold").And.Contains("s9.g3"));
        Assert.That(r.Failures, Has.Some.Contains("returned 1 row(s) more than once"));
    }

    [Test]
    public void RowOfAnotherRun_IsForeign()
    {
        TokenLedger ledger = LedgerWith(1);
        List<TokenRow> rows = [Row(0, 0), Row(7, 0)];

        TokenResult r = ledger.Judge(rows, Ids(rows));

        Assert.That(r.ForeignRows, Is.EqualTo(1));
        Assert.That(r.Passed, Is.False);
    }

    [Test]
    public void ReadThatDidNotFindExactlyOneRow_FailsTheRun()
    {
        TokenLedger ledger = LedgerWith(1);
        ledger.RecordRead(0, [("s0.g0", 0L)]);
        ledger.RecordRead(0, []);
        ledger.RecordRead(0, [("s0.g0", 0L), ("s0.g1", 1L)]);
        List<TokenRow> rows = [Row(0, 0)];

        TokenResult r = ledger.Judge(rows, Ids(rows));

        Assert.That(r.Reads, Is.EqualTo(3));
        Assert.That(r.ReadViolations, Is.EqualTo(2));
        Assert.That(r.Failures, Has.Some.Contains("2 of 3 read-only slot lookup(s)").And.Contains("s0.g0@0 s0.g1@1"));
    }

    [Test]
    public void Unavailable_FailsWithTheReason()
    {
        TokenResult r = TokenResult.Unavailable("timeout", LedgerWith(5), 5);

        Assert.That(r.Passed, Is.False);
        Assert.That(r.Status, Is.EqualTo("unavailable"));
        Assert.That(r.Failures.Single(), Does.Contain("timeout"));
    }

    [Test]
    public async Task Journal_HasOneLinePerMoveAndReadViolation()
    {
        TokenLedger ledger = LedgerWith(2, (0, 0, TokenMoveOutcome.ConflictRetry), (0, 0, TokenMoveOutcome.Committed));
        ledger.RecordRead(1, []);
        ledger.RecordRead(1, [("s1.g0", 0L)]);
        string dir = Path.Combine(Path.GetTempPath(), $"tokens-{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);
        try
        {
            await ledger.WriteAsync(dir, CancellationToken.None);
            string[] lines = await File.ReadAllLinesAsync(Path.Combine(dir, "token-ledger.csv"));

            Assert.That(lines[0], Is.EqualTo("wall_ms,event,slot,from_gen,kind,attempt,outcome,code"));
            Assert.That(lines.Skip(1).Select(l => l.Split(',')[1]), Is.EqualTo(new[] { "move", "move", "read" }));
            Assert.That(lines[2], Does.Contain(",committed,"));
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Test]
    public void Aim_SpreadsOverEverySlotAndEveryKind()
    {
        HashSet<long> slots = [];
        HashSet<TokenMoveKind> kinds = [];
        for (long i = 0; i < 20_000; i++)
        {
            (long slot, TokenMoveKind kind) = TokenOperation.Aim(i, 100);
            Assert.That(slot, Is.InRange(0, 99));
            slots.Add(slot);
            kinds.Add(kind);
        }

        Assert.That(slots, Has.Count.EqualTo(100));
        Assert.That(kinds, Has.Count.EqualTo(3));
    }

    [Test]
    public void FreshIds_NeverRepeat()
    {
        HashSet<string> ids = [];
        for (int i = 0; i < 1000; i++)
            Assert.That(ids.Add(TokenOperation.FreshId(3, 7)), Is.True);
        Assert.That(TokenOperation.SeedId(3), Is.EqualTo("s3.g0"));
    }
}
