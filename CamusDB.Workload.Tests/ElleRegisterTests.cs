/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using CamusDB.Workload.Elle;
using CamusDB.Workload.Operations;
using NUnit.Framework;

namespace CamusDB.Workload.Tests;

/// <summary>
/// The rw-register and predicate generators, the EDN they produce, and the end-of-run judge. The
/// generators must meet the checker's assumptions (unique values, nil as the initial state, ordered
/// steps), and the judge must fail exactly the final states the model forbids.
/// </summary>
[TestFixture]
public sealed class ElleRegisterTests
{
    private static string Edn(ElleMicroOp op)
    {
        StringBuilder sb = new();
        op.WriteEdn(sb);
        return sb.ToString();
    }

    [Test]
    public void RegisterStepsWriteTheRwRegisterForms()
    {
        Assert.That(Edn(ElleMicroOp.Write(3, 17)), Is.EqualTo("[:w 3 17]"));
        Assert.That(Edn(ElleMicroOp.RegisterReadOf(3)), Is.EqualTo("[:r 3 nil]"));
        Assert.That(Edn(ElleMicroOp.RegisterReadOf(3).WithRegisterRead(17)), Is.EqualTo("[:r 3 17]"));
        Assert.That(Edn(ElleMicroOp.RegisterReadOf(3).WithRegisterRead(null)), Is.EqualTo("[:r 3 nil]"));
        Assert.That(ElleMicroOp.Write(3, 17).IsMutation, Is.True);
        Assert.That(ElleMicroOp.RegisterReadOf(3).IsRead, Is.True);
    }

    [Test]
    public void ListStepsStillWriteTheListAppendForms()
    {
        Assert.That(Edn(ElleMicroOp.Append(3, 17)), Is.EqualTo("[:append 3 17]"));
        Assert.That(Edn(ElleMicroOp.ReadOf(3)), Is.EqualTo("[:r 3 nil]"));
        Assert.That(Edn(ElleMicroOp.ReadOf(3).WithRead([1L, 2L])), Is.EqualTo("[:r 3 [1 2]]"));
    }

    [Test]
    public void RegisterValuesAreUniqueAcrossTheRunAndReadWriteTxnsEndWithAWrite()
    {
        RegisterKeySpace keys = new(seed: 7, activeKeys: 4, maxWritesPerKey: 8, maxTxnLength: 4);
        HashSet<long> values = [];
        int writes = 0;
        for (int i = 0; i < 2000; i++)
        {
            ElleMicroOp[] txn = keys.NextTxn(readOnly: false);
            Assert.That(txn, Has.Length.InRange(1, 4));
            if (txn.Length > 1)
                Assert.That(txn[^1].IsWrite, Is.True, "a multi-step read-write transaction ends with a write");
            foreach (ElleMicroOp op in txn)
            {
                Assert.That(op.Kind, Is.EqualTo(op.IsWrite ? ElleMicroOpKind.Write : ElleMicroOpKind.RegisterRead));
                if (op.IsWrite)
                {
                    Assert.That(values.Add(op.Value), Is.True, $"value {op.Value} was handed out twice");
                    writes++;
                }
            }
        }
        Assert.That(writes, Is.GreaterThan(1000));
        Assert.That(keys.KeysUsed, Is.GreaterThan(4), "slots rotate to fresh keys after their writes");

        foreach (ElleMicroOp op in keys.NextTxn(readOnly: true))
            Assert.That(op.IsRead, Is.True);
    }

    [Test]
    public void RegisterGeneratorIsDeterministicForASeed()
    {
        RegisterKeySpace a = new(seed: 99, activeKeys: 5, maxWritesPerKey: 4, maxTxnLength: 3);
        RegisterKeySpace b = new(seed: 99, activeKeys: 5, maxWritesPerKey: 4, maxTxnLength: 3);
        for (int i = 0; i < 200; i++)
            Assert.That(a.NextTxn(i % 3 == 0), Is.EqualTo(b.NextTxn(i % 3 == 0)));
    }

    [Test]
    public void ExponentialDistributionMakesTheFirstSlotTheHottest()
    {
        AppendKeySpace uniform = new(seed: 5, activeKeys: 8, maxWritesPerKey: 1_000_000, maxTxnLength: 1, KeyDistribution.Uniform);
        AppendKeySpace skewed = new(seed: 5, activeKeys: 8, maxWritesPerKey: 1_000_000, maxTxnLength: 1, KeyDistribution.Exponential);

        long[] uniformHits = new long[8];
        long[] skewedHits = new long[8];
        for (int i = 0; i < 20_000; i++)
        {
            uniformHits[uniform.NextTxn(true)[0].Key]++;
            skewedHits[skewed.NextTxn(true)[0].Key]++;
        }

        // Uniform: every slot near 1/8. Exponential: slot 0 near 1/2, slot 1 near 1/4, and so on.
        foreach (long hits in uniformHits)
            Assert.That(hits, Is.InRange(2000, 3000));
        Assert.That(skewedHits[0], Is.InRange(9000, 11000));
        Assert.That(skewedHits[1], Is.InRange(4000, 6000));
        Assert.That(skewedHits[2], Is.InRange(1800, 3200));
        for (int i = 1; i < 8; i++)
            Assert.That(skewedHits[i], Is.LessThan(skewedHits[i - 1]), $"slot {i} must be colder than slot {i - 1}");
    }

    [Test]
    public void PredicateGroupReadExpandsToEveryKeyOfTheGroup()
    {
        PredicateKeySpace groups = new(seed: 3, activeGroups: 2, groupSize: 4, maxWritesPerGroup: 6, maxTxnLength: 3);
        PredicateStep[] steps = [PredicateStep.GroupRead(5), PredicateStep.Write(5, 21, 900)];
        ElleMicroOp[] ops = groups.ToMicroOps(steps);

        Assert.That(ops, Has.Length.EqualTo(5));
        Assert.That(ops.Take(4).Select(o => o.Key), Is.EqualTo(new long[] { 20, 21, 22, 23 }));
        Assert.That(ops.Take(4).All(o => o.Kind == ElleMicroOpKind.RegisterRead && o.RegisterValue is null), Is.True);
        Assert.That(Edn(ops[4]), Is.EqualTo("[:w 21 900]"));
        Assert.That(groups.GroupOf(21), Is.EqualTo(5));
        Assert.That(groups.FirstKeyOf(5), Is.EqualTo(20));
        Assert.That(groups.LastKeyOf(5), Is.EqualTo(23));
    }

    [Test]
    public void PredicateWritesStayInsideTheirGroupAndValuesAreUnique()
    {
        PredicateKeySpace groups = new(seed: 11, activeGroups: 3, groupSize: 8, maxWritesPerGroup: 12, maxTxnLength: 3);
        HashSet<long> values = [];
        for (int i = 0; i < 1000; i++)
        {
            foreach (PredicateStep step in groups.NextTxn(readOnly: false))
            {
                if (step.IsGroupRead)
                    continue;
                Assert.That(groups.GroupOf(step.Key), Is.EqualTo(step.Group));
                Assert.That(values.Add(step.Value), Is.True);
            }
        }
        Assert.That(groups.GroupsUsed, Is.GreaterThan(3));
    }

    private static ElleWriteLedger LedgerWith(params (long Key, long Value, ElleOutcome Outcome, long Invoke, long Complete)[] writes)
    {
        ElleWriteLedger ledger = new(ElleModel.RwRegister);
        foreach ((long key, long value, ElleOutcome outcome, long invoke, long complete) in writes)
        {
            ElleMicroOp[] txn = [ElleMicroOp.Write(key, value)];
            switch (outcome)
            {
                case ElleOutcome.Ok: ledger.RecordOk(txn, invoke, complete); break;
                case ElleOutcome.Info: ledger.RecordInfo(txn); break;
                default: ledger.RecordFail(txn); break;
            }
        }
        return ledger;
    }

    private static FinalRegister Final(long key, long? value, params FinalAlternate[] alternates)
        => new(key, value, value is null ? 0 : 1, alternates);

    [Test]
    public void RegisterJudgePassesAnAcknowledgedOrUnresolvedFinalValue()
    {
        ElleWriteLedger ledger = LedgerWith((1, 10, ElleOutcome.Ok, 0, 5), (1, 11, ElleOutcome.Info, 6, 0), (2, 20, ElleOutcome.Fail, 0, 1));

        QuiescentResult ok = ledger.JudgeRegisters([Final(1, 10, new FinalAlternate("index", 10, 1)), Final(2, null)]);
        Assert.That(ok.Passed, Is.True, string.Join("; ", ok.Failures));
        Assert.That(ok.RowsPresent, Is.EqualTo(1));
        Assert.That(ok.KeysRead, Is.EqualTo(2));

        QuiescentResult unresolved = ledger.JudgeRegisters([Final(1, 11), Final(2, null)]);
        Assert.That(unresolved.Passed, Is.True, "an unresolved write may have landed");
    }

    [Test]
    public void RegisterJudgeFailsLostAbortedAndGarbageFinals()
    {
        ElleWriteLedger ledger = LedgerWith((1, 10, ElleOutcome.Ok, 0, 5), (2, 20, ElleOutcome.Fail, 0, 1), (3, 30, ElleOutcome.Ok, 0, 1));

        QuiescentResult lost = ledger.JudgeRegisters([Final(1, null), Final(2, null), Final(3, 30)]);
        Assert.That(lost.Passed, Is.False);
        Assert.That(lost.LostWrites, Is.EqualTo(1));

        QuiescentResult aborted = ledger.JudgeRegisters([Final(1, 10), Final(2, 20), Final(3, 30)]);
        Assert.That(aborted.Passed, Is.False);
        Assert.That(aborted.AbortedVisible, Is.EqualTo(1));

        QuiescentResult garbage = ledger.JudgeRegisters([Final(1, 10), Final(2, null), Final(3, 31)]);
        Assert.That(garbage.Passed, Is.False);
        Assert.That(garbage.GarbageValues, Is.EqualTo(1));

        QuiescentResult unknownKey = ledger.JudgeRegisters([Final(1, 10), Final(2, null), Final(3, 30), Final(4, 40)]);
        Assert.That(unknownKey.Passed, Is.False);
        Assert.That(unknownKey.GarbageValues, Is.EqualTo(1));
    }

    [Test]
    public void RegisterJudgeFailsPathMismatchesAndDuplicateRows()
    {
        ElleWriteLedger ledger = LedgerWith((1, 10, ElleOutcome.Ok, 0, 5));

        QuiescentResult mismatch = ledger.JudgeRegisters([Final(1, 10, new FinalAlternate("index", null, 0))]);
        Assert.That(mismatch.Passed, Is.False);
        Assert.That(mismatch.PathMismatches, Is.EqualTo(1));

        QuiescentResult duplicate = ledger.JudgeRegisters([new FinalRegister(1, 10, 2, [])]);
        Assert.That(duplicate.Passed, Is.False);
        Assert.That(duplicate.DuplicateRows, Is.EqualTo(1));
    }

    [Test]
    public void RegisterJudgeCountsButDoesNotFailARealTimeStaleFinal()
    {
        // Write 10 completed at 5; write 11 started at 6, after it. The final value is 10.
        ElleWriteLedger ledger = LedgerWith((1, 10, ElleOutcome.Ok, 0, 5), (1, 11, ElleOutcome.Ok, 6, 9));

        QuiescentResult stale = ledger.JudgeRegisters([Final(1, 10)]);
        Assert.That(stale.Passed, Is.True, "serializability allows the later-started write to be ordered first");
        Assert.That(stale.RealtimeStaleFinals, Is.EqualTo(1));

        QuiescentResult fresh = ledger.JudgeRegisters([Final(1, 11)]);
        Assert.That(fresh.RealtimeStaleFinals, Is.EqualTo(0));
    }

    [Test]
    public void RegisterJudgeReportsAKeyNeverReadBackAndExtraFailures()
    {
        ElleWriteLedger ledger = LedgerWith((1, 10, ElleOutcome.Ok, 0, 5), (2, 20, ElleOutcome.Ok, 0, 5));

        QuiescentResult partial = ledger.JudgeRegisters([Final(1, 10)]);
        Assert.That(partial.Passed, Is.False);
        Assert.That(partial.Failures, Has.Some.Contains("never read back"));

        QuiescentResult extra = ledger.JudgeRegisters([Final(1, 10), Final(2, 20)], countMismatches: 3, extraFailures: ["the table holds 3 rows"]);
        Assert.That(extra.Passed, Is.False);
        Assert.That(extra.CountMismatches, Is.EqualTo(3));
        Assert.That(extra.Failures, Has.Some.Contains("the table holds 3 rows"));
    }

    /// <summary>A group read's COUNT(*) must match its SELECT under one snapshot per transaction; under
    /// one snapshot per statement the disagreement is a measurement, carried but not graded.</summary>
    [Test]
    public void RegisterJudgeFailsCountMismatchesOnlyWhenTheTransactionHasOneSnapshot()
    {
        ElleWriteLedger ledger = LedgerWith((1, 10, ElleOutcome.Ok, 0, 5));

        QuiescentResult graded = ledger.JudgeRegisters([Final(1, 10)], countMismatches: 7);
        Assert.That(graded.Passed, Is.False);
        Assert.That(graded.CountMismatches, Is.EqualTo(7));
        Assert.That(graded.Failures, Has.Some.Contains("COUNT(*)"));

        QuiescentResult measured = ledger.JudgeRegisters([Final(1, 10)], countMismatches: 7, countMismatchesFail: false);
        Assert.That(measured.Passed, Is.True, string.Join("; ", measured.Failures));
        Assert.That(measured.CountMismatches, Is.EqualTo(7));
    }

    [Test]
    public void ListJudgeRequiresEveryAcknowledgedAppendOnce()
    {
        ElleWriteLedger ledger = new(ElleModel.ListAppend);
        ledger.RecordOk([ElleMicroOp.Append(1, 10)], 0, 5);
        ledger.RecordOk([ElleMicroOp.Append(1, 11)], 6, 9);
        ledger.RecordInfo([ElleMicroOp.Append(1, 12)]);
        ledger.RecordFail([ElleMicroOp.Append(1, 13)]);

        Assert.That(ledger.JudgeLists([new FinalList(1, [10L, 11L], 1)]).Passed, Is.True);
        Assert.That(ledger.JudgeLists([new FinalList(1, [10L, 11L, 12L], 1)]).Passed, Is.True, "an unresolved append may be there");

        QuiescentResult lost = ledger.JudgeLists([new FinalList(1, [10L], 1)]);
        Assert.That(lost.Passed, Is.False);
        Assert.That(lost.LostWrites, Is.EqualTo(1));

        QuiescentResult aborted = ledger.JudgeLists([new FinalList(1, [10L, 11L, 13L], 1)]);
        Assert.That(aborted.Passed, Is.False);
        Assert.That(aborted.AbortedVisible, Is.EqualTo(1));

        QuiescentResult twice = ledger.JudgeLists([new FinalList(1, [10L, 11L, 10L], 1)]);
        Assert.That(twice.Passed, Is.False);
        Assert.That(twice.DuplicateRows, Is.EqualTo(1));

        QuiescentResult gone = ledger.JudgeLists([new FinalList(1, null, 0)]);
        Assert.That(gone.Passed, Is.False);
        Assert.That(gone.LostWrites, Is.EqualTo(1));

        QuiescentResult reordered = ledger.JudgeLists([new FinalList(1, [11L, 10L], 1)]);
        Assert.That(reordered.Passed, Is.True, "real-time order is counted, not failed");
        Assert.That(reordered.RealtimeStaleFinals, Is.EqualTo(1));
    }

    [Test]
    public void UnavailableFinalStateIsNotAPass()
    {
        ElleWriteLedger ledger = LedgerWith((1, 10, ElleOutcome.Ok, 0, 5));
        QuiescentResult result = QuiescentResult.Unavailable("timeout", ElleModel.RwRegister, ledger);
        Assert.That(result.Passed, Is.False);
        Assert.That(result.Status, Is.EqualTo("unavailable"));
        Assert.That(result.Keys, Is.EqualTo(1));
    }

    [Test]
    public void KeyBaseSeparatesRunsAndFitsALong()
    {
        long a = RegisterOperation.KeyBaseFor(DateTimeOffset.FromUnixTimeSeconds(1_800_000_000));
        long b = RegisterOperation.KeyBaseFor(DateTimeOffset.FromUnixTimeSeconds(1_800_000_001));
        Assert.That(b - a, Is.EqualTo(RegisterOperation.KeyRangeSpan));
        Assert.That(a + RegisterOperation.KeyRangeSpan, Is.LessThan(long.MaxValue / 2));
    }

    [TestCase("explicit", ElleTxnMode.Explicit)]
    [TestCase("Autocommit", ElleTxnMode.Autocommit)]
    [TestCase(" pipeline ", ElleTxnMode.Pipeline)]
    public void TxnModeParses(string text, ElleTxnMode expected)
    {
        Assert.That(ElleTxnModes.TryParse(text, out ElleTxnMode mode), Is.True);
        Assert.That(mode, Is.EqualTo(expected));
        Assert.That(ElleTxnModes.TryParse("batch", out _), Is.False);
    }

    [Test]
    public void PipelinedWritesBySecondaryColumnAreRefused()
    {
        Assert.That(RegisterOperation.TryParseAccess("index", out RegisterAccess access), Is.True);
        Assert.That(access, Is.EqualTo(RegisterAccess.Index));
        Assert.That(RegisterOperation.TryParseKeyType("numeric", out RegisterKeyType keyType), Is.True);
        Assert.That(keyType, Is.EqualTo(RegisterKeyType.Numeric));
        Assert.That(RegisterOperation.DdlFor(RegisterKeyType.String)[0], Does.Contain("k STRING PRIMARY KEY"));
        Assert.That(PredicateOperation.TryParseRead("range", out PredicateRead read), Is.True);
        Assert.That(read, Is.EqualTo(PredicateRead.Range));
    }
}
