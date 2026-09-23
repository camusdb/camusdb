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
/// The history is the evidence Elle judges, so its shape is the contract: one EDN map per line, in
/// index order, with process ids that Elle can treat as single-threaded clients.
/// </summary>
[TestFixture]
public sealed class ElleHistoryTests
{
    private static (ElleHistory History, Func<string[]> Lines) NewHistory()
    {
        MemoryStream stream = new();
        StreamWriter writer = new(stream, new UTF8Encoding(false), leaveOpen: true);
        ElleHistory history = new(writer);
        return (history, () =>
        {
            writer.Flush();
            return Encoding.UTF8.GetString(stream.ToArray()).Split('\n', StringSplitOptions.RemoveEmptyEntries);
        });
    }

    private static string Process(string line)
    {
        int at = line.IndexOf(":process ", StringComparison.Ordinal) + ":process ".Length;
        return line[at..line.IndexOf(',', at)];
    }

    [Test]
    public void WritesInvokeAndOkWithObservedReads()
    {
        (ElleHistory history, Func<string[]> lines) = NewHistory();
        ElleMicroOp[] txn = [ElleMicroOp.Append(3, 17), ElleMicroOp.ReadOf(3), ElleMicroOp.ReadOf(4)];

        ElleInvocation invocation = history.Invoke(txn);
        history.Complete(invocation, ElleOutcome.Ok,
            [txn[0], txn[1].WithRead([1L, 2L, 17L]), txn[2].WithRead(null)]);
        history.Close();

        string[] written = lines();
        Assert.That(written, Has.Length.EqualTo(2));
        Assert.That(written[0], Does.StartWith("{:index 0, :type :invoke, :f :txn, :process 0, :time "));
        Assert.That(written[0], Does.EndWith(":value [[:append 3 17] [:r 3 nil] [:r 4 nil]]}"));
        Assert.That(written[1], Does.StartWith("{:index 1, :type :ok, :f :txn, :process 0, :time "));
        Assert.That(written[1], Does.EndWith(":value [[:append 3 17] [:r 3 [1 2 17]] [:r 4 nil]]}"));
    }

    [Test]
    public void FailAndInfoRepeatTheInvokedValue()
    {
        (ElleHistory history, Func<string[]> lines) = NewHistory();
        ElleMicroOp[] txn = [ElleMicroOp.Append(1, 5), ElleMicroOp.ReadOf(1)];

        history.Complete(history.Invoke(txn), ElleOutcome.Fail, [txn[0], txn[1].WithRead([5L])]);
        history.Complete(history.Invoke(txn), ElleOutcome.Info);
        history.Close();

        string[] written = lines();
        Assert.That(written[1], Does.Contain(":type :fail"));
        Assert.That(written[1], Does.EndWith(":value [[:append 1 5] [:r 1 nil]]}"));
        Assert.That(written[3], Does.Contain(":type :info"));
        Assert.That(written[3], Does.EndWith(":value [[:append 1 5] [:r 1 nil]]}"));
    }

    [Test]
    public void ProcessIsReusedAfterOkOrFailButRetiredAfterInfo()
    {
        (ElleHistory history, Func<string[]> lines) = NewHistory();
        ElleMicroOp[] txn = [ElleMicroOp.ReadOf(1)];

        ElleInvocation first = history.Invoke(txn);
        history.Complete(first, ElleOutcome.Ok, txn);
        ElleInvocation second = history.Invoke(txn);
        history.Complete(second, ElleOutcome.Info);
        ElleInvocation third = history.Invoke(txn);
        history.Close();

        Assert.That(second.Process, Is.EqualTo(first.Process), "an :ok process must be reusable");
        Assert.That(third.Process, Is.Not.EqualTo(second.Process), "an :info process must never be reused");
        Assert.That(lines().Select(Process).Distinct().Count(), Is.EqualTo(2));
    }

    [Test]
    public void ConcurrentInvocationsGetDistinctProcesses()
    {
        (ElleHistory history, _) = NewHistory();
        ElleMicroOp[] txn = [ElleMicroOp.ReadOf(1)];

        ElleInvocation a = history.Invoke(txn);
        ElleInvocation b = history.Invoke(txn);

        Assert.That(a.Process, Is.Not.EqualTo(b.Process), "one process must never have two operations in flight");
        history.Close();
    }

    [Test]
    public void CloseCompletesOpenOperationsAsInfoAndIgnoresLateCompletions()
    {
        (ElleHistory history, Func<string[]> lines) = NewHistory();
        ElleMicroOp[] txn = [ElleMicroOp.Append(2, 9)];

        ElleInvocation open = history.Invoke(txn);
        ElleHistoryStats stats = history.Close();
        history.Complete(open, ElleOutcome.Ok, txn);
        history.Invoke(txn);

        string[] written = lines();
        Assert.That(written, Has.Length.EqualTo(2));
        Assert.That(written[1], Does.Contain(":type :info"));
        Assert.That(stats, Is.EqualTo(new ElleHistoryStats(1, 0, 0, 1, 1, 1)));
    }

    [Test]
    public void EveryInvocationHasExactlyOneCompletionUnderConcurrency()
    {
        (ElleHistory history, Func<string[]> lines) = NewHistory();

        Parallel.For(0, 2000, i =>
        {
            ElleMicroOp[] txn = [ElleMicroOp.Append(i % 7, i + 1)];
            ElleInvocation invocation = history.Invoke(txn);
            history.Complete(invocation, (i % 3) switch { 0 => ElleOutcome.Ok, 1 => ElleOutcome.Fail, _ => ElleOutcome.Info }, txn);
        });
        history.Close();

        string[] written = lines();
        Assert.That(written, Has.Length.EqualTo(4000));

        // Replaying the file: a process never has two open operations, and never runs after an :info.
        Dictionary<string, bool> open = new();
        HashSet<string> retired = new();
        for (int i = 0; i < written.Length; i++)
        {
            Assert.That(written[i], Does.StartWith($"{{:index {i}, "), "lines must be in index order");
            string process = Process(written[i]);
            if (written[i].Contains(":type :invoke"))
            {
                Assert.That(retired, Does.Not.Contain(process), $"line {i}: process {process} reused after :info");
                Assert.That(open.GetValueOrDefault(process), Is.False, $"line {i}: process {process} already busy");
                open[process] = true;
            }
            else
            {
                Assert.That(open.GetValueOrDefault(process), Is.True, $"line {i}: completion without invocation");
                open[process] = false;
                if (written[i].Contains(":type :info"))
                    retired.Add(process);
            }
        }
    }

    [Test]
    public void KeySpaceGivesUniqueValuesAndRotatesKeys()
    {
        AppendKeySpace keys = new(seed: 1847, activeKeys: 2, maxWritesPerKey: 3, maxTxnLength: 4);
        List<ElleMicroOp> appends = [];
        for (int i = 0; i < 500; i++)
            appends.AddRange(keys.NextTxn(readOnly: false).Where(s => s.IsAppend));

        Assert.That(appends.Select(a => a.Value).Distinct().Count(), Is.EqualTo(appends.Count), "append values must be unique");
        Assert.That(appends.GroupBy(a => a.Key).Max(g => g.Count()), Is.LessThanOrEqualTo(3));
        Assert.That(keys.KeysUsed, Is.GreaterThan(2), "slots must move to new keys");
    }

    [Test]
    public void ReadOnlyTransactionsHaveOnlyReads()
    {
        AppendKeySpace keys = new(seed: 3, activeKeys: 4, maxWritesPerKey: 8, maxTxnLength: 5);
        for (int i = 0; i < 200; i++)
        {
            ElleMicroOp[] txn = keys.NextTxn(readOnly: true);
            Assert.That(txn, Has.Length.InRange(1, 5));
            Assert.That(txn.Any(s => s.IsAppend), Is.False);
        }
    }

    [Test]
    public void KeySpaceRepeatsForAFixedSeed()
    {
        AppendKeySpace a = new(seed: 99, activeKeys: 5, maxWritesPerKey: 4, maxTxnLength: 3);
        AppendKeySpace b = new(seed: 99, activeKeys: 5, maxWritesPerKey: 4, maxTxnLength: 3);
        for (int i = 0; i < 100; i++)
            Assert.That(a.NextTxn(i % 4 == 0), Is.EqualTo(b.NextTxn(i % 4 == 0)).AsCollection);
    }

    [Test]
    public void ParsesStoredLists()
    {
        Assert.That(AppendOperation.ParseList("17", 1), Is.EqualTo(new long[] { 17 }));
        Assert.That(AppendOperation.ParseList("1,2,30", 1), Is.EqualTo(new long[] { 1, 2, 30 }));
    }

    [TestCase("")]
    [TestCase("1,,2")]
    [TestCase("1,-2")]
    [TestCase("1, 2")]
    [TestCase("abc")]
    public void RejectsAListNoAppendCouldHaveWritten(string stored)
    {
        Assert.Throws<InvalidOperationException>(() => AppendOperation.ParseList(stored, 1));
    }
}
