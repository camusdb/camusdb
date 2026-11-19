/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.Queries;

/// <summary>
/// The row evaluator reads the argument nodes of a function call and the WHEN clauses of a CASE from an
/// array that it builds at the first evaluation of the node. Later evaluations of the same node walk no
/// chain, so a CASE and a lazy function such as <c>COALESCE</c> allocate nothing for each row, and an
/// eager function allocates only the array of its argument values. The tests count the bytes that the
/// current thread allocates over many evaluations. They also check the order of many WHEN clauses and
/// many arguments, and many threads that evaluate one shared tree for the first time together.
/// </summary>
[TestFixture]
internal sealed class TestExpressionEvaluationAllocations
{
    private const int Evaluations = 10_000;

    private static NodeAst Expression(string expression) =>
        new SelectQueryCreator()
            .CreateSelectQuery(SQLParserProcessor.Parse($"SELECT {expression} FROM t"))
            .Projections[0].Expression;

    private static ColumnValue L(long value) => new(ColumnType.Integer64, value);

    private static ColumnValue B(bool value) => new(ColumnType.Bool, value);

    private static Dictionary<string, ColumnValue> Row() => new()
    {
        ["b1"] = B(false),
        ["b2"] = B(false),
        ["b3"] = B(true),
        ["n"] = ColumnValue.Null,
        ["x"] = L(1),
        ["y"] = L(2),
        ["z"] = L(3),
        ["w"] = L(4),
    };

    private static ColumnValue Eval(NodeAst expression, IReadOnlyDictionary<string, ColumnValue> row) =>
        SQLExecutorBaseCreator.EvalExpr(expression, row, null, null);

    /// <summary>
    /// The bytes that one evaluation allocates, averaged over <see cref="Evaluations"/> evaluations
    /// after a warm-up. The warm-up runs the first evaluation, which builds the cached arrays.
    /// </summary>
    private static double BytesPerEvaluation(NodeAst expression, IReadOnlyDictionary<string, ColumnValue> row)
    {
        for (int i = 0; i < 200; i++)
            Eval(expression, row);

        long before = GC.GetAllocatedBytesForCurrentThread();

        for (int i = 0; i < Evaluations; i++)
            Eval(expression, row);

        return (GC.GetAllocatedBytesForCurrentThread() - before) / (double)Evaluations;
    }

    [Test]
    public void SearchedCaseAllocatesNothing()
    {
        NodeAst expression = Expression("CASE WHEN b1 THEN x WHEN b2 THEN y WHEN b3 THEN z ELSE w END");
        Dictionary<string, ColumnValue> row = Row();

        Assert.That(Eval(expression, row).LongValue, Is.EqualTo(3));
        Assert.That(BytesPerEvaluation(expression, row), Is.EqualTo(0));
    }

    [Test]
    public void SimpleCaseAllocatesNothing()
    {
        NodeAst expression = Expression("CASE x WHEN 5 THEN y WHEN 1 THEN z ELSE w END");
        Dictionary<string, ColumnValue> row = Row();

        Assert.That(Eval(expression, row).LongValue, Is.EqualTo(3));
        Assert.That(BytesPerEvaluation(expression, row), Is.EqualTo(0));
    }

    [Test]
    public void LazyFunctionCallAllocatesNothing()
    {
        // An upper-case name: the lower-case name is part of the cached call shape.
        NodeAst expression = Expression("COALESCE(n, n, y, z)");
        Dictionary<string, ColumnValue> row = Row();

        Assert.That(Eval(expression, row).LongValue, Is.EqualTo(2));
        Assert.That(BytesPerEvaluation(expression, row), Is.EqualTo(0));
    }

    [Test]
    public void EagerFunctionCallAllocatesOnlyItsArgumentValues()
    {
        // NULLIF returns its first argument when the two differ, so the result allocates nothing. The
        // one allocation left is the array of the two argument values: a 16-byte header, an 8-byte
        // length and two 8-byte references. A chain walk for each row allocated a stack and two lists,
        // several times that size.
        NodeAst expression = Expression("NULLIF(x, y)");
        Dictionary<string, ColumnValue> row = Row();

        Assert.That(Eval(expression, row).LongValue, Is.EqualTo(1));
        Assert.That(BytesPerEvaluation(expression, row), Is.LessThanOrEqualTo(40));
    }

    [Test]
    public void ManyWhenClausesKeepFirstMatchOrder()
    {
        // Clause i matches when v = i % 10, so for each v the first match is clause v.
        StringBuilder sql = new("CASE");
        for (int i = 0; i < 60; i++)
            sql.Append($" WHEN v = {i % 10} THEN {i}");
        sql.Append(" ELSE -1 END");

        NodeAst expression = Expression(sql.ToString());

        for (int v = 0; v < 12; v++)
        {
            Dictionary<string, ColumnValue> row = new() { ["v"] = L(v) };
            long expected = v < 10 ? v : -1;

            // Two passes: the first builds the cached clause array, the second reads it.
            Assert.That(Eval(expression, row).LongValue, Is.EqualTo(expected), $"v = {v}, first pass");
            Assert.That(Eval(expression, row).LongValue, Is.EqualTo(expected), $"v = {v}, second pass");
        }
    }

    [Test]
    public void ManyArgumentsKeepSourceOrder()
    {
        string arguments = string.Join(", ", Enumerable.Range(0, 50).Select(i => $"'{(char)('a' + i % 26)}'"));
        string expected = string.Concat(Enumerable.Range(0, 50).Select(i => (char)('a' + i % 26)));

        NodeAst expression = Expression($"concat({arguments})");
        Dictionary<string, ColumnValue> row = Row();

        Assert.That(Eval(expression, row).StrValue, Is.EqualTo(expected));
        Assert.That(Eval(expression, row).StrValue, Is.EqualTo(expected));
    }

    [Test]
    public void SingleArgumentAndNoArgumentCallsKeepTheirArity()
    {
        Dictionary<string, ColumnValue> row = Row();

        Assert.That(Eval(Expression("abs(-7)"), row).LongValue, Is.EqualTo(7));

        CamusDB.Core.CamusDBException? error = Assert.Throws<CamusDB.Core.CamusDBException>(() => Eval(Expression("abs()"), row));
        Assert.That(error!.Message, Does.Contain("expects 1 argument(s) but received 0"));
    }

    [Test]
    public async Task ThreadsThatShareOneTreeAllGetTheRightResults()
    {
        // One tree for all threads, as a cached plan gives. The threads start together, so several of
        // them build the cached arrays of the same node at the same time.
        NodeAst expression = Expression(
            "CASE WHEN b1 THEN 0 WHEN COALESCE(n, x) = v THEN NULLIF(y, n) WHEN b3 THEN mod(17, 7) ELSE w END");

        using Barrier start = new(8);
        int wrong = 0;

        Task[] workers = Enumerable.Range(0, 8).Select(worker => Task.Run(() =>
        {
            start.SignalAndWait();

            for (int i = 0; i < 2_000; i++)
            {
                Dictionary<string, ColumnValue> row = Row();
                row["v"] = L(i % 2);

                ColumnValue value = Eval(expression, row);
                bool ok = value.LongValue == (i % 2 == 1 ? 2 : 3);

                if (!ok)
                    Interlocked.Increment(ref wrong);
            }
        })).ToArray();

        await Task.WhenAll(workers);

        Assert.That(wrong, Is.EqualTo(0));
    }
}
