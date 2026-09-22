
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Parity tests for the pre-materialized IN-list lookup.
///
/// Unit tests verify <see cref="PreparedInSet.Evaluate"/> against
/// <see cref="SubqueryValueListAst.EvaluateMembership"/> (TRUE / FALSE / UNKNOWN) for:
///   - same-type integer and string lists (value present / absent)
///   - NULL in the list (never a member; no match with a NULL item is UNKNOWN)
///   - NULL lhs (never a member; the predicate is UNKNOWN)
///   - cross-type lookup (int lhs vs float list, and vice versa)
///   - small list (≤8, linear scan) vs large list (>8, HashSet)
///   - empty list
///
/// Integration tests run real SELECT queries and verify that the PreparedInSet fast path
/// produces results identical to the expected filter semantics.
/// </summary>
[TestFixture]
public class TestPreparedInSet : SharedNodeBaseTest
{
    // ── value helpers ─────────────────────────────────────────────────────────

    private static ColumnValue Int(long v) => new(ColumnType.Integer64, v);
    private static ColumnValue Str(string v) => new(ColumnType.String, v);
    private static ColumnValue Flt(double v) => new(ColumnType.Float64, v);
    private static ColumnValue Null() => ColumnValue.Null;

    // The SQL value of `lhs IN (listValues)` on the AST reference path, as TRUE / FALSE / UNKNOWN,
    // so the unit tests can assert three-valued parity with PreparedInSet.Evaluate.
    private static string AstTruth(ColumnValue lhs, IReadOnlyList<ColumnValue> listValues)
    {
        NodeAst node = new(NodeType.ExprInMembership, new NodeAst(NodeType.Null, null, null, null, null, null, null, null, null),
            SubqueryValueListAst.Build(listValues), null, null, null, null, null, null);
        return Truth(SubqueryValueListAst.EvaluateMembership(lhs, node));
    }

    private static string Truth(ColumnValue value) => value.Type switch
    {
        ColumnType.Null => "UNKNOWN",
        ColumnType.Bool => value.BoolValue ? "TRUE" : "FALSE",
        _ => throw new AssertionException($"membership returned a {value.Type}")
    };

    // ── same-type integer lists ───────────────────────────────────────────────

    [Test]
    public void SmallIntList_Present_ReturnsTrue()
    {
        ColumnValue[] values = [Int(1), Int(2), Int(3)];
        PreparedInSet set = new(values);
        Assert.IsTrue(set.Contains(Int(2)));
        Assert.AreEqual(AstTruth(Int(2), values), Truth(set.Evaluate(Int(2))));
    }

    [Test]
    public void SmallIntList_Absent_ReturnsFalse()
    {
        ColumnValue[] values = [Int(1), Int(2), Int(3)];
        PreparedInSet set = new(values);
        Assert.IsFalse(set.Contains(Int(99)));
        Assert.AreEqual(AstTruth(Int(99), values), Truth(set.Evaluate(Int(99))));
    }

    [Test]
    public void LargeIntList_Present_ReturnsTrue()
    {
        ColumnValue[] values = Enumerable.Range(1, 50).Select(i => Int(i)).ToArray();
        PreparedInSet set = new(values);
        Assert.IsTrue(set.Contains(Int(25)));
        Assert.AreEqual(AstTruth(Int(25), values), Truth(set.Evaluate(Int(25))));
    }

    [Test]
    public void LargeIntList_Absent_ReturnsFalse()
    {
        ColumnValue[] values = Enumerable.Range(1, 50).Select(i => Int(i)).ToArray();
        PreparedInSet set = new(values);
        Assert.IsFalse(set.Contains(Int(999)));
        Assert.AreEqual(AstTruth(Int(999), values), Truth(set.Evaluate(Int(999))));
    }

    // ── same-type string lists ────────────────────────────────────────────────

    [Test]
    public void StringList_Present_ReturnsTrue()
    {
        ColumnValue[] values = [Str("alpha"), Str("beta"), Str("gamma")];
        PreparedInSet set = new(values);
        Assert.IsTrue(set.Contains(Str("beta")));
        Assert.AreEqual(AstTruth(Str("beta"), values), Truth(set.Evaluate(Str("beta"))));
    }

    [Test]
    public void StringList_Absent_ReturnsFalse()
    {
        ColumnValue[] values = [Str("alpha"), Str("beta"), Str("gamma")];
        PreparedInSet set = new(values);
        Assert.IsFalse(set.Contains(Str("delta")));
        Assert.AreEqual(AstTruth(Str("delta"), values), Truth(set.Evaluate(Str("delta"))));
    }

    [Test]
    public void StringList_CaseSensitive_NoMatch()
    {
        ColumnValue[] values = [Str("Alpha")];
        PreparedInSet set = new(values);
        Assert.IsFalse(set.Contains(Str("alpha")));
        Assert.AreEqual(AstTruth(Str("alpha"), values), Truth(set.Evaluate(Str("alpha"))));
    }

    // ── String against Uuid / Id: the rule `=` uses ───────────────────────────
    // A uuid column filtered by string parameters builds a set of strings and probes it with Uuid
    // values. Both the linear and the hash path must parse the strings, or a table scan finds none
    // of the rows that the index seek finds.

    [TestCase(3)]
    [TestCase(50)]
    public void UuidProbe_StringItems_MatchesByParsedValue(int size)
    {
        Guid[] guids = Enumerable.Range(0, size).Select(_ => Guid.NewGuid()).ToArray();
        ColumnValue[] values = guids.Select(g => Str(g.ToString())).ToArray();
        PreparedInSet set = new(values);

        foreach (ColumnValue probe in new[] { ColumnValue.FromUuid(guids[size - 1]), ColumnValue.FromUuid(Guid.NewGuid()) })
        {
            Assert.AreEqual(AstTruth(probe, values), Truth(set.Evaluate(probe)));
        }

        Assert.IsTrue(set.Contains(ColumnValue.FromUuid(guids[0])));
        Assert.IsFalse(set.Contains(ColumnValue.FromUuid(Guid.NewGuid())));
    }

    [TestCase(3)]
    [TestCase(50)]
    public void UuidProbe_OtherSpellingsAndMalformedItems(int size)
    {
        Guid[] guids = Enumerable.Range(0, size).Select(_ => Guid.NewGuid()).ToArray();
        ColumnValue[] values =
        [
            .. guids.Select((g, i) => Str(i % 2 == 0 ? g.ToString().ToUpperInvariant() : g.ToString("N"))),
            Str("not-a-uuid"),
        ];
        PreparedInSet set = new(values);

        foreach (Guid g in guids)
            Assert.IsTrue(set.Contains(ColumnValue.FromUuid(g)));

        ColumnValue absent = ColumnValue.FromUuid(Guid.NewGuid());
        Assert.IsFalse(set.Contains(absent));
        Assert.AreEqual(AstTruth(absent, values), Truth(set.Evaluate(absent)));
    }

    [TestCase(3)]
    [TestCase(50)]
    public void StringProbe_UuidItems_MatchesByParsedValue(int size)
    {
        Guid[] guids = Enumerable.Range(0, size).Select(_ => Guid.NewGuid()).ToArray();
        ColumnValue[] values = guids.Select(ColumnValue.FromUuid).ToArray();
        PreparedInSet set = new(values);

        foreach (ColumnValue probe in new[] { Str(guids[1].ToString().ToUpperInvariant()), Str("not-a-uuid"), Str(Guid.NewGuid().ToString()) })
        {
            Assert.AreEqual(AstTruth(probe, values), Truth(set.Evaluate(probe)));
        }

        Assert.IsTrue(set.Contains(Str(guids[1].ToString())));
        Assert.IsFalse(set.Contains(Str("not-a-uuid")));
    }

    [TestCase(3)]
    [TestCase(50)]
    public void IdProbe_StringItems_MatchesByParsedValue(int size)
    {
        string[] ids = Enumerable.Range(0, size).Select(i => (0x2000 + i).ToString("x24")).ToArray();
        ColumnValue[] values = ids.Select(Str).ToArray();
        PreparedInSet set = new(values);

        ColumnValue present = new(ColumnType.Id, ids[size - 1]);
        ColumnValue absent = new(ColumnType.Id, "ffffffffffffffffffffffff");

        Assert.IsTrue(set.Contains(present));
        Assert.IsFalse(set.Contains(absent));
        Assert.AreEqual(AstTruth(present, values), Truth(set.Evaluate(present)));
        Assert.AreEqual(AstTruth(absent, values), Truth(set.Evaluate(absent)));
    }

    [Test]
    public void UuidProbe_StringItems_ConcurrentProbesAgree()
    {
        // A parallel scan shares one set across workers; the converted items are built on first use.
        Guid[] guids = Enumerable.Range(0, 200).Select(_ => Guid.NewGuid()).ToArray();
        PreparedInSet set = new(guids.Select(g => Str(g.ToString())).ToArray());

        int matches = 0;
        Parallel.For(0, 2_000, new ParallelOptions { MaxDegreeOfParallelism = 8 }, i =>
        {
            if (set.Contains(ColumnValue.FromUuid(guids[i % guids.Length])))
                System.Threading.Interlocked.Increment(ref matches);
        });

        Assert.AreEqual(2_000, matches);
    }

    // ── NULL semantics ────────────────────────────────────────────────────────

    [Test]
    public void NullLhs_IsNeverAMember_AndIsUnknown()
    {
        // NULL IN (1, 2) is NULL = 1 OR NULL = 2, which is UNKNOWN, not FALSE: a NOT above it
        // must stay UNKNOWN.
        ColumnValue[] values = [Int(1), Int(2)];
        PreparedInSet set = new(values);
        Assert.IsFalse(set.Contains(Null()));
        Assert.AreEqual("UNKNOWN", Truth(set.Evaluate(Null())));
        Assert.AreEqual(AstTruth(Null(), values), Truth(set.Evaluate(Null())));
    }

    [Test]
    public void NullInList_NonNullLhsCanStillMatch_NoMatchIsUnknown()
    {
        // A NULL value in the list matches nothing; non-null values still match. With no match,
        // the NULL item's equality is UNKNOWN, so the whole predicate is UNKNOWN.
        ColumnValue[] values = [Null(), Int(1), Null(), Int(2)];
        PreparedInSet set = new(values);
        Assert.IsTrue(set.Contains(Int(1)));
        Assert.IsFalse(set.Contains(Int(99)));
        Assert.AreEqual("TRUE", Truth(set.Evaluate(Int(1))));
        Assert.AreEqual("UNKNOWN", Truth(set.Evaluate(Int(99))));
        Assert.AreEqual(AstTruth(Int(1), values), Truth(set.Evaluate(Int(1))));
        Assert.AreEqual(AstTruth(Int(99), values), Truth(set.Evaluate(Int(99))));
    }

    [Test]
    public void NullDroppedByCaller_StillMakesNoMatchUnknown()
    {
        // PredicateAnalyzer removes NULL items before it builds the set and passes the fact in.
        ColumnValue[] nonNullValues = [Int(1), Int(2)];
        PreparedInSet set = new(nonNullValues, containsNull: true);
        Assert.AreEqual("TRUE", Truth(set.Evaluate(Int(2))));
        Assert.AreEqual("UNKNOWN", Truth(set.Evaluate(Int(99))));
        Assert.AreEqual(AstTruth(Int(99), [Int(1), Null(), Int(2)]), Truth(set.Evaluate(Int(99))));
    }

    [Test]
    public void AllNullList_NeverMatches_IsUnknown()
    {
        ColumnValue[] values = [Null(), Null()];
        PreparedInSet set = new(values);
        Assert.IsFalse(set.Contains(Int(1)));
        Assert.AreEqual("UNKNOWN", Truth(set.Evaluate(Int(1))));
        Assert.AreEqual(AstTruth(Int(1), values), Truth(set.Evaluate(Int(1))));
        Assert.AreEqual(AstTruth(Null(), values), Truth(set.Evaluate(Null())));
    }

    [Test]
    public void EmptyList_NeverMatches()
    {
        PreparedInSet set = new(Array.Empty<ColumnValue>());
        Assert.IsFalse(set.Contains(Int(1)));
        Assert.IsFalse(set.Contains(Null()));
        Assert.AreEqual("FALSE", Truth(set.Evaluate(Int(1))));
    }

    // ── cross-type: lhs type does not match list type ─────────────────────────

    [Test]
    public void CrossType_IntLhsFloatList_WidensLikeEquality()
    {
        // x IN (a, b) is x = a OR x = b, and `=` widens a mixed numeric pair to double, so
        // 1 IN (1.0, 2.0) is true and 3 IN (1.0, 2.0) is false — on both paths.
        ColumnValue[] values = [Flt(1.0), Flt(2.0)];
        PreparedInSet set = new(values);
        Assert.IsTrue(set.Contains(Int(1)));
        Assert.IsFalse(set.Contains(Int(3)));
        Assert.AreEqual(AstTruth(Int(1), values), Truth(set.Evaluate(Int(1))));
        Assert.AreEqual(AstTruth(Int(3), values), Truth(set.Evaluate(Int(3))));
    }

    [Test]
    public void CrossType_FloatLhsIntList_WidensLikeEquality()
    {
        ColumnValue[] values = [Int(1), Int(2)];
        PreparedInSet set = new(values);
        Assert.IsTrue(set.Contains(Flt(1.0)));
        Assert.IsFalse(set.Contains(Flt(1.5)));
        Assert.AreEqual(AstTruth(Flt(1.0), values), Truth(set.Evaluate(Flt(1.0))));
        Assert.AreEqual(AstTruth(Flt(1.5), values), Truth(set.Evaluate(Flt(1.5))));
    }

    [Test]
    public void CrossType_FractionalFloatInIntList_NoMatch()
    {
        // 1.5 equals no integer; the widened comparison says so, no rounding is involved.
        ColumnValue[] values = [Int(1), Int(2)];
        PreparedInSet set = new(values);
        Assert.IsFalse(set.Contains(Flt(1.5)));
    }

    [Test]
    public void CrossType_LargeList_HashPathWidensToo()
    {
        // Past the hash threshold the set is a HashSet; an Integer64 probe must land in the bucket
        // of an equal Float64 member, so the hash must be type-agnostic across numeric types.
        ColumnValue[] values = new ColumnValue[20];
        for (int i = 0; i < values.Length; i++)
            values[i] = Flt(i);
        PreparedInSet set = new(values);

        Assert.IsTrue(set.Contains(Int(7)));
        Assert.IsFalse(set.Contains(Int(25)));
        Assert.IsFalse(set.Contains(Flt(7.5)));
        Assert.AreEqual(AstTruth(Int(7), values), Truth(set.Evaluate(Int(7))));
    }

    [Test]
    public void CrossType_MixedList_HomogeneousElementsStillMatch()
    {
        // 5 IN (1, 2, 'foo') ≡ 5=1 OR 5=2 OR 5='foo'. The string element is a non-match, not an
        // error; an int that IS present still matches. Verifies the AST path no longer throws and
        // agrees with PreparedInSet.
        ColumnValue[] values = [Int(1), Int(2), Str("foo")];
        PreparedInSet set = new(values);

        Assert.IsFalse(set.Contains(Int(5)));
        Assert.AreEqual(AstTruth(Int(5), values), Truth(set.Evaluate(Int(5))));

        Assert.IsTrue(set.Contains(Int(2)));
        Assert.AreEqual(AstTruth(Int(2), values), Truth(set.Evaluate(Int(2))));
    }

    // ── large list uses HashSet ───────────────────────────────────────────────

    [Test]
    public void LargeList_BoundaryValues_CorrectlyMatched()
    {
        // 1 000-element list spanning the benchmark shape.
        ColumnValue[] values = Enumerable.Range(1, 1000).Select(i => Int(i)).ToArray();
        PreparedInSet set = new(values);
        Assert.IsTrue(set.Contains(Int(1)));
        Assert.IsTrue(set.Contains(Int(500)));
        Assert.IsTrue(set.Contains(Int(1000)));
        Assert.IsFalse(set.Contains(Int(0)));
        Assert.IsFalse(set.Contains(Int(1001)));
    }

    // ── SQL integration: PreparedInSet is wired into QueryFilterer ────────────

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CreateTableTicket createTicket = new(
            databaseName: dbname,
            tableName: "robots",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("value", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false);

        await executor.CreateTable(createTicket);

        KvTransaction txn = await database.Transactions.BeginAsync();
        for (int i = 0; i < 20; i++)
        {
            await executor.Insert(new InsertTicket(
                txnState: txn,
                databaseName: dbname,
                tableName: "robots",
                values: new()
                {
                    new()
                    {
                        { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "name", new(ColumnType.String, "robot-" + i) },
                        { "value", new(ColumnType.Integer64, i) },
                    }
                }));
        }
        await database.Transactions.CommitAsync(txn);

        return (dbname, database, executor);
    }

    private static async Task<List<QueryResultRow>> RunSql(
        CommandExecutor executor, KvTransaction txn, string dbname, string sql)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: txn, database: dbname, sql: sql, parameters: null));
        return await cursor.ToListAsync();
    }

    [Test]
    public async Task SelectWhereInSmall_PreparedPath_MatchesExpected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        KvTransaction txn = await database.Transactions.BeginAsync();

        List<QueryResultRow> rows = await RunSql(executor, txn, dbname,
            "SELECT name FROM robots WHERE value IN (1, 3, 5)");

        Assert.AreEqual(3, rows.Count);
        CollectionAssert.AreEquivalent(
            new[] { "robot-1", "robot-3", "robot-5" },
            rows.Select(r => r.Row["name"].StrValue));
    }

    [Test]
    public async Task SelectWhereInLarge_PreparedPath_AllMatch()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        KvTransaction txn = await database.Transactions.BeginAsync();

        // 50-value IN list; only 0..19 exist in the table → 20 matches.
        string inList = string.Join(", ", Enumerable.Range(0, 50));
        List<QueryResultRow> rows = await RunSql(executor, txn, dbname,
            $"SELECT name FROM robots WHERE value IN ({inList})");

        Assert.AreEqual(20, rows.Count);
    }

    [Test]
    public async Task SelectWhereInAbsent_PreparedPath_EmptyResult()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        KvTransaction txn = await database.Transactions.BeginAsync();

        List<QueryResultRow> rows = await RunSql(executor, txn, dbname,
            "SELECT name FROM robots WHERE value IN (100, 200, 300)");

        Assert.IsEmpty(rows);
    }
}
