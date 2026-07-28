
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
/// Parity tests for Task 2: pre-materialized IN-list lookup.
///
/// Unit tests verify <see cref="PreparedInSet.Contains"/> against
/// <see cref="SubqueryValueListAst.ContainsValue"/> for:
///   - same-type integer and string lists (value present / absent)
///   - NULL in the list (ignored; a NULL lhs always returns false)
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

    // Wrap the current AST-walk ContainsValue so we can assert parity in unit tests.
    private static bool AstContains(ColumnValue lhs, IReadOnlyList<ColumnValue> listValues)
    {
        NodeAst? ast = SubqueryValueListAst.Build(listValues);
        return SubqueryValueListAst.ContainsValue(lhs, ast);
    }

    // ── same-type integer lists ───────────────────────────────────────────────

    [Test]
    public void SmallIntList_Present_ReturnsTrue()
    {
        ColumnValue[] values = [Int(1), Int(2), Int(3)];
        PreparedInSet set = new(values);
        Assert.IsTrue(set.Contains(Int(2)));
        Assert.AreEqual(AstContains(Int(2), values), set.Contains(Int(2)));
    }

    [Test]
    public void SmallIntList_Absent_ReturnsFalse()
    {
        ColumnValue[] values = [Int(1), Int(2), Int(3)];
        PreparedInSet set = new(values);
        Assert.IsFalse(set.Contains(Int(99)));
        Assert.AreEqual(AstContains(Int(99), values), set.Contains(Int(99)));
    }

    [Test]
    public void LargeIntList_Present_ReturnsTrue()
    {
        ColumnValue[] values = Enumerable.Range(1, 50).Select(i => Int(i)).ToArray();
        PreparedInSet set = new(values);
        Assert.IsTrue(set.Contains(Int(25)));
        Assert.AreEqual(AstContains(Int(25), values), set.Contains(Int(25)));
    }

    [Test]
    public void LargeIntList_Absent_ReturnsFalse()
    {
        ColumnValue[] values = Enumerable.Range(1, 50).Select(i => Int(i)).ToArray();
        PreparedInSet set = new(values);
        Assert.IsFalse(set.Contains(Int(999)));
        Assert.AreEqual(AstContains(Int(999), values), set.Contains(Int(999)));
    }

    // ── same-type string lists ────────────────────────────────────────────────

    [Test]
    public void StringList_Present_ReturnsTrue()
    {
        ColumnValue[] values = [Str("alpha"), Str("beta"), Str("gamma")];
        PreparedInSet set = new(values);
        Assert.IsTrue(set.Contains(Str("beta")));
        Assert.AreEqual(AstContains(Str("beta"), values), set.Contains(Str("beta")));
    }

    [Test]
    public void StringList_Absent_ReturnsFalse()
    {
        ColumnValue[] values = [Str("alpha"), Str("beta"), Str("gamma")];
        PreparedInSet set = new(values);
        Assert.IsFalse(set.Contains(Str("delta")));
        Assert.AreEqual(AstContains(Str("delta"), values), set.Contains(Str("delta")));
    }

    [Test]
    public void StringList_CaseSensitive_NoMatch()
    {
        ColumnValue[] values = [Str("Alpha")];
        PreparedInSet set = new(values);
        Assert.IsFalse(set.Contains(Str("alpha")));
        Assert.AreEqual(AstContains(Str("alpha"), values), set.Contains(Str("alpha")));
    }

    // ── NULL semantics ────────────────────────────────────────────────────────

    [Test]
    public void NullLhs_AlwaysFalse()
    {
        ColumnValue[] values = [Int(1), Int(2)];
        PreparedInSet set = new(values);
        Assert.IsFalse(set.Contains(Null()));
        Assert.AreEqual(AstContains(Null(), values), set.Contains(Null()));
    }

    [Test]
    public void NullInList_IsIgnored_NonNullLhsCanStillMatch()
    {
        // A NULL value in the list matches nothing; non-null values still match.
        ColumnValue[] values = [Null(), Int(1), Null(), Int(2)];
        PreparedInSet set = new(values);
        Assert.IsTrue(set.Contains(Int(1)));
        Assert.IsFalse(set.Contains(Int(99)));
        // ContainsValue skips NULLs in the list, so compare against equivalent non-null list.
        ColumnValue[] nonNullValues = [Int(1), Int(2)];
        Assert.AreEqual(AstContains(Int(1), nonNullValues), set.Contains(Int(1)));
        Assert.AreEqual(AstContains(Int(99), nonNullValues), set.Contains(Int(99)));
    }

    [Test]
    public void AllNullList_NeverMatches()
    {
        ColumnValue[] values = [Null(), Null()];
        PreparedInSet set = new(values);
        Assert.IsFalse(set.Contains(Int(1)));
    }

    [Test]
    public void EmptyList_NeverMatches()
    {
        PreparedInSet set = new(Array.Empty<ColumnValue>());
        Assert.IsFalse(set.Contains(Int(1)));
        Assert.IsFalse(set.Contains(Null()));
    }

    // ── cross-type: lhs type does not match list type ─────────────────────────

    [Test]
    public void CrossType_IntLhsFloatList_NoMatch()
    {
        // Cross-type is a non-match (not an error) on both paths: the prepared set catches the
        // CompareTo throw and the AST reference path now skips cross-type candidates identically.
        ColumnValue[] values = [Flt(1.0), Flt(2.0)];
        PreparedInSet set = new(values);
        Assert.IsFalse(set.Contains(Int(1)));
        Assert.AreEqual(AstContains(Int(1), values), set.Contains(Int(1)));
    }

    [Test]
    public void CrossType_FloatLhsIntList_NoMatch()
    {
        ColumnValue[] values = [Int(1), Int(2)];
        PreparedInSet set = new(values);
        Assert.IsFalse(set.Contains(Flt(1.0)));
        Assert.AreEqual(AstContains(Flt(1.0), values), set.Contains(Flt(1.0)));
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
        Assert.AreEqual(AstContains(Int(5), values), set.Contains(Int(5)));

        Assert.IsTrue(set.Contains(Int(2)));
        Assert.AreEqual(AstContains(Int(2), values), set.Contains(Int(2)));
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
