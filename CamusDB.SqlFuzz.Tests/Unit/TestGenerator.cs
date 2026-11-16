/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using CamusDB.SqlFuzz.Tests.Campaign;
using CamusDB.SqlFuzz.Tests.Engine;
using CamusDB.SqlFuzz.Tests.Generator;
using CamusDB.SqlFuzz.Tests.Oracles;
using CamusDB.SqlFuzz.Tests.Printing;
using CamusDB.SqlFuzz.Tests.Runner;
using CamusDB.SqlFuzz.Tests.Shrinking;

namespace CamusDB.SqlFuzz.Tests.Unit;

/// <summary>
/// The generator: one seed gives one statement list, a profile removes what it removes, and most
/// generated statements run without an error.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestGenerator
{
    private static List<string> Program(ulong seed, GeneratorProfile profile, int statements)
    {
        StatementGenerator generator = new(new FuzzRandom(seed), profile);
        List<Statement> list = generator.GenerateSchema();
        list.AddRange(generator.GenerateData());
        for (int i = 0; i < statements; i++)
            list.Add(generator.GenerateStatement());
        return list.Select(CamusSqlPrinter.Instance.Print).ToList();
    }

    [Test]
    public void OneSeedGivesOneStatementList()
    {
        for (ulong seed = 1; seed <= 20; seed++)
            Assert.That(Program(seed, GeneratorProfile.CamusDB, 50), Is.EqualTo(Program(seed, GeneratorProfile.CamusDB, 50)), $"seed {seed}");

        Assert.That(Program(1, GeneratorProfile.CamusDB, 50), Is.Not.EqualTo(Program(2, GeneratorProfile.CamusDB, 50)));
    }

    [Test]
    public void InListItemsAreColumnsOfTheScopeOrLiterals()
    {
        // CamusDB reads a column item of an IN list from the row, so the generator writes some; the
        // grammar takes nothing but a literal, a parameter or a name as an item, so it writes no other
        // form. Literals stay the common case: a list of columns rarely matches, and the planner seeks
        // only a list of constants.
        int columnItems = 0, literalItems = 0;

        for (ulong seed = 1; seed <= 40; seed++)
        {
            StatementGenerator generator = new(new FuzzRandom(seed), GeneratorProfile.CamusDB);
            generator.GenerateSchema();
            generator.GenerateData();

            for (int i = 0; i < 250; i++)
            {
                Statement statement = generator.GenerateStatement();

                foreach (object node in ExprWalker.Nodes(statement))
                {
                    if (node is not InListExpr list)
                        continue;

                    foreach (Expr item in list.Items)
                    {
                        switch (item)
                        {
                            case ColumnExpr:
                                columnItems++;
                                break;
                            case LiteralExpr:
                                literalItems++;
                                break;
                            default:
                                Assert.Fail("an IN-list item that is neither a column nor a literal: " + CamusSqlPrinter.Instance.Print(statement));
                                break;
                        }
                    }
                }
            }
        }

        Assert.That(columnItems, Is.GreaterThan(0), "no column item in 10,000 statements");
        Assert.That(literalItems, Is.GreaterThan(columnItems), "literal items must stay the common case");
    }

    [Test]
    public void TheRandomSourceIsFixed()
    {
        // The sequence is part of the replay contract: a CI seed must give the same statements on every
        // machine and runtime. A change to FuzzRandom breaks every recorded seed.
        FuzzRandom random = new(42);
        // xoshiro256** seeded through SplitMix64 with 42; the values match the reference algorithm.
        Assert.That(new[] { random.NextULong(), random.NextULong(), random.NextULong() },
            Is.EqualTo(new[] { 1546998764402558742UL, 6990951692964543102UL, 12544586762248559009UL }));
        Assert.That(FuzzRandom.DeriveSeed(7, 3), Is.EqualTo(13015481187462834606UL));
        Assert.That(FuzzRandom.DeriveSeed(7, 3), Is.EqualTo(FuzzRandom.DeriveSeed(7, 3)));
        Assert.That(FuzzRandom.DeriveSeed(7, 3), Is.Not.EqualTo(FuzzRandom.DeriveSeed(7, 4)));
    }

    [Test]
    public void SubqueriesGoInEveryPositionAndAreCorrelatedOnlyInTheWhereOfASelect()
    {
        // CamusDB folds an uncorrelated subquery in every expression position, and evaluates a
        // correlated EXISTS per row in the WHERE of a SELECT only. The generator must write the first
        // kind everywhere it can, and the second kind nowhere else: a correlated EXISTS in a select
        // list, an ON clause or a DML WHERE is refused, and a stream of refusals hides findings.
        int inSelectList = 0, inOn = 0, inDmlWhere = 0, correlatedInSelectWhere = 0, belowAnotherNode = 0;

        for (ulong seed = 1; seed <= 40; seed++)
        {
            StatementGenerator generator = new(new FuzzRandom(seed), GeneratorProfile.CamusDB);
            generator.GenerateSchema();
            generator.GenerateData();

            for (int i = 0; i < 250; i++)
            {
                Statement statement = generator.GenerateStatement();

                foreach (object node in ExprWalker.Nodes(statement))
                {
                    if (node is not SelectStmt select)
                        continue;

                    foreach (SelectItem item in select.Items)
                    {
                        foreach (ExistsExpr exists in DirectSubqueries(item.Expr))
                        {
                            inSelectList++;
                            Assert.That(IsCorrelated(exists), Is.False, "a correlated EXISTS in a select list: " + CamusSqlPrinter.Instance.Print(statement));
                        }
                    }

                    foreach (Expr on in ExprWalker.JoinPredicates(select.From))
                    {
                        foreach (ExistsExpr exists in DirectSubqueries(on))
                        {
                            inOn++;
                            Assert.That(IsCorrelated(exists), Is.False, "a correlated EXISTS in an ON clause: " + CamusSqlPrinter.Instance.Print(statement));
                        }
                    }

                    if (select.Where is not null)
                    {
                        foreach (ExistsExpr exists in DirectSubqueries(select.Where))
                        {
                            if (IsCorrelated(exists))
                                correlatedInSelectWhere++;
                        }

                        // A subquery below a node other than AND, OR or NOT: a CASE branch, a comparison operand, an IS NULL test.
                        if (select.Where is not (ExistsExpr or InSubqueryExpr or BinaryExpr { Op: BinaryOp.And or BinaryOp.Or } or UnaryExpr { Op: UnaryOp.Not })
                            && DirectSubqueries(select.Where).Any())
                            belowAnotherNode++;
                    }
                }

                Expr? dmlWhere = statement switch { UpdateStmt u => u.Where, DeleteStmt d => d.Where, _ => null };
                if (dmlWhere is not null)
                {
                    foreach (ExistsExpr exists in DirectSubqueries(dmlWhere))
                    {
                        inDmlWhere++;
                        Assert.That(IsCorrelated(exists), Is.False, "a correlated EXISTS in a DML WHERE: " + CamusSqlPrinter.Instance.Print(statement));
                    }
                }
            }
        }

        Assert.That(inSelectList, Is.GreaterThan(0), "no subquery in a select list");
        Assert.That(inOn, Is.GreaterThan(0), "no subquery in an ON clause");
        Assert.That(inDmlWhere, Is.GreaterThan(0), "no subquery in a DML WHERE");
        Assert.That(correlatedInSelectWhere, Is.GreaterThan(0), "no correlated EXISTS in a SELECT WHERE");
        Assert.That(belowAnotherNode, Is.GreaterThan(0), "no subquery below a node other than AND, OR or NOT");
    }

    /// <summary>The EXISTS subqueries of an expression, without entering the subqueries themselves.</summary>
    private static IEnumerable<ExistsExpr> DirectSubqueries(Expr expression)
    {
        Stack<object> pending = new();
        pending.Push(expression);
        while (pending.Count > 0)
        {
            object node = pending.Pop();
            if (node is ExistsExpr exists)
            {
                yield return exists;
                continue;
            }
            if (node is SelectStmt)
                continue;
            foreach (object child in ExprWalker.Children(node))
                pending.Push(child);
        }
    }

    /// <summary>True when the subquery's WHERE reads a column of the enclosing query (one not qualified by the subquery's own alias).</summary>
    private static bool IsCorrelated(ExistsExpr exists)
    {
        string alias = ((TableRef)exists.Subquery.From).Alias;
        if (exists.Subquery.Where is null)
            return false;

        Stack<object> pending = new();
        pending.Push(exists.Subquery.Where);
        while (pending.Count > 0)
        {
            object node = pending.Pop();
            if (node is ColumnExpr column && column.Source != alias)
                return true;
            if (node is SelectStmt)
                continue;
            foreach (object child in ExprWalker.Children(node))
                pending.Push(child);
        }

        return false;
    }

    /// <summary>
    /// The generator writes UPDATE … FROM, DELETE … USING and the alias-only forms. In them every column
    /// outside a subquery is qualified, by the target qualifier or by an alias of the list, because a bare
    /// name that the target and a source both have is ambiguous. The WHERE links the target with the list.
    /// </summary>
    [Test]
    public void CtesAppearAndPrintInFrontOfTheirSelect()
    {
        int withSelects = 0, readTwice = 0;

        for (ulong seed = 1; seed <= 40; seed++)
        {
            StatementGenerator generator = new(new FuzzRandom(seed), GeneratorProfile.CamusDB);
            generator.GenerateSchema();
            generator.GenerateData();

            for (int i = 0; i < 50; i++)
            {
                SelectStmt select = generator.GenerateSelect(SelectOptions.Comparable);
                if (select.CteTables is not { Count: > 0 } tables)
                    continue;

                withSelects++;
                string sql = CamusSqlPrinter.Instance.Print(select);
                Assert.That(sql, Does.StartWith("WITH " + CamusSqlPrinter.CteName(tables[0]) + " AS (SELECT * FROM " + tables[0] + ")"));

                List<TableRef> marked = Tables(select.From).Where(t => t.ViaCte).ToList();
                Assert.That(marked, Is.Not.Empty, sql);
                foreach (TableRef table in marked)
                {
                    Assert.That(tables, Does.Contain(table.Table), sql);
                    Assert.That(table.IndexHint, Is.Null, sql);
                    Assert.That(sql, Does.Contain(CamusSqlPrinter.CteName(table.Table) + " AS " + table.Alias), sql);
                }

                if (marked.GroupBy(t => t.Table).Any(g => g.Count() > 1))
                    readTwice++;
            }
        }

        Assert.That(withSelects, Is.GreaterThan(100), "SELECTs with a WITH");
        Assert.That(readTwice, Is.GreaterThan(3), "a CTE that the statement reads two times");

        // A statement that reuses the FROM without the WITH prints the plain table.
        SelectStmt plain = new([], new TableRef("t0", "a0") { ViaCte = true });
        Assert.That(CamusSqlPrinter.Instance.Print(plain), Is.EqualTo("SELECT * FROM t0 AS a0"));
        Assert.That(CamusSqlPrinter.Instance.Print(plain with { CteTables = ["t0"] }),
            Is.EqualTo("WITH cte_t0 AS (SELECT * FROM t0) SELECT * FROM cte_t0 AS a0"));

        // The right operand of a set operation reads the CTEs of the WITH in front of the whole query.
        SelectStmt union = new([new SelectItem(new ColumnExpr("a0", "c0", FuzzType.Int64))], new TableRef("t0", "a0") { ViaCte = true },
            SetOp: new SetOperation(SetOpKind.UnionAll, new SelectStmt([new SelectItem(new ColumnExpr("a1", "c0", FuzzType.Int64))],
                new TableRef("t0", "a1") { ViaCte = true }) { CteTables = ["t0"] })) { CteTables = ["t0"] };
        Assert.That(CamusSqlPrinter.Instance.Print(union),
            Is.EqualTo("WITH cte_t0 AS (SELECT * FROM t0) SELECT a0.c0 FROM cte_t0 AS a0 UNION ALL SELECT a1.c0 FROM cte_t0 AS a1"));
    }

    /// <summary>
    /// Some SELECTs with an inner or LEFT join print one of those joins as a LATERAL derived table that holds
    /// the ON condition as its WHERE. A statement that reuses the FROM without the flag prints the join.
    /// </summary>
    [Test]
    public void LateralJoinsAppearAndPrintAsDerivedTables()
    {
        int lateralSelects = 0;

        for (ulong seed = 1; seed <= 40; seed++)
        {
            StatementGenerator generator = new(new FuzzRandom(seed), GeneratorProfile.CamusDB);
            generator.GenerateSchema();
            generator.GenerateData();

            for (int i = 0; i < 50; i++)
            {
                SelectStmt select = generator.GenerateSelect(SelectOptions.Comparable);
                if (!select.LateralJoins)
                    continue;

                lateralSelects++;
                string sql = CamusSqlPrinter.Instance.Print(select);
                Assert.That(sql, Does.Contain(" JOIN LATERAL (SELECT * FROM "), sql);
            }
        }

        Assert.That(lateralSelects, Is.GreaterThan(30), "SELECTs with a LATERAL join");

        ColumnExpr left = new("a0", "c0", FuzzType.Int64);
        ColumnExpr right = new("a1", "c0", FuzzType.Int64);
        BinaryExpr on = new(BinaryOp.Eq, left, right, FuzzType.Bool);

        JoinFrom inner = new(new TableRef("t0", "a0"), JoinKind.Inner, new TableRef("t1", "a1"), on) { ViaLateral = true };
        Assert.That(CamusSqlPrinter.Instance.Print(new SelectStmt([], inner)),
            Is.EqualTo("SELECT * FROM t0 AS a0 INNER JOIN t1 AS a1 ON (a0.c0 = a1.c0)"));
        Assert.That(CamusSqlPrinter.Instance.Print(new SelectStmt([], inner) { LateralJoins = true }),
            Is.EqualTo("SELECT * FROM t0 AS a0 CROSS JOIN LATERAL (SELECT * FROM t1 AS a1 WHERE (a0.c0 = a1.c0)) AS a1"));

        JoinFrom outer = inner with { Kind = JoinKind.Left };
        Assert.That(CamusSqlPrinter.Instance.Print(new SelectStmt([], outer) { LateralJoins = true }),
            Is.EqualTo("SELECT * FROM t0 AS a0 LEFT JOIN LATERAL (SELECT * FROM t1 AS a1 WHERE (a0.c0 = a1.c0)) AS a1 ON TRUE"));

        // An index hint on the right table keeps the join form.
        JoinFrom hinted = inner with { Right = new TableRef("t1", "a1", "t1_i0") };
        StringAssert.DoesNotContain("LATERAL", CamusSqlPrinter.Instance.Print(new SelectStmt([], hinted) { LateralJoins = true }));
    }

    /// <summary>
    /// A VALUES list appears first in the FROM of some SELECTs and of some UPDATE … FROM and DELETE … USING lists,
    /// prints as <c>(VALUES (…), …) AS a0 (v0, …)</c>, and never appears for a profile without VALUES lists.
    /// </summary>
    [Test]
    public void RangeJoinsBoundANumericRightColumnWithTwoLeftColumns()
    {
        int between = 0, comparisons = 0;

        for (ulong seed = 1; seed <= 40; seed++)
        {
            StatementGenerator generator = new(new FuzzRandom(seed), GeneratorProfile.CamusDB);
            generator.GenerateSchema();
            generator.GenerateData();

            for (int i = 0; i < 50; i++)
            {
                SelectStmt select = generator.GenerateSelect(SelectOptions.Comparable);
                for (FromItem item = select.From; item is JoinFrom join; item = join.Left)
                {
                    string rightAlias = join.Right.Alias;
                    switch (join.On)
                    {
                        case BetweenExpr { Value: ColumnExpr key, Low: ColumnExpr low, High: ColumnExpr high, Negated: false }
                            when key.Source == rightAlias && low.Source != rightAlias && high.Source != rightAlias:
                            between++;
                            Assert.That(key.Type, Is.EqualTo(FuzzType.Int64).Or.EqualTo(FuzzType.Float64));
                            break;

                        case BinaryExpr { Op: BinaryOp.And, Left: BinaryExpr { Op: BinaryOp.Ge or BinaryOp.Gt, Left: ColumnExpr key },
                                Right: BinaryExpr { Op: BinaryOp.Le or BinaryOp.Lt, Left: ColumnExpr same } }
                            when key.Source == rightAlias && same == key:
                            comparisons++;
                            break;
                    }
                }
            }
        }

        Assert.That(between, Is.GreaterThan(5), "range joins written with BETWEEN");
        Assert.That(comparisons, Is.GreaterThan(5), "range joins written with two comparisons");
    }

    /// <summary>
    /// Some join equalities compare a column of one side with an expression of the other, the shape the
    /// planner keys a hash join on.
    /// </summary>
    [Test]
    public void SomeJoinEqualitiesCompareAColumnWithAnExpression()
    {
        int expressionSides = 0;

        for (ulong seed = 1; seed <= 40; seed++)
        {
            StatementGenerator generator = new(new FuzzRandom(seed), GeneratorProfile.CamusDB);
            generator.GenerateSchema();
            generator.GenerateData();

            for (int i = 0; i < 50; i++)
            {
                SelectStmt select = generator.GenerateSelect(SelectOptions.Comparable);
                for (FromItem item = select.From; item is JoinFrom join; item = join.Left)
                {
                    if (join.On is BinaryExpr { Op: BinaryOp.Eq, Left: var l, Right: var r }
                        && (l is ColumnExpr) != (r is ColumnExpr))
                        expressionSides++;
                }
            }
        }

        Assert.That(expressionSides, Is.GreaterThan(5), "join equalities with an expression on one side");
    }

    [Test]
    public void ValuesListsAppearInSelectsAndJoinedWrites()
    {
        int inSelects = 0, inJoins = 0, inWrites = 0;

        for (ulong seed = 1; seed <= 40; seed++)
        {
            StatementGenerator generator = new(new FuzzRandom(seed), GeneratorProfile.CamusDB);
            generator.GenerateSchema();
            generator.GenerateData();

            for (int i = 0; i < 50; i++)
            {
                SelectStmt select = generator.GenerateSelect(SelectOptions.Comparable);
                FromItem first = select.From;
                while (first is JoinFrom join)
                    first = join.Left;
                if (first is not ValuesRef values)
                    continue;

                inSelects++;
                inJoins += select.From is JoinFrom ? 1 : 0;
                string sql = CamusSqlPrinter.Instance.Print(select);
                Assert.That(sql, Does.Contain("FROM (VALUES ("), sql);
                Assert.That(sql, Does.Contain(") AS a0 (" + string.Join(", ", values.Columns.Select(c => c.Name)) + ")"), sql);
                Assert.That(values.Rows[0].All(item => item is LiteralExpr { Value.IsNull: false }), Is.True, sql);
            }

            for (int i = 0; i < 100; i++)
            {
                FromItem? list = generator.GenerateWrite() switch
                {
                    UpdateStmt u => u.From,
                    DeleteStmt d => d.Using,
                    _ => null,
                };
                while (list is JoinFrom join)
                    list = join.Left;
                inWrites += list is ValuesRef ? 1 : 0;
            }
        }

        Assert.That(inSelects, Is.GreaterThan(20), "SELECTs with a VALUES list");
        Assert.That(inJoins, Is.GreaterThan(5), "joins with a VALUES list");
        Assert.That(inWrites, Is.GreaterThan(2), "joined writes with a VALUES list");

        ValuesRef printed = new("a0", [("v0", FuzzType.Int64), ("v1", FuzzType.String)],
            [[new LiteralExpr(FuzzValue.Of(1L)), new LiteralExpr(FuzzValue.String("x"))],
             [new LiteralExpr(FuzzValue.Null(FuzzType.Int64)), new LiteralExpr(FuzzValue.String("y"))]]);
        Assert.That(CamusSqlPrinter.Instance.Print(new SelectStmt([], printed)),
            Is.EqualTo("SELECT * FROM (VALUES (1, 'x'), (CAST(NULL AS int64), 'y')) AS a0 (v0, v1)"));

        GeneratorProfile without = GeneratorProfile.CamusDB.WithoutFeatures(StatementFeature.ValuesLists);
        for (ulong seed = 1; seed <= 10; seed++)
        {
            StatementGenerator generator = new(new FuzzRandom(seed), without);
            generator.GenerateSchema();
            generator.GenerateData();
            for (int i = 0; i < 50; i++)
                StringAssert.DoesNotContain("(VALUES", CamusSqlPrinter.Instance.Print(generator.GenerateSelect(SelectOptions.Comparable)));
        }
    }

    private static IEnumerable<TableRef> Tables(FromItem from) => from switch
    {
        TableRef table => [table],
        JoinFrom join => Tables(join.Left).Append(join.Right),
        _ => [],
    };

    [Test]
    public void JoinedWritesAppearAndQualifyEveryColumn()
    {
        int updateFrom = 0, deleteUsing = 0, aliasOnly = 0;

        for (ulong seed = 1; seed <= 40; seed++)
        {
            StatementGenerator generator = new(new FuzzRandom(seed), GeneratorProfile.CamusDB);
            generator.GenerateSchema();
            generator.GenerateData();

            for (int i = 0; i < 100; i++)
            {
                (string? qualifier, FromItem? list, IEnumerable<Expr> parts) = generator.GenerateWrite() switch
                {
                    UpdateStmt u => (u.Qualifier, u.From, u.Set.Select(a => a.Value).Append(u.Where)),
                    DeleteStmt d => (d.Qualifier, d.Using, new[] { d.Where }),
                    _ => (null, null, []),
                };
                if (qualifier is null)
                    continue;

                if (list is null)
                    aliasOnly++;
                else if (parts.Count() > 1)
                    updateFrom++;
                else
                    deleteUsing++;

                HashSet<string> names = list is null ? [qualifier] : [qualifier, .. Aliases(list)];
                foreach (Expr part in parts)
                    foreach (ColumnExpr column in OuterColumns(part))
                        Assert.That(column.Source is not null && names.Contains(column.Source), Is.True,
                            $"column {column.Source}.{column.Column} in a joined write");
            }
        }

        Assert.That(updateFrom, Is.GreaterThan(20), "UPDATE … FROM");
        Assert.That(deleteUsing, Is.GreaterThan(20), "DELETE … USING");
        Assert.That(aliasOnly, Is.GreaterThan(5), "a target alias without a list");

        UpdateStmt sample = new("t0", [new Assignment("c1", new ColumnExpr("a0", "c2", FuzzType.Int64))],
            new BinaryExpr(BinaryOp.Eq, new ColumnExpr("u", "c0", FuzzType.Int64), new ColumnExpr("a0", "c0", FuzzType.Int64), FuzzType.Bool),
            "u", new TableRef("t1", "a0"));
        Assert.That(CamusSqlPrinter.Instance.Print(sample), Is.EqualTo("UPDATE t0 AS u SET c1 = a0.c2 FROM t1 AS a0 WHERE (u.c0 = a0.c0)"));
        Expr byTableName = new BinaryExpr(BinaryOp.Eq, new ColumnExpr("t0", "c0", FuzzType.Int64), new ColumnExpr("a0", "c0", FuzzType.Int64), FuzzType.Bool);
        Assert.That(CamusSqlPrinter.Instance.Print(new DeleteStmt("t0", byTableName, null, new TableRef("t1", "a0"))),
            Is.EqualTo("DELETE FROM t0 USING t1 AS a0 WHERE (t0.c0 = a0.c0)"));
    }

    private static IEnumerable<string> Aliases(FromItem from)
    {
        FromItem current = from;
        while (current is JoinFrom join)
        {
            yield return join.Right.Alias;
            current = join.Left;
        }
        if (current is TableRef table)
            yield return table.Alias;
        if (current is ValuesRef values)
            yield return values.Alias;
    }

    /// <summary>The column references of an expression, without entering its subqueries.</summary>
    private static IEnumerable<ColumnExpr> OuterColumns(Expr expression)
    {
        Stack<object> pending = new();
        pending.Push(expression);
        while (pending.Count > 0)
        {
            object node = pending.Pop();
            if (node is ColumnExpr column)
                yield return column;
            if (node is SelectStmt)
                continue;
            foreach (object child in ExprWalker.Children(node))
                pending.Push(child);
        }
    }

    [Test]
    public void AProfileThatRemovesAFormStopsTheGeneratorFromSelectingIt()
    {
        GeneratorProfile profile = GeneratorProfile.CamusDB
            .WithoutOperators(BinaryOp.Concat, BinaryOp.Mod)
            .WithoutOperators(UnaryOp.IsTrue)
            .WithoutFunctions("lower", "substring")
            .WithoutFeatures(ExprFeature.Like, ExprFeature.Case)
            .WithoutFeatures(StatementFeature.WindowFunctions)
            .WithoutJoins(JoinKind.Right)
            .WithoutTypes(FuzzType.Jsonb)
            .Named("restricted");

        List<string> sql = new();
        for (ulong seed = 1; seed <= 40; seed++)
            sql.AddRange(Program(seed, profile, 250));
        string all = string.Join("\n", sql);

        Assert.That(all, Does.Not.Contain(" || "));
        Assert.That(all, Does.Not.Contain(" % "));
        Assert.That(all, Does.Not.Contain(" IS TRUE"));
        Assert.That(all, Does.Not.Contain("lower("));
        Assert.That(all, Does.Not.Contain("substring("));
        Assert.That(all, Does.Not.Contain(" LIKE "));
        Assert.That(all, Does.Not.Contain("(CASE "));
        Assert.That(all, Does.Not.Contain(" OVER ("));
        Assert.That(all, Does.Not.Contain("RIGHT JOIN"));
        Assert.That(all, Does.Not.Contain("jsonb"));

        // The same seeds with the full profile do select them, so the assertions above test the profile.
        string full = string.Join("\n", Enumerable.Range(1, 40).SelectMany(s => Program((ulong)s, GeneratorProfile.CamusDB, 250)));
        Assert.That(full, Does.Contain(" || "));
        Assert.That(full, Does.Contain(" LIKE "));
        Assert.That(full, Does.Contain("(CASE "));
        Assert.That(full, Does.Contain("RIGHT JOIN"));
        Assert.That(full, Does.Contain("jsonb"));
    }

    [Test]
    public void TheTreePrintsAsCamusSql()
    {
        TableDef table = new("t0", [new ColumnDef("c0", FuzzType.Int64, true, true), new ColumnDef("c1", FuzzType.Float64, false, false)]);
        SelectStmt select = new(
            [new SelectItem(new ColumnExpr("a0", "c1", FuzzType.Float64), "o0")],
            new TableRef("t0", "a0", "t0_i0"),
            new BinaryExpr(BinaryOp.Gt, new ColumnExpr("a0", "c1", FuzzType.Float64), new LiteralExpr(FuzzValue.Of(double.NaN)), FuzzType.Bool));

        Assert.That(CamusSqlPrinter.Instance.Print(new CreateTableStmt(table)), Is.EqualTo("CREATE TABLE t0 (c0 int64 PRIMARY KEY, c1 float64)"));
        Assert.That(CamusSqlPrinter.Instance.Print(select),
            Is.EqualTo("SELECT a0.c1 AS o0 FROM t0 AS a0@{FORCE_INDEX=t0_i0} WHERE (a0.c1 > CAST('NaN' AS float64))"));
        Assert.That(CamusSqlPrinter.Instance.Print(new LiteralExpr(FuzzValue.Of(-0.0))), Is.EqualTo("-0.0"));
        Assert.That(CamusSqlPrinter.Instance.Print(new LiteralExpr(FuzzValue.Of(long.MinValue))), Is.EqualTo("-9223372036854775808"));
        Assert.That(CamusSqlPrinter.Instance.Print(new LiteralExpr(FuzzValue.Of(1e300))), Is.EqualTo("1e300"));
        Assert.That(CamusSqlPrinter.Instance.Print(new LiteralExpr(FuzzValue.Null(FuzzType.Date))), Is.EqualTo("CAST(NULL AS date)"));
        Assert.That(CamusSqlPrinter.Instance.Print(new LiteralExpr(FuzzValue.Interval("1 day 2 hours"))), Is.EqualTo("INTERVAL '1 day 2 hours'"));
        Assert.That(CamusSqlPrinter.Instance.Print(new LiteralExpr(FuzzValue.Null(FuzzType.Interval))), Is.EqualTo("CAST(NULL AS interval)"));
        Assert.That(CamusSqlPrinter.Instance.Print(new LiteralExpr(FuzzValue.DateTime("2024-02-29T12:30:00Z"))), Is.EqualTo("CAST('2024-02-29T12:30:00Z' AS datetime)"));
        Assert.That(CamusSqlPrinter.Instance.Print(new InListExpr(new ColumnExpr("a0", "c1", FuzzType.DateTime), [new LiteralExpr(FuzzValue.DateTime("2024-01-01T00:00:00Z"))], false)),
            Is.EqualTo("(a0.c1 IN ('2024-01-01T00:00:00Z'))"));
    }

    /// <summary>
    /// An interval is an expression type only. Over many seeds the generator writes interval literals,
    /// interval arithmetic, datetime columns and the interval functions, but never an interval column.
    /// </summary>
    [Test]
    public void IntervalsAppearInExpressionsButNeverAsAColumn()
    {
        List<string> sql = new();
        for (ulong seed = 1; seed <= 40; seed++)
            sql.AddRange(Program(seed, GeneratorProfile.CamusDB, 250));
        string all = string.Join("\n", sql);

        Assert.Multiple(() =>
        {
            Assert.That(sql.Where(s => s.StartsWith("CREATE TABLE", StringComparison.Ordinal)), Has.None.Contains(" interval"));
            Assert.That(all, Does.Contain("INTERVAL '"));
            Assert.That(all, Does.Contain(" datetime"));
            Assert.That(all, Does.Contain("justify_"));
            Assert.That(all, Does.Contain("make_interval("));
            Assert.That(all, Does.Contain("date_bin("));
            Assert.That(all, Does.Contain("age("));
            Assert.That(all, Does.Contain("date_part("));
        });
    }

    /// <summary>
    /// The profiles to check: each operator, function, expression form, statement form, join kind and
    /// set operation on its own, each removable type, and combinations, among them the one that let
    /// INSERT … SELECT bring back a removed + and COALESCE.
    /// </summary>
    private static IEnumerable<GeneratorProfile> ExclusionProfiles()
    {
        GeneratorProfile full = GeneratorProfile.CamusDB;
        foreach (BinaryOp op in full.BinaryOps.Where(o => o is not (BinaryOp.And or BinaryOp.Or or BinaryOp.Eq)))
            yield return full.WithoutOperators(op).Named("without " + op);
        foreach (UnaryOp op in full.UnaryOps.Where(o => o != UnaryOp.Not))
            yield return full.WithoutOperators(op).Named("without " + op);
        foreach (string function in FunctionCatalog.Names)
            yield return full.WithoutFunctions(function).Named("without " + function);
        foreach (ExprFeature feature in Enum.GetValues<ExprFeature>())
            yield return full.WithoutFeatures(feature).Named("without " + feature);
        foreach (StatementFeature feature in Enum.GetValues<StatementFeature>())
            yield return full.WithoutFeatures(feature).Named("without " + feature);
        foreach (JoinKind kind in Enum.GetValues<JoinKind>())
            yield return full.WithoutJoins(kind).Named("without " + kind + " joins");
        foreach (SetOpKind kind in Enum.GetValues<SetOpKind>())
            yield return full.WithoutSetOperations(kind).Named("without " + kind);
        foreach (FuzzType type in new[] { FuzzType.Float64, FuzzType.Numeric, FuzzType.String, FuzzType.Date, FuzzType.DateTime, FuzzType.Jsonb, FuzzType.Interval })
            yield return full.WithoutTypes(type).Named("without " + type);

        yield return full.WithoutOperators(BinaryOp.Add).WithoutFeatures(ExprFeature.Coalesce).Named("without + and COALESCE");
        yield return full.WithoutJoins(Enum.GetValues<JoinKind>()).Named("without any join kind");
        yield return full.WithoutFeatures(Enum.GetValues<ExprFeature>()).Named("without any expression feature");
        yield return full.WithoutOperators(BinaryOp.Add, BinaryOp.Sub, BinaryOp.Mul, BinaryOp.Div, BinaryOp.Mod).Named("without arithmetic");
        yield return full.WithoutOperators(BinaryOp.Lt, BinaryOp.Le, BinaryOp.Gt, BinaryOp.Ge, BinaryOp.Ne).Named("without inequalities");
    }

    /// <summary>
    /// A profile is a contract for the model and reference consumers: no generated tree may hold a form
    /// the profile removed, the helper expressions the generator adds on its own included. The check
    /// walks the whole tree (<see cref="ProfileValidator"/>), for every statement kind the generator
    /// makes, over many seeds.
    /// </summary>
    [Test]
    public void EveryProfileExclusionHoldsOnTheWholeTree()
    {
        List<string> problems = new();
        int profiles = 0;
        foreach (GeneratorProfile profile in ExclusionProfiles())
        {
            profiles++;
            bool reported = false;
            for (ulong seed = 1; seed <= 12 && !reported; seed++)
            {
                StatementGenerator generator = new(new FuzzRandom(seed), profile);
                List<Statement> statements = generator.GenerateSchema();
                statements.AddRange(generator.GenerateData());
                for (int i = 0; i < 60; i++)
                {
                    statements.Add(generator.GenerateStatement());
                    statements.Add(generator.GenerateWrite());
                    statements.Add(generator.GenerateSelect(SelectOptions.Comparable));
                }
                foreach (Statement statement in statements)
                {
                    List<string> violations = ProfileValidator.Violations(statement, profile);
                    if (violations.Count == 0)
                        continue;
                    // The first violation of each profile, so one run shows every profile that leaks.
                    string text = CamusSqlPrinter.Instance.Print(statement);
                    problems.Add($"{profile.Name}, seed {seed}: {string.Join(", ", violations.Distinct())}\n    {(text.Length > 300 ? text[..300] + " …" : text)}");
                    reported = true;
                    break;
                }
            }
        }
        Assert.That(profiles, Is.GreaterThan(50));
        Assert.That(problems, Is.Empty, string.Join("\n", problems));

        // The validator sees the forms: the full profile, checked against a profile without them, has violations.
        GeneratorProfile none = GeneratorProfile.CamusDB.WithoutFeatures(Enum.GetValues<ExprFeature>()).WithoutFeatures(Enum.GetValues<StatementFeature>());
        StatementGenerator full = new(new FuzzRandom(1), GeneratorProfile.CamusDB);
        List<Statement> sample = full.GenerateSchema();
        sample.AddRange(full.GenerateData());
        for (int i = 0; i < 200; i++)
            sample.Add(full.GenerateStatement());
        Assert.That(sample.Sum(s => ProfileValidator.Violations(s, none).Count), Is.GreaterThan(0));
    }

    /// <summary>
    /// At least 90 % of 10,000 generated statements run with no error, and none of them, the setup of
    /// each block included, is a finding of the error-classification rule (known issues are not
    /// findings). Each block of 100 statements runs on a fresh database with a new schema and new data.
    /// </summary>
    [Test]
    public async Task NinetyPercentOfGeneratedStatementsRunWithoutAnError()
    {
        const int total = 10_000;
        const int perDatabase = 100;

        await using FuzzEngine engine = await FuzzEngine.StartAsync();
        int succeeded = 0;
        int run = 0;
        int intervalSucceeded = 0;
        int intervalRun = 0;
        Dictionary<string, int> errors = new(StringComparer.Ordinal);
        Dictionary<string, string> samples = new(StringComparer.Ordinal);
        List<string> unclassified = new();

        for (ulong seed = 1; run < total; seed++)
        {
            StatementGenerator generator = new(new FuzzRandom(FuzzRandom.DeriveSeed(20261102, (long)seed)), GeneratorProfile.CamusDB);
            List<SetupStatement> setup = generator.GenerateSchema().Concat(generator.GenerateData()).Select(SetupStatement.Of).ToList();

            FuzzArm database = await engine.CreateDatabaseAsync();
            SqlRunner runner = new(database, TimeSpan.FromSeconds(20));
            foreach (SetupOutcome outcome in await SetupRunner.RunAsync(runner, setup, CamusSqlPrinter.Instance))
            {
                // The setup is generated too: a defect there is a finding, and a refused CREATE leaves the
                // block without the schema its statements assume.
                if (ErrorClassifier.Classify(outcome.Outcome) == ErrorVerdict.Finding || (outcome.Statement.Route == StatementRoute.Ddl && !outcome.Outcome.IsSuccess))
                    unclassified.Add("setup: " + outcome.Outcome + "\n    " + outcome.Outcome.Sql);
            }

            for (int i = 0; i < perDatabase && run < total; i++, run++)
            {
                Statement statement = generator.GenerateStatement();
                StatementOutcome outcome = await runner.ExecuteAsync(SetupStatement.RouteOf(statement), CamusSqlPrinter.Instance.Print(statement));
                bool hasInterval = ExprWalker.Nodes(statement).OfType<Expr>().Any(e => e.Type == FuzzType.Interval);
                if (hasInterval)
                    intervalRun++;
                if (outcome.IsSuccess && outcome.WireError is null)
                {
                    succeeded++;
                    if (hasInterval)
                        intervalSucceeded++;
                    continue;
                }

                string key = outcome.Kind + " " + outcome.ErrorCode + ": " + Shape(outcome.ErrorMessage);
                errors[key] = errors.GetValueOrDefault(key) + 1;
                samples.TryAdd(key, outcome.Sql.Length > 400 ? outcome.Sql[..400] + " …" : outcome.Sql);
                if (ErrorClassifier.Classify(outcome) == ErrorVerdict.Finding)
                    unclassified.Add(outcome + "\n    " + outcome.Sql);
            }

            await engine.DropDatabaseAsync(database.DatabaseName);
        }

        double rate = 100.0 * succeeded / run;
        double intervalRate = intervalRun == 0 ? 0 : 100.0 * intervalSucceeded / intervalRun;
        string report = $"{succeeded} of {run} statements ({rate:0.0} %) ran with no error; "
            + $"{intervalSucceeded} of the {intervalRun} statements with an interval expression ({intervalRate:0.0} %).\n"
            + string.Join("\n", errors.OrderByDescending(kv => kv.Value).Take(25).Select(kv => $"  {kv.Value,5}x {kv.Key}\n         e.g. {samples[kv.Key]}"))
            + (unclassified.Count > 0 ? $"\nfindings ({unclassified.Count}):\n  " + string.Join("\n  ", unclassified.Take(20)) : "");
        TestContext.Progress.WriteLine(report);
        Assert.That(rate, Is.GreaterThanOrEqualTo(90.0), report);
        // Intervals have no column, so they come from expressions only. This proves that the oracles see
        // enough of them, and that most of them run: an interval statement that always fails checks nothing.
        // Measured 2026-11-12 with this seed: 1,207 of 10,000 statements, 95.4 % of them with no error.
        Assert.That(intervalRun, Is.GreaterThanOrEqualTo(500), report);
        Assert.That(intervalRate, Is.GreaterThanOrEqualTo(85.0), report);
        // A defect is a failure on its own, whatever the rate: a few internal-code outcomes in 10,000
        // statements would not move the rate, and must not hide in it.
        Assert.That(unclassified, Is.Empty, report);
    }

    private static string Shape(string? message)
    {
        if (string.IsNullOrEmpty(message))
            return "";
        string text = System.Text.RegularExpressions.Regex.Replace(message, "'[^']*'", "'…'");
        text = System.Text.RegularExpressions.Regex.Replace(text, "\\d+", "N");
        return text.Length > 140 ? text[..140] : text;
    }
}
