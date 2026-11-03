/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.SqlFuzz.Tests.Generator;

/// <summary>The generated schema of one round: the tables, their indexes, and which STRING columns hold large values.</summary>
internal sealed class SchemaState
{
    public List<TableDef> Tables { get; } = new();

    public Dictionary<string, List<IndexDef>> Indexes { get; } = new(StringComparer.Ordinal);

    /// <summary>STRING columns that sometimes hold a value long enough to go out of line, as "table.column".</summary>
    public HashSet<string> LargeColumns { get; } = new(StringComparer.Ordinal);

    /// <summary>The next primary-key value of each table with an INT64 key. New rows take fresh keys, so most INSERTs do not fail on a duplicate.</summary>
    public Dictionary<string, long> NextKey { get; } = new(StringComparer.Ordinal);

    /// <summary>
    /// The table that <see cref="StatementGenerator.GenerateData"/> gave 100 to 200 rows, or null. A FROM
    /// clause then has at most two sources and names this table at most once: its cross join with itself
    /// gives some 40,000 pairs, and a probe for each passes the statement time limit without any defect.
    /// </summary>
    public string? SizedTable { get; set; }

    /// <summary>A counter for fresh values of unique-index columns.</summary>
    public long NextUnique { get; set; } = 1;

    /// <summary>True when <paramref name="column"/> of <paramref name="table"/> is a key column of a unique index.</summary>
    public bool IsUniqueColumn(string table, string column) =>
        IndexesOf(table).Any(i => i.Unique && i.Columns.Contains(column));

    public TableDef Table(string name) => Tables.First(t => t.Name == name);

    public IReadOnlyList<IndexDef> IndexesOf(string table) => Indexes.TryGetValue(table, out List<IndexDef>? list) ? list : [];
}

/// <summary>Limits for one generated SELECT, so an oracle gets a query whose result it can compare.</summary>
internal sealed record SelectOptions
{
    /// <summary>The default for a free-form statement: every form the profile allows.</summary>
    public static SelectOptions Any { get; } = new();

    /// <summary>For an oracle: no LIMIT, window function or set operation, whose rows depend on order or ties.</summary>
    public static SelectOptions Comparable { get; } = new() { AllowLimit = false, AllowWindow = false, AllowSetOp = false, AllowAggregates = false, AllowDistinct = false };

    public bool AllowLimit { get; init; } = true;
    public bool AllowWindow { get; init; } = true;
    public bool AllowSetOp { get; init; } = true;
    public bool AllowAggregates { get; init; } = true;
    public bool AllowDistinct { get; init; } = true;
    public bool AllowOuterJoins { get; init; } = true;
    public bool AllowHints { get; init; } = true;
    public int MaxTables { get; init; } = 3;
}

/// <summary>
/// Makes statement trees: the schema, the data, and a stream of SELECT and DML statements. It never
/// writes SQL text; <see cref="Printing.ISqlPrinter"/> does.
///
/// <para>Rules that come from the CamusDB SQL surface:</para>
/// <list type="bullet">
/// <item>Every table has a primary key (<c>c0</c>), because CamusDB refuses a table without one.</item>
/// <item>An UPDATE or DELETE always has a WHERE. A full-table statement is <c>WHERE TRUE</c>.</item>
/// <item>A RIGHT JOIN has a single table on its left, because CamusDB refuses a RIGHT JOIN after another join.</item>
/// <item>No JSONB column is in an ordered index or a primary key.</item>
/// </list>
/// </summary>
internal sealed class StatementGenerator
{
    private readonly FuzzRandom random;
    private readonly GeneratorProfile profile;
    private readonly ExpressionGenerator expressions;
    private readonly ValueGenerator values;

    public StatementGenerator(FuzzRandom random, GeneratorProfile profile, SchemaState? state = null)
    {
        this.random = random;
        this.profile = profile;
        expressions = new ExpressionGenerator(random, profile);
        values = expressions.Values;
        State = state ?? new SchemaState();
    }

    public SchemaState State { get; }

    public ExpressionGenerator Expressions => expressions;

    public FuzzRandom Random => random;

    public GeneratorProfile Profile => profile;

    // -----------------------------------------------------------------------------------------
    // Schema
    // -----------------------------------------------------------------------------------------

    /// <summary>1 to 3 tables with their indexes, as CREATE TABLE and CREATE INDEX statements.</summary>
    public List<Statement> GenerateSchema()
    {
        List<Statement> statements = new();
        int tableCount = random.Between(1, 3);
        for (int t = 0; t < tableCount; t++)
        {
            TableDef table = GenerateTable("t" + t);
            State.Tables.Add(table);
            State.Indexes[table.Name] = new List<IndexDef>();
            statements.Add(new CreateTableStmt(table));

            int indexCount = random.Between(0, 4);
            for (int i = 0; i < indexCount; i++)
            {
                IndexDef? index = GenerateIndex(table, table.Name + "_i" + i);
                if (index is null)
                    continue;
                State.Indexes[table.Name].Add(index);
                statements.Add(new CreateIndexStmt(table.Name, index));
            }
        }
        return statements;
    }

    private TableDef GenerateTable(string name)
    {
        FuzzType keyType = profile.Allows(FuzzType.String) && random.Chance(0.15) ? FuzzType.String : FuzzType.Int64;
        List<ColumnDef> columns = [new ColumnDef("c0", keyType, NotNull: true, PrimaryKey: true)];

        List<FuzzType> types = profile.Types.ToList();
        int count = random.Between(2, 6);
        for (int i = 1; i <= count; i++)
        {
            FuzzType type = random.Pick(types);
            columns.Add(new ColumnDef("c" + i, type, NotNull: random.Chance(0.2), PrimaryKey: false));
            if (type == FuzzType.String && profile.Allows(StatementFeature.LargeValues) && random.Chance(0.25))
                State.LargeColumns.Add(name + ".c" + i);
        }

        if (keyType == FuzzType.Int64)
            State.NextKey[name] = 1;
        return new TableDef(name, columns);
    }

    private IndexDef? GenerateIndex(TableDef table, string name)
    {
        List<ColumnDef> orderable = table.Columns.Where(c => !c.PrimaryKey && FuzzTypes.IsOrderable(c.Type)).ToList();
        if (orderable.Count == 0)
            return null;

        int keyCount = orderable.Count > 1 && random.Chance(0.35) ? 2 : 1;
        List<string> keys = random.Shuffled(orderable).Take(keyCount).Select(c => c.Name).ToList();

        // A BOOL has two values, so a unique index on it refuses most rows.
        bool unique = profile.Allows(StatementFeature.UniqueIndexes) && random.Chance(0.25)
            && keys.All(k => table.Column(k).Type != FuzzType.Bool);

        List<string> include = new();
        if (profile.Allows(StatementFeature.CoveringIndexes) && random.Chance(0.3))
        {
            List<ColumnDef> rest = table.Columns.Where(c => !c.PrimaryKey && !keys.Contains(c.Name)).ToList();
            if (rest.Count > 0)
                include.Add(random.Pick(rest).Name);
        }

        return new IndexDef(name, keys, unique, include);
    }

    // -----------------------------------------------------------------------------------------
    // Data
    // -----------------------------------------------------------------------------------------

    /// <summary>
    /// The rows of every table, as INSERT statements of up to 10 rows. Some tables get 0 or 1 row.
    /// Rows that would duplicate a key of a unique index are left out, so the load itself does not
    /// fail. <paramref name="sizedTable"/> names one table that gets 100 to 200 rows instead: a join
    /// planner picks a merge join, and a cost model weighs a hash join against an index nested loop,
    /// only for an outer side above about 100 rows. Only one table grows, so a three-table join stays
    /// below the statement time limit.
    /// </summary>
    public List<Statement> GenerateData(string? sizedTable = null)
    {
        State.SizedTable = sizedTable;
        List<Statement> statements = new();
        foreach (TableDef table in State.Tables)
        {
            int rows = table.Name == sizedTable ? random.Between(100, 200) : random.Next(10) switch
            {
                0 => 0,
                1 => 1,
                < 6 => random.Between(2, 12),
                _ => random.Between(12, 40),
            };

            List<IReadOnlyList<Expr>> pending = GenerateRows(table, rows, boundaries: true, initialLoad: true);
            int batch = profile.Allows(StatementFeature.MultiRowInsert) ? 10 : 1;
            for (int start = 0; start < pending.Count; start += batch)
                statements.Add(new InsertStmt(table.Name, table.Columns.Select(c => c.Name).ToList(), pending.Skip(start).Take(batch).ToList()));
        }
        return statements;
    }

    /// <summary>
    /// <paramref name="count"/> new rows of <paramref name="table"/> with fresh primary keys, in the
    /// column order of the table. Rows that repeat a unique-index key of another new row are dropped.
    /// For the <paramref name="initialLoad"/> no large value goes into an INCLUDE column: its payload
    /// passes the per-entry limit (<c>CADB0408</c>), the load of ten rows fails, and the round's checks
    /// run on a table with fewer rows. Later INSERT statements still meet the limit.
    /// </summary>
    public List<IReadOnlyList<Expr>> GenerateRows(TableDef table, int count, bool boundaries, bool initialLoad = false)
    {
        HashSet<string> included = initialLoad
            ? State.IndexesOf(table.Name).SelectMany(i => i.Include).ToHashSet(StringComparer.Ordinal)
            : [];
        List<IReadOnlyList<Expr>> rows = new(count);
        List<HashSet<string>> seen = State.IndexesOf(table.Name).Where(i => i.Unique).Select(_ => new HashSet<string>(StringComparer.Ordinal)).ToList();
        List<IndexDef> uniques = State.IndexesOf(table.Name).Where(i => i.Unique).ToList();

        for (int r = 0; r < count; r++)
        {
            List<Expr> row = new(table.Columns.Count);
            Dictionary<string, FuzzValue> cells = new(StringComparer.Ordinal);
            foreach (ColumnDef column in table.Columns)
            {
                FuzzValue value = column.PrimaryKey
                    ? NextKey(table)
                    : State.IsUniqueColumn(table.Name, column.Name) && random.Chance(0.85)
                        ? FreshValue(column.Type)
                        : State.LargeColumns.Contains(table.Name + "." + column.Name) && !included.Contains(column.Name) && random.Chance(0.3)
                            ? values.LargeString()
                            : values.Next(column.Type, nullable: !column.NotNull, boundaries);
                cells[column.Name] = value;
                row.Add(new LiteralExpr(value));
            }

            bool duplicate = false;
            for (int u = 0; u < uniques.Count && !duplicate; u++)
            {
                if (uniques[u].Columns.Any(c => cells[c].IsNull))
                    continue;
                string key = string.Join("\u0001", uniques[u].Columns.Select(c => KeyText(cells[c])));
                duplicate = !seen[u].Add(key);
            }

            if (!duplicate)
                rows.Add(row);
        }
        return rows;
    }

    /// <summary>The text of a value as an index key compares it: -0 equals 0, and a number keeps its value, not its spelling.</summary>
    private static string KeyText(FuzzValue value) => value.Type switch
    {
        FuzzType.Float64 => value.Float == 0 ? "0" : value.Float.ToString("R", System.Globalization.CultureInfo.InvariantCulture),
        FuzzType.Numeric => decimal.TryParse(value.Text, System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out decimal d) ? d.ToString("G29", System.Globalization.CultureInfo.InvariantCulture) : value.Text!,
        FuzzType.Int64 => value.Int.ToString(System.Globalization.CultureInfo.InvariantCulture),
        FuzzType.Bool => value.Bool ? "t" : "f",
        _ => value.Text ?? "",
    };

    /// <summary>
    /// A value that no earlier fresh value of this round has, for a unique-index column. Most new rows
    /// take one, so few writes fail on a duplicate; the rest take pool values, so some still do.
    /// </summary>
    public FuzzValue FreshValue(FuzzType type)
    {
        long n = State.NextUnique++;
        return type switch
        {
            FuzzType.Int64 => FuzzValue.Of(1000 + n),
            FuzzType.Float64 => FuzzValue.Of(1000.5 + n),
            FuzzType.Numeric => FuzzValue.Numeric((1000 + n).ToString(System.Globalization.CultureInfo.InvariantCulture) + ".25"),
            FuzzType.String => FuzzValue.String("u" + n.ToString(System.Globalization.CultureInfo.InvariantCulture)),
            FuzzType.Date => FuzzValue.Date(new DateTime(2100, 1, 1).AddDays(n).ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture)),
            _ => values.Next(type, nullable: false, boundaries: false),
        };
    }

    private FuzzValue NextKey(TableDef table)
    {
        if (table.PrimaryKey.Type == FuzzType.String)
        {
            long n = State.NextKey.TryGetValue(table.Name, out long k) ? k : 1;
            State.NextKey[table.Name] = n + 1;
            return FuzzValue.String(random.Chance(0.1) ? "k" + n + "é" : "k" + n);
        }

        long next = State.NextKey[table.Name];
        State.NextKey[table.Name] = next + random.Between(1, 3);
        if (profile.Allows(ExprFeature.BoundaryLiterals) && random.Chance(0.04))
            return FuzzValue.Of(random.NextBool() ? long.MaxValue - next : long.MinValue + next);
        return FuzzValue.Of(random.Chance(0.15) ? -next : next);
    }

    // -----------------------------------------------------------------------------------------
    // FROM and SELECT
    // -----------------------------------------------------------------------------------------

    /// <summary>
    /// A FROM clause of 1 to <see cref="SelectOptions.MaxTables"/> tables and the scope of its
    /// columns. A join takes an equality on two comparable columns most of the time, so the join
    /// operators see matches, NULL keys and mixed-type keys.
    /// </summary>
    public (FromItem From, ExprScope Scope) GenerateFrom(SelectOptions options)
    {
        int tableCount = 1;
        if (profile.Allows(StatementFeature.Joins) && options.MaxTables > 1)
        {
            int roll = random.Next(10);
            tableCount = roll < 5 ? 1 : roll < 9 ? 2 : 3;
            tableCount = Math.Min(tableCount, State.SizedTable is null ? options.MaxTables : Math.Min(options.MaxTables, 2));
        }

        List<ScopeColumn> columns = new();
        TableDef first = random.Pick(State.Tables);
        bool sizedUsed = first.Name == State.SizedTable;
        FromItem from = TableSource(first, "a0", options, columns);

        for (int i = 1; i < tableCount; i++)
        {
            List<JoinKind> kinds = Enum.GetValues<JoinKind>().Where(k => profile.Allows(k)).ToList();
            // In a chain of three tables only one join may lack an equality, so the row count stays near
            // the table sizes and below the statement time limit.
            bool needsEquality = tableCount == 3 && i == 2 && from is JoinFrom { Kind: JoinKind.Cross };
            if (needsEquality)
                kinds.Remove(JoinKind.Cross);
            if (!options.AllowOuterJoins)
                kinds.RemoveAll(k => k is JoinKind.Left or JoinKind.Right);
            if (from is not TableRef)
                kinds.Remove(JoinKind.Right);
            // No join kind the profile and this position allow: the FROM clause stops here.
            if (kinds.Count == 0)
                break;
            JoinKind kind = random.Pick(kinds);

            List<ScopeColumn> rightColumns = new();
            List<TableDef> candidates = sizedUsed && State.Tables.Count > 1 ? State.Tables.Where(t => t.Name != State.SizedTable).ToList() : State.Tables;
            TableDef next = random.Pick(candidates);
            if (sizedUsed && next.Name == State.SizedTable)
                break;
            sizedUsed |= next.Name == State.SizedTable;
            TableRef right = TableSource(next, "a" + i, options, rightColumns);
            List<ScopeColumn> joined = columns.Concat(rightColumns).ToList();

            Expr? on = null;
            if (kind != JoinKind.Cross)
            {
                ExprScope onScope = new(joined, State.Tables);
                on = EquiJoin(columns, rightColumns, force: needsEquality || tableCount == 3) ?? expressions.Predicate(onScope);
                if (random.Chance(0.25))
                    on = new BinaryExpr(BinaryOp.And, on, expressions.Predicate(onScope), FuzzType.Bool);
            }

            from = new JoinFrom(from, kind, right, on);
            columns = joined;
        }

        return (from, new ExprScope(columns, State.Tables));
    }

    private TableRef TableSource(TableDef table, string alias, SelectOptions options, List<ScopeColumn> columns)
    {
        columns.AddRange(table.Columns.Select(c => new ScopeColumn(alias, c.Name, c.Type)));
        string? hint = null;
        if (options.AllowHints && profile.Allows(StatementFeature.IndexHints) && random.Chance(0.12))
        {
            List<string> names = State.IndexesOf(table.Name).Select(i => i.Name).Append("pk").ToList();
            hint = random.Pick(names);
        }
        return new TableRef(table.Name, alias, hint);
    }

    private Expr? EquiJoin(List<ScopeColumn> left, List<ScopeColumn> right, bool force = false)
    {
        if (!force && !random.Chance(0.75))
            return null;
        List<(ScopeColumn L, ScopeColumn R)> pairs = (from l in left
                                                       from r in right
                                                       where FuzzTypes.AreComparable(l.Type, r.Type)
                                                       select (l, r)).ToList();
        if (pairs.Count == 0)
            return null;
        // Equal types first: they are the common join, and they match more rows.
        List<(ScopeColumn L, ScopeColumn R)> same = pairs.Where(p => p.L.Type == p.R.Type).ToList();
        (ScopeColumn a, ScopeColumn b) = same.Count > 0 && random.Chance(0.8) ? random.Pick(same) : random.Pick(pairs);
        return new BinaryExpr(BinaryOp.Eq, new ColumnExpr(a.Source, a.Column, a.Type), new ColumnExpr(b.Source, b.Column, b.Type), FuzzType.Bool);
    }

    /// <summary>1 to 4 typed output columns over <paramref name="scope"/>, most of them plain columns.</summary>
    public List<SelectItem> GenerateProjection(ExprScope scope, int min = 1, int max = 4)
    {
        int count = random.Between(min, max);
        List<SelectItem> items = new(count);
        for (int i = 0; i < count; i++)
        {
            if (random.Chance(0.7))
            {
                ScopeColumn column = random.Pick(scope.Columns);
                items.Add(new SelectItem(new ColumnExpr(column.Source, column.Column, column.Type), "o" + i));
            }
            else
            {
                FuzzType type = random.Pick(profile.Types.ToList());
                items.Add(new SelectItem(expressions.Generate(type, scope, 1), "o" + i));
            }
        }
        return items;
    }

    /// <summary>A free-form SELECT: any form the profile and <paramref name="options"/> allow.</summary>
    public SelectStmt GenerateSelect(SelectOptions options)
    {
        (FromItem from, ExprScope scope) = GenerateFrom(options);
        Expr? where = random.Chance(0.75) ? expressions.Predicate(scope.ForWhere()) : null;

        if (options.AllowAggregates && profile.Allows(StatementFeature.GroupBy) && random.Chance(0.2))
            return GroupedSelect(from, scope, where);

        List<SelectItem> items = random.Chance(0.12) ? [] : GenerateProjection(scope);

        if (options.AllowWindow && profile.Allows(StatementFeature.WindowFunctions) && items.Count > 0 && random.Chance(0.08))
        {
            ScopeColumn partition = random.Pick(scope.Columns.Where(c => FuzzTypes.IsOrderable(c.Type)).ToList());
            ScopeColumn order = random.Pick(scope.Columns.Where(c => FuzzTypes.IsOrderable(c.Type)).ToList());
            WindowFunc func = random.Pick(new[] { WindowFunc.RowNumber, WindowFunc.Rank, WindowFunc.DenseRank, WindowFunc.Count });
            items.Add(new SelectItem(new WindowExpr(func, null,
                [new ColumnExpr(partition.Source, partition.Column, partition.Type)],
                [new OrderItem(new ColumnExpr(order.Source, order.Column, order.Type), random.NextBool())], FuzzType.Int64), "w"));
        }

        bool distinct = options.AllowDistinct && profile.Allows(StatementFeature.Distinct) && random.Chance(0.15);

        List<OrderItem>? orderBy = null;
        long? limit = null;
        long? offset = null;
        if (options.AllowLimit && profile.Allows(StatementFeature.OrderByLimit) && items.Count > 0 && random.Chance(0.15))
        {
            // An integer literal in ORDER BY is a column position, not a value, so literals are left out.
            orderBy = items.Where(i => FuzzTypes.IsOrderable(i.Expr.Type) && i.Expr is not LiteralExpr).Select(i => new OrderItem(i.Expr, random.NextBool())).ToList();
            if (orderBy.Count == 0)
                orderBy = null;
            limit = random.Between(0, 10);
            offset = random.Chance(0.3) ? random.Between(0, 3) : null;
        }

        SetOperation? setOp = null;
        if (orderBy is null)
        {
            limit = null;
            offset = null;
        }

        if (options.AllowSetOp && profile.Allows(StatementFeature.SetOperations) && items.Count > 0 && orderBy is null && random.Chance(0.1))
        {
            List<SetOpKind> kinds = profile.SetOps.ToList();
            if (kinds.Count > 0)
                setOp = new SetOperation(random.Pick(kinds), MatchingSelect(items.Select(i => i.Expr.Type).ToList()));
        }

        return new SelectStmt(items, from, where, distinct, OrderBy: orderBy, Limit: limit, Offset: offset, SetOp: setOp);
    }

    /// <summary>A one-table SELECT whose output columns have exactly <paramref name="types"/>, for the right operand of a set operation.</summary>
    private SelectStmt MatchingSelect(IReadOnlyList<FuzzType> types)
    {
        (FromItem from, ExprScope scope) = GenerateFrom(SelectOptions.Comparable with { MaxTables = 1 });
        List<SelectItem> items = types.Select((t, i) => new SelectItem(expressions.Leaf(t, scope), "o" + i)).ToList();
        Expr? where = random.Chance(0.6) ? expressions.Predicate(scope) : null;
        return new SelectStmt(items, from, where);
    }

    private SelectStmt GroupedSelect(FromItem from, ExprScope scope, Expr? where)
    {
        List<ScopeColumn> groupable = scope.Columns.Where(c => FuzzTypes.IsOrderable(c.Type)).ToList();
        List<ScopeColumn> keys = random.Shuffled(groupable).Take(random.Between(1, 2)).ToList();
        List<Expr> groupBy = keys.Select(k => (Expr)new ColumnExpr(k.Source, k.Column, k.Type)).ToList();

        List<SelectItem> items = groupBy.Select((g, i) => new SelectItem(g, "g" + i)).ToList();
        int aggregates = random.Between(1, 3);
        for (int i = 0; i < aggregates; i++)
            items.Add(new SelectItem(Aggregate(scope), "x" + i));

        Expr? having = null;
        if (profile.Allows(StatementFeature.Having) && random.Chance(0.4))
            having = HavingPredicate(scope, groupBy);

        return new SelectStmt(items, from, where, GroupBy: groupBy, Having: having);
    }

    /// <summary>An aggregate over a column of <paramref name="scope"/>: COUNT(*), COUNT, SUM and AVG of numbers, MIN and MAX of orderable types.</summary>
    public AggregateExpr Aggregate(ExprScope scope)
    {
        List<ScopeColumn> numbers = scope.Columns.Where(c => FuzzTypes.IsNumber(c.Type)).ToList();
        List<ScopeColumn> orderable = scope.Columns.Where(c => FuzzTypes.IsOrderable(c.Type)).ToList();

        int roll = random.Next(6);
        if (roll == 0 || orderable.Count == 0)
            return new AggregateExpr(AggregateFunc.CountStar, null, FuzzType.Int64);
        if (roll == 1)
        {
            ScopeColumn any = random.Pick(scope.Columns);
            return new AggregateExpr(AggregateFunc.Count, new ColumnExpr(any.Source, any.Column, any.Type), FuzzType.Int64);
        }
        if (roll is 2 or 3 && numbers.Count > 0)
        {
            ScopeColumn n = random.Pick(numbers);
            if (roll == 3 && n.Type == FuzzType.Float64)
                return new AggregateExpr(AggregateFunc.Avg, new ColumnExpr(n.Source, n.Column, n.Type), FuzzType.Float64);
            return new AggregateExpr(AggregateFunc.Sum, new ColumnExpr(n.Source, n.Column, n.Type), n.Type);
        }

        ScopeColumn o = random.Pick(orderable);
        return new AggregateExpr(random.NextBool() ? AggregateFunc.Min : AggregateFunc.Max, new ColumnExpr(o.Source, o.Column, o.Type), o.Type);
    }

    /// <summary>A HAVING predicate: an aggregate compared with a literal, or a predicate over the group keys.</summary>
    public Expr HavingPredicate(ExprScope scope, IReadOnlyList<Expr> groupBy)
    {
        List<BinaryOp> comparisons = new[] { BinaryOp.Eq, BinaryOp.Ne, BinaryOp.Lt, BinaryOp.Le, BinaryOp.Gt, BinaryOp.Ge }.Where(profile.Allows).ToList();
        if (comparisons.Count > 0 && random.Chance(0.6))
        {
            AggregateExpr aggregate = Aggregate(scope);
            BinaryOp op = random.Pick(comparisons);
            Expr literal = aggregate.Type == FuzzType.Int64 && aggregate.Func is AggregateFunc.CountStar or AggregateFunc.Count
                ? new LiteralExpr(FuzzValue.Of((long)random.Between(0, 3)))
                : expressions.Literal(aggregate.Type);
            return new BinaryExpr(op, aggregate, literal, FuzzType.Bool);
        }

        List<ScopeColumn> keys = groupBy.OfType<ColumnExpr>().Select(c => new ScopeColumn(c.Source, c.Column, c.ColumnType)).ToList();
        return expressions.Predicate(new ExprScope(keys, []));
    }

    // -----------------------------------------------------------------------------------------
    // DML
    // -----------------------------------------------------------------------------------------

    /// <summary>One statement of the free-form stream: mostly SELECT, with INSERT, UPDATE, DELETE, UPSERT, INSERT … SELECT and ANALYZE.</summary>
    public Statement GenerateStatement()
    {
        int roll = random.Next(100);
        Statement? statement = roll switch
        {
            < 60 => GenerateSelect(SelectOptions.Any),
            < 70 => GenerateInsert(),
            < 80 => GenerateUpdate(),
            < 86 => GenerateDelete(),
            < 92 => GenerateUpsert(),
            < 97 => GenerateInsertSelect(),
            _ => profile.Allows(StatementFeature.Analyze) ? new AnalyzeStmt(random.Pick(State.Tables).Name) : null,
        };
        return statement ?? GenerateSelect(SelectOptions.Any);
    }

    /// <summary>A write statement: INSERT, UPDATE, DELETE, UPSERT or INSERT … SELECT.</summary>
    public Statement GenerateWrite()
    {
        for (int attempt = 0; attempt < 8; attempt++)
        {
            Statement? statement = random.Next(5) switch
            {
                0 => GenerateInsert(),
                1 => GenerateUpdate(),
                2 => GenerateDelete(),
                3 => GenerateUpsert(),
                _ => GenerateInsertSelect(),
            };
            if (statement is not null)
                return statement;
        }
        return GenerateInsert();
    }

    public InsertStmt GenerateInsert()
    {
        TableDef table = random.Pick(State.Tables);
        int count = profile.Allows(StatementFeature.MultiRowInsert) ? random.Between(1, 5) : 1;
        List<IReadOnlyList<Expr>> rows = GenerateRows(table, count, boundaries: true);
        if (rows.Count == 0)
            rows = GenerateRows(table, 1, boundaries: false);
        return new InsertStmt(table.Name, table.Columns.Select(c => c.Name).ToList(), rows);
    }

    public UpdateStmt? GenerateUpdate()
    {
        if (!profile.Allows(StatementFeature.Update))
            return null;
        TableDef table = random.Pick(State.Tables);
        List<ColumnDef> targets = table.Columns.Where(c => !c.PrimaryKey).ToList();
        if (targets.Count == 0)
            return null;

        ExprScope scope = DmlScope(table);
        List<Assignment> set = new();
        foreach (ColumnDef column in random.Shuffled(targets).Take(random.Between(1, Math.Min(2, targets.Count))))
            set.Add(new Assignment(column.Name, State.IsUniqueColumn(table.Name, column.Name) && random.Chance(0.85)
                ? new LiteralExpr(FreshValue(column.Type))
                : AssignedValue(column, scope)));

        return new UpdateStmt(table.Name, set, DmlWhere(scope));
    }

    public DeleteStmt? GenerateDelete()
    {
        if (!profile.Allows(StatementFeature.Delete))
            return null;
        TableDef table = random.Pick(State.Tables);
        return new DeleteStmt(table.Name, DmlWhere(DmlScope(table)));
    }

    public InsertStmt? GenerateUpsert()
    {
        if (!profile.Allows(StatementFeature.Upsert))
            return null;
        TableDef table = random.Pick(State.Tables);

        // Some rows take an existing key, so the conflict arm runs. Small keys exist in most tables.
        List<IReadOnlyList<Expr>> rows = GenerateRows(table, profile.Allows(StatementFeature.MultiRowInsert) ? random.Between(1, 3) : 1, boundaries: false);
        if (rows.Count == 0)
            return null;
        if (table.PrimaryKey.Type == FuzzType.Int64)
        {
            rows = rows.Select(row =>
            {
                if (!random.Chance(0.6))
                    return row;
                List<Expr> copy = row.ToList();
                copy[0] = new LiteralExpr(FuzzValue.Of((long)random.Between(1, 20)));
                return (IReadOnlyList<Expr>)copy;
            }).ToList();
            // Two rows of one statement with the same key are a separate error; keep the first.
            rows = rows.GroupBy(r => ((LiteralExpr)r[0]).Value.Int).Select(g => g.First()).ToList();
        }

        IReadOnlyList<Assignment>? doUpdate = null;
        if (random.Chance(0.6))
        {
            List<ColumnDef> targets = table.Columns.Where(c => !c.PrimaryKey).ToList();
            ExprScope scope = DmlScope(table);
            doUpdate = random.Shuffled(targets).Take(random.Between(1, Math.Min(2, targets.Count))).Select(c =>
                new Assignment(c.Name, random.Chance(0.5) ? new ColumnExpr("excluded", c.Name, c.Type) : AssignedValue(c, scope))).ToList();
            if (doUpdate.Count == 0)
                doUpdate = null;
        }

        return new InsertStmt(table.Name, table.Columns.Select(c => c.Name).ToList(), rows, new OnConflictClause([table.PrimaryKey.Name], doUpdate));
    }

    /// <summary>
    /// INSERT … SELECT that copies rows of a table into the same table with new keys: the INT64 key
    /// moves by a large offset, so it does not meet the keys already there.
    /// </summary>
    public InsertSelectStmt? GenerateInsertSelect()
    {
        // The form needs + for the new keys and ORDER BY … LIMIT to bound the copied rows; a profile
        // without them gets no INSERT … SELECT rather than one that breaks its own rules.
        if (!profile.Allows(StatementFeature.InsertSelect) || !profile.Allows(BinaryOp.Add) || !profile.Allows(StatementFeature.OrderByLimit))
            return null;
        List<TableDef> tables = State.Tables.Where(t => t.PrimaryKey.Type == FuzzType.Int64).ToList();
        if (tables.Count == 0)
            return null;
        TableDef table = random.Pick(tables);

        ExprScope scope = new(table.Columns.Select(c => new ScopeColumn("s", c.Name, c.Type)).ToList(), State.Tables);
        long offset = 1_000_000L * random.Between(1, 1000) + State.NextKey[table.Name];
        State.NextKey[table.Name] += 1;

        List<SelectItem> items = new();
        foreach (ColumnDef column in table.Columns)
        {
            // A copy of a NOT NULL column is never NULL. A generated value for one needs a COALESCE; a
            // profile without COALESCE copies the column instead.
            bool copy = random.Chance(0.7) || (column.NotNull && !profile.Allows(ExprFeature.Coalesce));
            Expr value = column.PrimaryKey
                ? new BinaryExpr(BinaryOp.Add, new ColumnExpr("s", column.Name, column.Type), new LiteralExpr(FuzzValue.Of(offset)), FuzzType.Int64)
                : copy ? new ColumnExpr("s", column.Name, column.Type) : expressions.Generate(column.Type, scope, 2);
            if (column.NotNull && !column.PrimaryKey && !copy)
                value = new FunctionExpr("coalesce", [value, new LiteralExpr(values.Next(column.Type, nullable: false, boundaries: false))], column.Type);
            items.Add(new SelectItem(value));
        }

        // At most a few rows per copy: repeated copies of a whole table double it each time, and a later
        // join of three such tables reads millions of rows and passes the statement time limit.
        Expr where = expressions.Predicate(scope.ForWhere());
        return new InsertSelectStmt(table.Name, table.Columns.Select(c => c.Name).ToList(), new SelectStmt(items, new TableRef(table.Name, "s"), where,
            OrderBy: [new OrderItem(new ColumnExpr("s", table.PrimaryKey.Name, table.PrimaryKey.Type), false)], Limit: random.Between(1, 5)));
    }

    /// <summary>The scope of a DML statement on one table: unqualified columns.</summary>
    public ExprScope DmlScope(TableDef table) =>
        new(table.Columns.Select(c => new ScopeColumn(null, c.Name, c.Type)).ToList(), State.Tables);

    /// <summary>A WHERE for UPDATE or DELETE. About one in twenty is <c>WHERE TRUE</c>, the full-table form.</summary>
    public Expr DmlWhere(ExprScope scope) =>
        random.Chance(0.05) ? new LiteralExpr(FuzzValue.Of(true)) : expressions.Predicate(scope.ForWhere());

    /// <summary>
    /// A new value for <paramref name="column"/> of exactly its type. A generated value for a NOT NULL
    /// column gets a COALESCE with a literal; a profile without COALESCE gets a non-NULL literal instead.
    /// </summary>
    public Expr AssignedValue(ColumnDef column, ExprScope scope)
    {
        bool literal = random.Chance(0.5) || (column.NotNull && !profile.Allows(ExprFeature.Coalesce));
        Expr value = literal ? expressions.Literal(column.Type, allowNull: !column.NotNull) : expressions.Generate(column.Type, scope, 2);
        if (column.NotNull && value is not LiteralExpr)
            value = new FunctionExpr("coalesce", [value, new LiteralExpr(values.Next(column.Type, nullable: false, boundaries: false))], column.Type);
        return value;
    }
}
