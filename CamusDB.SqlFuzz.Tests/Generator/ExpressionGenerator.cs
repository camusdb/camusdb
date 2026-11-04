/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.SqlFuzz.Tests.Generator;

/// <summary>A column that an expression can read: the source alias (null in DML), the column name and its type.</summary>
internal sealed record ScopeColumn(string? Source, string Column, FuzzType Type);

/// <summary>
/// The columns an expression may read, and the tables a subquery may read. <see cref="Outer"/> holds
/// the columns of the enclosing query, for a correlated subquery.
///
/// <para><see cref="AllowSubqueries"/> is true for the positions where CamusDB evaluates a subquery:
/// a WHERE, an ON clause, a select list and an oracle predicate, at any depth of the expression (a
/// CASE branch, a comparison operand, an IS NULL test). <see cref="AllowCorrelation"/> is true only
/// for the WHERE of a SELECT: that is the one position where CamusDB evaluates a correlated EXISTS
/// per row. In every other position a correlated EXISTS is refused with CADB0533, and a correlated
/// IN is refused everywhere, so the generator writes only uncorrelated subqueries there.</para>
/// </summary>
internal sealed record ExprScope(IReadOnlyList<ScopeColumn> Columns, IReadOnlyList<TableDef> Tables, ExprScope? Outer = null, int SubqueryDepth = 0, bool AllowSubqueries = false, bool AllowCorrelation = false)
{
    /// <summary>This scope for the WHERE of a SELECT: subqueries allowed, a correlated EXISTS among them.</summary>
    public ExprScope ForSelectWhere() => this with { AllowSubqueries = true, AllowCorrelation = true };

    /// <summary>This scope for an ON clause, a select list, a DML WHERE or an oracle predicate: uncorrelated subqueries only.</summary>
    public ExprScope ForUncorrelatedSubqueries() => this with { AllowSubqueries = true, AllowCorrelation = false };

    public IEnumerable<ScopeColumn> OfType(FuzzType type) => Columns.Where(c => c.Type == type);

    public IEnumerable<FuzzType> TypesPresent => Columns.Select(c => c.Type).Distinct();
}

/// <summary>
/// Makes typed expression trees. Every choice is legal for the operand types, so most statements
/// reach execution: a comparison takes two comparable types, arithmetic takes numbers, LIKE takes
/// strings, and a CASE, COALESCE or NULLIF takes one type in every branch.
///
/// <para><b>Depth.</b> The generator stops at <see cref="GeneratorProfile.MaxExpressionDepth"/>, then
/// it writes a column or a literal. The tree printer and the shrinker walk the tree by recursion,
/// which is safe only because of this cap.</para>
/// </summary>
internal sealed class ExpressionGenerator
{
    private static readonly BinaryOp[] Comparisons = [BinaryOp.Eq, BinaryOp.Ne, BinaryOp.Lt, BinaryOp.Le, BinaryOp.Gt, BinaryOp.Ge];

    private readonly FuzzRandom random;
    private readonly GeneratorProfile profile;
    private readonly ValueGenerator values;
    private int subqueryAliasCounter;

    public ExpressionGenerator(FuzzRandom random, GeneratorProfile profile)
    {
        this.random = random;
        this.profile = profile;
        values = new ValueGenerator(random, profile);
    }

    public ValueGenerator Values => values;

    /// <summary>A BOOL expression: a WHERE, ON or HAVING predicate.</summary>
    public Expr Predicate(ExprScope scope) => Generate(FuzzType.Bool, scope, 0);

    /// <summary>A literal of <paramref name="type"/>, sometimes NULL.</summary>
    public Expr Literal(FuzzType type, bool allowNull = true) => new LiteralExpr(values.Next(type, allowNull, boundaries: true));

    /// <summary>An expression of exactly <paramref name="type"/>.</summary>
    public Expr Generate(FuzzType type, ExprScope scope, int depth)
    {
        if (depth >= profile.MaxExpressionDepth || random.Chance(0.25 + 0.15 * depth))
            return Leaf(type, scope);

        return type switch
        {
            FuzzType.Bool => BoolExpr(scope, depth),
            FuzzType.Int64 or FuzzType.Float64 or FuzzType.Numeric => NumberExpr(type, scope, depth),
            FuzzType.String => StringExpr(scope, depth),
            FuzzType.Date => Conditional(FuzzType.Date, scope, depth) ?? Leaf(type, scope),
            FuzzType.Jsonb => JsonExpr(scope, depth),
            _ => Leaf(type, scope),
        };
    }

    /// <summary>A column of the type when the scope has one (most of the time), else a literal.</summary>
    public Expr Leaf(FuzzType type, ExprScope scope)
    {
        List<ScopeColumn> columns = scope.OfType(type).ToList();
        if (columns.Count > 0 && random.Chance(0.7))
        {
            ScopeColumn column = random.Pick(columns);
            return new ColumnExpr(column.Source, column.Column, column.Type);
        }

        if (scope.Outer is not null && random.Chance(0.15) && profile.Allows(ExprFeature.CorrelatedSubquery))
        {
            List<ScopeColumn> outer = scope.Outer.OfType(type).ToList();
            if (outer.Count > 0)
            {
                ScopeColumn column = random.Pick(outer);
                return new ColumnExpr(column.Source, column.Column, column.Type);
            }
        }

        if (type == FuzzType.Bool && random.Chance(0.5))
            return new LiteralExpr(FuzzValue.Of(random.NextBool()));

        return Literal(type);
    }

    /// <summary>A type for one operand of a comparison: one the scope has, most of the time.</summary>
    private FuzzType PickComparableType(ExprScope scope)
    {
        List<FuzzType> present = scope.TypesPresent.Where(t => FuzzTypes.IsOrderable(t) && profile.Allows(t)).ToList();
        if (present.Count > 0 && random.Chance(0.85))
            return random.Pick(present);
        return random.Pick(profile.Types.Where(FuzzTypes.IsOrderable).ToList());
    }

    /// <summary>A type comparable with <paramref name="type"/>: the same one, or another number type.</summary>
    private FuzzType Partner(FuzzType type)
    {
        if (FuzzTypes.IsNumber(type) && random.Chance(0.3))
        {
            List<FuzzType> numbers = profile.Types.Where(FuzzTypes.IsNumber).ToList();
            return random.Pick(numbers);
        }
        return type;
    }

    private Expr BoolExpr(ExprScope scope, int depth)
    {
        for (int attempt = 0; attempt < 8; attempt++)
        {
            int roll = random.Next(100);
            Expr? result = roll switch
            {
                < 34 => Comparison(scope, depth),
                < 46 => Logical(scope, depth),
                < 52 => profile.Allows(UnaryOp.Not) ? new UnaryExpr(UnaryOp.Not, Generate(FuzzType.Bool, scope, depth + 1)) : null,
                < 59 => NullTest(scope, depth),
                < 62 => TruthTest(scope, depth),
                < 68 => InList(scope, depth),
                < 73 => Between(scope, depth),
                < 78 => Like(scope, depth),
                < 81 => StringPredicate(scope, depth),
                < 85 => Exists(scope),
                < 88 => InSubquery(scope, depth),
                < 91 => JsonContains(scope, depth),
                < 94 => Conditional(FuzzType.Bool, scope, depth),
                _ => Leaf(FuzzType.Bool, scope),
            };
            if (result is not null)
                return result;
        }
        return Comparison(scope, depth) ?? Leaf(FuzzType.Bool, scope);
    }

    private Expr? Comparison(ExprScope scope, int depth)
    {
        List<BinaryOp> ops = Comparisons.Where(profile.Allows).ToList();
        if (ops.Count == 0)
            return null;

        FuzzType left = PickComparableType(scope);
        FuzzType right = Partner(left);
        return new BinaryExpr(random.Pick(ops), Generate(left, scope, depth + 1), Generate(right, scope, depth + 1), FuzzType.Bool);
    }

    private Expr? Logical(ExprScope scope, int depth)
    {
        BinaryOp op = random.NextBool() ? BinaryOp.And : BinaryOp.Or;
        if (!profile.Allows(op))
            return null;
        return new BinaryExpr(op, Generate(FuzzType.Bool, scope, depth + 1), Generate(FuzzType.Bool, scope, depth + 1), FuzzType.Bool);
    }

    private Expr? NullTest(ExprScope scope, int depth)
    {
        UnaryOp op = random.NextBool() ? UnaryOp.IsNull : UnaryOp.IsNotNull;
        if (!profile.Allows(op))
            return null;
        FuzzType type = random.Chance(0.8) && scope.Columns.Count > 0 ? random.Pick(scope.Columns).Type : random.Pick(profile.Types.ToList());
        return new UnaryExpr(op, Generate(type, scope, depth + 1));
    }

    private Expr? TruthTest(ExprScope scope, int depth)
    {
        UnaryOp op = random.NextBool() ? UnaryOp.IsTrue : UnaryOp.IsFalse;
        if (!profile.Allows(op))
            return null;
        return new UnaryExpr(op, Generate(FuzzType.Bool, scope, depth + 1));
    }

    private Expr? InList(ExprScope scope, int depth)
    {
        if (!profile.Allows(ExprFeature.InList))
            return null;
        // CamusDB accepts a literal, a parameter or a name as an IN-list item (a simple_expr), so an
        // item is a column of the scope or a literal in the forms that rule accepts: no CAST, no NaN
        // or infinity.
        FuzzType type = PickComparableType(scope);
        int count = random.Between(1, 4);
        List<Expr> items = new(count);
        for (int i = 0; i < count; i++)
            items.Add(InListItem(Partner(type), scope));
        return new InListExpr(Generate(type, scope, depth + 1), items, random.Chance(0.3));
    }

    /// <summary>An IN-list item: a column of <paramref name="scope"/> with the item's type one time in four, otherwise a literal.</summary>
    private Expr InListItem(FuzzType type, ExprScope scope)
    {
        List<ScopeColumn> columns = scope.OfType(type).ToList();
        if (columns.Count > 0 && random.Chance(0.25))
        {
            ScopeColumn column = random.Pick(columns);
            return new ColumnExpr(column.Source, column.Column, column.Type);
        }

        return InListLiteral(type);
    }

    private Expr InListLiteral(FuzzType type)
    {
        FuzzValue value = values.Next(type, nullable: true, boundaries: true);
        if (value.Type == FuzzType.Float64 && !double.IsFinite(value.Float))
            value = FuzzValue.Of(1.5);
        return new LiteralExpr(value);
    }

    private Expr? Between(ExprScope scope, int depth)
    {
        if (!profile.Allows(ExprFeature.Between))
            return null;
        FuzzType type = PickComparableType(scope);
        return new BetweenExpr(Generate(type, scope, depth + 1), Generate(Partner(type), scope, depth + 2), Generate(Partner(type), scope, depth + 2), random.Chance(0.3));
    }

    private Expr? Like(ExprScope scope, int depth)
    {
        if (!profile.Allows(ExprFeature.Like) || !profile.Allows(FuzzType.String))
            return null;
        return new LikeExpr(Generate(FuzzType.String, scope, depth + 1), new LiteralExpr(values.LikePattern()), random.Chance(0.3));
    }

    private Expr? StringPredicate(ExprScope scope, int depth)
    {
        if (!profile.Allows(FuzzType.String))
            return null;
        List<FunctionSignature> candidates = FunctionCatalog.All.Where(f => f.Result == FuzzType.Bool && profile.AllowsFunction(f.Name)).ToList();
        if (candidates.Count == 0)
            return null;
        return Call(random.Pick(candidates), scope, depth);
    }

    private Expr? JsonContains(ExprScope scope, int depth)
    {
        if (!profile.Allows(FuzzType.Jsonb) || !profile.Allows(BinaryOp.JsonContains))
            return null;
        if (!scope.OfType(FuzzType.Jsonb).Any())
            return null;
        return new BinaryExpr(BinaryOp.JsonContains, Generate(FuzzType.Jsonb, scope, depth + 1), Literal(FuzzType.Jsonb, allowNull: false), FuzzType.Bool);
    }

    private Expr? Exists(ExprScope scope)
    {
        if (!profile.Allows(ExprFeature.Exists) || !scope.AllowSubqueries || scope.SubqueryDepth >= 1 || scope.Tables.Count == 0)
            return null;
        (SelectStmt sub, _) = Subquery(scope, items: s => [new SelectItem(new LiteralExpr(FuzzValue.Of(1L)))]);
        return new ExistsExpr(sub, random.Chance(0.3));
    }

    private Expr? InSubquery(ExprScope scope, int depth)
    {
        if (!profile.Allows(ExprFeature.InSubquery) || !scope.AllowSubqueries || scope.SubqueryDepth >= 1 || scope.Tables.Count == 0)
            return null;

        // CamusDB does not support a correlated IN subquery, so the IN subquery is uncorrelated.
        FuzzType type = PickComparableType(scope);
        (SelectStmt sub, bool ok) = Subquery(scope, correlated: false, items: s =>
        {
            List<ScopeColumn> columns = s.OfType(type).ToList();
            if (columns.Count == 0)
                return null;
            ScopeColumn column = random.Pick(columns);
            return [new SelectItem(new ColumnExpr(column.Source, column.Column, column.Type))];
        });
        if (!ok)
            return null;
        return new InSubqueryExpr(Generate(type, scope, depth + 1), sub, random.Chance(0.3));
    }

    /// <summary>
    /// A one-table subquery with a WHERE. Its alias is unique in the statement. It may read the outer
    /// columns when the position allows correlation (<see cref="ExprScope.AllowCorrelation"/>) and
    /// the profile allows it.
    /// </summary>
    private (SelectStmt Query, bool Ok) Subquery(ExprScope scope, Func<ExprScope, IReadOnlyList<SelectItem>?> items, bool correlated = true)
    {
        TableDef table = random.Pick(scope.Tables);
        string alias = "s" + subqueryAliasCounter++;
        ExprScope inner = new(
            table.Columns.Select(c => new ScopeColumn(alias, c.Name, c.Type)).ToList(),
            scope.Tables,
            correlated && scope.AllowCorrelation && profile.Allows(ExprFeature.CorrelatedSubquery) ? scope : null,
            scope.SubqueryDepth + 1);

        IReadOnlyList<SelectItem>? list = items(inner);
        if (list is null)
            return (null!, false);

        Expr? where = random.Chance(0.85) ? Generate(FuzzType.Bool, inner, 1) : null;
        return (new SelectStmt(list, new TableRef(table.Name, alias), where), true);
    }

    private Expr NumberExpr(FuzzType type, ExprScope scope, int depth)
    {
        int roll = random.Next(100);
        Expr? result = roll switch
        {
            < 40 => Arithmetic(type, scope, depth),
            < 50 => profile.Allows(UnaryOp.Negate) ? new UnaryExpr(UnaryOp.Negate, Generate(type, scope, depth + 1)) : null,
            < 65 => FunctionOf(type, scope, depth),
            < 75 => NumberCast(type, scope, depth),
            < 92 => Conditional(type, scope, depth),
            _ => null,
        };
        return result ?? Leaf(type, scope);
    }

    /// <summary>
    /// Arithmetic whose result is exactly <paramref name="type"/>: INT64 from two INT64 operands,
    /// NUMERIC from NUMERIC with NUMERIC or INT64, FLOAT64 from FLOAT64 with any number. A divisor is
    /// usually a non-zero literal, so most divisions do not fail.
    /// </summary>
    private Expr? Arithmetic(FuzzType type, ExprScope scope, int depth)
    {
        // Only operators the profile allows, and MOD only for INT64: a disallowed operator is never a
        // fallback for another one.
        List<BinaryOp> ops = new[] { BinaryOp.Add, BinaryOp.Sub, BinaryOp.Mul, BinaryOp.Div, BinaryOp.Mod }
            .Where(o => profile.Allows(o) && (o != BinaryOp.Mod || type == FuzzType.Int64)).ToList();
        if (ops.Count == 0)
            return null;
        BinaryOp op = random.Pick(ops);

        FuzzType leftType = type;
        FuzzType rightType = type switch
        {
            FuzzType.Float64 => random.Pick(profile.Types.Where(FuzzTypes.IsNumber).ToList()),
            FuzzType.Numeric => random.Chance(0.3) ? FuzzType.Int64 : FuzzType.Numeric,
            _ => FuzzType.Int64,
        };
        if (random.NextBool())
            (leftType, rightType) = (rightType, leftType);
        if (FuzzTypes.Widen(leftType, rightType) != type)
            (leftType, rightType) = (type, type);

        Expr left = Generate(leftType, scope, depth + 1);
        Expr right = (op is BinaryOp.Div or BinaryOp.Mod) && random.Chance(0.8)
            ? new LiteralExpr(rightType == FuzzType.Int64 ? values.NonZeroInt() : rightType == FuzzType.Float64 ? FuzzValue.Of(2.0) : FuzzValue.Numeric("2"))
            : Generate(rightType, scope, depth + 1);
        return new BinaryExpr(op, left, right, type);
    }

    private Expr? FunctionOf(FuzzType type, ExprScope scope, int depth)
    {
        List<FunctionSignature> candidates = FunctionCatalog.All.Where(f => f.Result == type && profile.AllowsFunction(f.Name) && f.Args.All(profile.Allows)).ToList();
        if (candidates.Count == 0)
            return null;
        return Call(random.Pick(candidates), scope, depth);
    }

    private Expr Call(FunctionSignature signature, ExprScope scope, int depth)
    {
        List<Expr> args = new(signature.Args.Count);
        for (int i = 0; i < signature.Args.Count; i++)
        {
            FuzzType argType = signature.Args[i];
            // The position arguments of substring are small literals, so the call does not fail on a negative length.
            if (signature.Name == "substring" && i > 0)
                args.Add(new LiteralExpr(FuzzValue.Of((long)random.Between(i == 1 ? 1 : 0, 3))));
            else
                args.Add(Generate(argType, scope, depth + 1));
        }
        return new FunctionExpr(signature.Name, args, signature.Result);
    }

    private Expr? NumberCast(FuzzType type, ExprScope scope, int depth)
    {
        if (!profile.Allows(ExprFeature.Cast))
            return null;
        FuzzType source = type switch
        {
            FuzzType.Float64 => random.NextBool() ? FuzzType.Int64 : FuzzType.Numeric,
            FuzzType.Numeric => FuzzType.Int64,
            _ => random.Chance(0.7) ? FuzzType.Bool : FuzzType.Float64,
        };
        if (!profile.Allows(source))
            return null;
        return new CastExpr(Generate(source, scope, depth + 1), type);
    }

    private Expr StringExpr(ExprScope scope, int depth)
    {
        int roll = random.Next(100);
        Expr? result = roll switch
        {
            < 20 => profile.Allows(BinaryOp.Concat) ? new BinaryExpr(BinaryOp.Concat, Generate(FuzzType.String, scope, depth + 1), Generate(FuzzType.String, scope, depth + 1), FuzzType.String) : null,
            < 50 => FunctionOf(FuzzType.String, scope, depth),
            < 65 => StringCast(scope, depth),
            < 75 => JsonText(scope, depth),
            _ => Conditional(FuzzType.String, scope, depth),
        };
        return result ?? Leaf(FuzzType.String, scope);
    }

    private Expr? StringCast(ExprScope scope, int depth)
    {
        if (!profile.Allows(ExprFeature.Cast))
            return null;
        List<FuzzType> sources = profile.Types.Where(t => t is FuzzType.Int64 or FuzzType.Bool or FuzzType.Date or FuzzType.Numeric).ToList();
        if (sources.Count == 0)
            return null;
        return new CastExpr(Generate(random.Pick(sources), scope, depth + 1), FuzzType.String);
    }

    private Expr? JsonText(ExprScope scope, int depth)
    {
        if (!profile.Allows(ExprFeature.JsonField) || !scope.OfType(FuzzType.Jsonb).Any())
            return null;
        return new JsonFieldExpr(Generate(FuzzType.Jsonb, scope, depth + 1), random.Pick(ValueGenerator.JsonKeys), AsText: true);
    }

    private Expr JsonExpr(ExprScope scope, int depth)
    {
        if (profile.Allows(ExprFeature.JsonField) && random.Chance(0.4))
            return new JsonFieldExpr(Generate(FuzzType.Jsonb, scope, depth + 1), random.Pick(ValueGenerator.JsonKeys), AsText: false);
        return Leaf(FuzzType.Jsonb, scope);
    }

    /// <summary>CASE, COALESCE or NULLIF with every branch of exactly <paramref name="type"/>.</summary>
    private Expr? Conditional(FuzzType type, ExprScope scope, int depth)
    {
        int roll = random.Next(3);
        if (roll == 0 && profile.Allows(ExprFeature.Case))
        {
            int count = random.Between(1, 2);
            List<CaseBranch> branches = new(count);
            for (int i = 0; i < count; i++)
                branches.Add(new CaseBranch(Generate(FuzzType.Bool, scope, depth + 1), Generate(type, scope, depth + 1)));
            Expr? @else = random.Chance(0.7) ? Generate(type, scope, depth + 1) : null;
            return new CaseExpr(branches, @else, type);
        }

        if (roll == 1 && profile.Allows(ExprFeature.Coalesce))
            return new FunctionExpr("coalesce", [Generate(type, scope, depth + 1), Generate(type, scope, depth + 1)], type);

        if (roll == 2 && profile.Allows(ExprFeature.NullIf) && FuzzTypes.IsOrderable(type))
            return new FunctionExpr("nullif", [Generate(type, scope, depth + 1), Generate(type, scope, depth + 1)], type);

        return null;
    }
}
