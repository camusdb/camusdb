/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.SqlFuzz.Tests.Generator;

/// <summary>Expression forms that a profile can allow or remove, in addition to the operators and functions.</summary>
internal enum ExprFeature
{
    Case,
    Coalesce,
    NullIf,
    InList,
    Between,
    Like,
    Cast,
    JsonField,
    Exists,
    InSubquery,
    CorrelatedSubquery,
    BoundaryLiterals,
}

/// <summary>Statement forms that a profile can allow or remove.</summary>
internal enum StatementFeature
{
    Joins,
    GroupBy,
    Having,
    Distinct,
    OrderByLimit,
    SetOperations,
    WindowFunctions,
    IndexHints,
    MultiRowInsert,
    InsertSelect,
    Upsert,
    Update,
    Delete,
    Analyze,
    UniqueIndexes,
    CoveringIndexes,
    LargeValues,
}

/// <summary>One scalar function signature: a neutral name, the argument types and the result type.</summary>
internal sealed record FunctionSignature(string Name, IReadOnlyList<FuzzType> Args, FuzzType Result);

/// <summary>
/// The scalar functions the generator can call. Names are neutral; the CamusDB printer writes them as
/// they are, and another printer maps a name its dialect spells differently.
/// </summary>
internal static class FunctionCatalog
{
    public static readonly IReadOnlyList<FunctionSignature> All =
    [
        new("abs", [FuzzType.Int64], FuzzType.Int64),
        new("abs", [FuzzType.Float64], FuzzType.Float64),
        new("abs", [FuzzType.Numeric], FuzzType.Numeric),
        new("floor", [FuzzType.Float64], FuzzType.Float64),
        new("ceil", [FuzzType.Float64], FuzzType.Float64),
        new("round", [FuzzType.Float64], FuzzType.Float64),
        new("length", [FuzzType.String], FuzzType.Int64),
        new("lower", [FuzzType.String], FuzzType.String),
        new("upper", [FuzzType.String], FuzzType.String),
        new("trim", [FuzzType.String], FuzzType.String),
        new("substring", [FuzzType.String, FuzzType.Int64, FuzzType.Int64], FuzzType.String),
        new("replace", [FuzzType.String, FuzzType.String, FuzzType.String], FuzzType.String),
        new("strpos", [FuzzType.String, FuzzType.String], FuzzType.Int64),
        new("starts_with", [FuzzType.String, FuzzType.String], FuzzType.Bool),
        new("ends_with", [FuzzType.String, FuzzType.String], FuzzType.Bool),
        new("concat", [FuzzType.String, FuzzType.String], FuzzType.String),
    ];

    public static IEnumerable<string> Names => All.Select(f => f.Name).Distinct();
}

/// <summary>
/// A generator setting that limits the types, operators, functions, expression forms and statement
/// forms the generator may select. The generator asks the profile before every choice, so an item a
/// profile removes never appears in its output.
///
/// <para>This project defines <see cref="CamusDB"/>, the full set. A consumer that compares CamusDB
/// with a reference defines its own subset: an in-memory model supports fewer operators, and a
/// differential test against another database keeps only what both have. Build a subset with the
/// <c>Without…</c> methods, from <see cref="CamusDB"/> or from another subset.</para>
/// </summary>
internal sealed class GeneratorProfile
{
    private GeneratorProfile(
        string name,
        IReadOnlySet<FuzzType> types,
        IReadOnlySet<UnaryOp> unaryOps,
        IReadOnlySet<BinaryOp> binaryOps,
        IReadOnlySet<string> functions,
        IReadOnlySet<ExprFeature> exprFeatures,
        IReadOnlySet<StatementFeature> statementFeatures,
        IReadOnlySet<JoinKind> joinKinds,
        IReadOnlySet<SetOpKind> setOps,
        int maxExpressionDepth)
    {
        Name = name;
        Types = types;
        UnaryOps = unaryOps;
        BinaryOps = binaryOps;
        Functions = functions;
        ExprFeatures = exprFeatures;
        StatementFeatures = statementFeatures;
        JoinKinds = joinKinds;
        SetOps = setOps;
        MaxExpressionDepth = maxExpressionDepth;
    }

    public string Name { get; }

    /// <summary>The column types. INT64 is always kept: every table has an INT64 or STRING primary key, and the counts of the oracles are INT64.</summary>
    public IReadOnlySet<FuzzType> Types { get; }

    public IReadOnlySet<UnaryOp> UnaryOps { get; }

    public IReadOnlySet<BinaryOp> BinaryOps { get; }

    public IReadOnlySet<string> Functions { get; }

    public IReadOnlySet<ExprFeature> ExprFeatures { get; }

    public IReadOnlySet<StatementFeature> StatementFeatures { get; }

    public IReadOnlySet<JoinKind> JoinKinds { get; }

    public IReadOnlySet<SetOpKind> SetOps { get; }

    /// <summary>
    /// The deepest expression tree the generator makes. The cap keeps every tree far below the
    /// statement nesting limit of the parser, so a refusal for depth is never a generated outcome.
    /// </summary>
    public int MaxExpressionDepth { get; }

    /// <summary>The full set: every type, operator, function and statement form the generator knows for CamusDB.</summary>
    public static GeneratorProfile CamusDB { get; } = new(
        "CamusDB",
        Enum.GetValues<FuzzType>().ToHashSet(),
        Enum.GetValues<UnaryOp>().ToHashSet(),
        Enum.GetValues<BinaryOp>().ToHashSet(),
        FunctionCatalog.Names.ToHashSet(StringComparer.Ordinal),
        Enum.GetValues<ExprFeature>().ToHashSet(),
        Enum.GetValues<StatementFeature>().ToHashSet(),
        Enum.GetValues<JoinKind>().ToHashSet(),
        Enum.GetValues<SetOpKind>().ToHashSet(),
        maxExpressionDepth: 4);

    public bool Allows(FuzzType type) => Types.Contains(type);
    public bool Allows(UnaryOp op) => UnaryOps.Contains(op);
    public bool Allows(BinaryOp op) => BinaryOps.Contains(op);
    public bool Allows(ExprFeature feature) => ExprFeatures.Contains(feature);
    public bool Allows(StatementFeature feature) => StatementFeatures.Contains(feature);
    public bool Allows(JoinKind kind) => JoinKinds.Contains(kind);
    public bool AllowsFunction(string name) => Functions.Contains(name);

    public GeneratorProfile Named(string name) => With(name: name);

    public GeneratorProfile WithoutTypes(params FuzzType[] types)
    {
        HashSet<FuzzType> kept = Types.Except(types).ToHashSet();
        kept.Add(FuzzType.Int64);
        kept.Add(FuzzType.Bool);
        return With(types: kept);
    }

    public GeneratorProfile WithoutOperators(params BinaryOp[] ops)
    {
        HashSet<BinaryOp> kept = BinaryOps.Except(ops).ToHashSet();
        // A predicate needs at least one comparison and AND.
        kept.Add(BinaryOp.Eq);
        kept.Add(BinaryOp.And);
        return With(binaryOps: kept);
    }

    public GeneratorProfile WithoutOperators(params UnaryOp[] ops) => With(unaryOps: UnaryOps.Except(ops).ToHashSet());

    public GeneratorProfile WithoutFunctions(params string[] names) => With(functions: Functions.Except(names, StringComparer.Ordinal).ToHashSet(StringComparer.Ordinal));

    public GeneratorProfile WithoutFeatures(params ExprFeature[] features) => With(exprFeatures: ExprFeatures.Except(features).ToHashSet());

    public GeneratorProfile WithoutFeatures(params StatementFeature[] features) => With(statementFeatures: StatementFeatures.Except(features).ToHashSet());

    public GeneratorProfile WithoutJoins(params JoinKind[] kinds) => With(joinKinds: JoinKinds.Except(kinds).ToHashSet());

    public GeneratorProfile WithoutSetOperations(params SetOpKind[] kinds) => With(setOps: SetOps.Except(kinds).ToHashSet());

    public GeneratorProfile WithMaxExpressionDepth(int depth) => With(maxExpressionDepth: Math.Max(1, depth));

    private GeneratorProfile With(
        string? name = null,
        IReadOnlySet<FuzzType>? types = null,
        IReadOnlySet<UnaryOp>? unaryOps = null,
        IReadOnlySet<BinaryOp>? binaryOps = null,
        IReadOnlySet<string>? functions = null,
        IReadOnlySet<ExprFeature>? exprFeatures = null,
        IReadOnlySet<StatementFeature>? statementFeatures = null,
        IReadOnlySet<JoinKind>? joinKinds = null,
        IReadOnlySet<SetOpKind>? setOps = null,
        int? maxExpressionDepth = null) =>
        new(name ?? Name,
            types ?? Types,
            unaryOps ?? UnaryOps,
            binaryOps ?? BinaryOps,
            functions ?? Functions,
            exprFeatures ?? ExprFeatures,
            statementFeatures ?? StatementFeatures,
            joinKinds ?? JoinKinds,
            setOps ?? SetOps,
            maxExpressionDepth ?? MaxExpressionDepth);
}
