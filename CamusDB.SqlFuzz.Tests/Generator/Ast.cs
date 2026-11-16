/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.SqlFuzz.Tests.Generator;

// The statement tree of the generator.
//
// The tree is dialect-neutral: it holds no SQL text, no type spelling, no hint syntax and no literal
// form of any one database. A printer turns it into SQL (Printing/CamusSqlPrinter.cs). A second
// printer for another dialect, such as PostgreSQL for a differential oracle, needs no change here.
//
// Every node is an immutable record. A shrinker removes a part by building a new tree with the part
// left out, and prints the result again.

/// <summary>The value types the generator knows. A printer maps each one to the type name of its dialect.</summary>
internal enum FuzzType
{
    Int64,
    Float64,
    Numeric,
    String,
    Bool,
    Date,

    /// <summary>A UTC point in time with microseconds. A column type, and the result of <c>date ± interval</c>.</summary>
    DateTime,

    Jsonb,

    /// <summary>
    /// A span of months, days and microseconds. An expression type only: no CamusDB table stores an
    /// interval, so the generator never makes a column, an index key or a written value of this type
    /// (<see cref="FuzzTypes.IsExpressionOnly"/>).
    /// </summary>
    Interval,
}

internal static class FuzzTypes
{
    /// <summary>INT64, FLOAT64 and NUMERIC compare with one another and mix in arithmetic.</summary>
    public static bool IsNumber(FuzzType type) => type is FuzzType.Int64 or FuzzType.Float64 or FuzzType.Numeric;

    /// <summary>DATE and DATETIME compare with each other (a date is midnight UTC) and take an interval.</summary>
    public static bool IsPointInTime(FuzzType type) => type is FuzzType.Date or FuzzType.DateTime;

    /// <summary>A type that only an expression has: never a column, an index key or a value that a write stores.</summary>
    public static bool IsExpressionOnly(FuzzType type) => type == FuzzType.Interval;

    /// <summary>Two types whose values compare with <c>=</c> and <c>&lt;</c> without an error.</summary>
    public static bool AreComparable(FuzzType a, FuzzType b) =>
        a == b ? a != FuzzType.Jsonb : (IsNumber(a) && IsNumber(b)) || (IsPointInTime(a) && IsPointInTime(b));

    /// <summary>A type with an order (ORDER BY, MIN, MAX, BETWEEN), and for a column type, a type an ordered index can hold. JSONB has GIN indexes only.</summary>
    public static bool IsOrderable(FuzzType type) => type != FuzzType.Jsonb;

    /// <summary>The result type of arithmetic on two number types: FLOAT64 wins, then NUMERIC, then INT64.</summary>
    public static FuzzType Widen(FuzzType a, FuzzType b) =>
        a == FuzzType.Float64 || b == FuzzType.Float64 ? FuzzType.Float64
        : a == FuzzType.Numeric || b == FuzzType.Numeric ? FuzzType.Numeric
        : FuzzType.Int64;
}

/// <summary>
/// One literal value. <see cref="Text"/> holds the canonical text of a NUMERIC, DATE, DATETIME (ISO-8601
/// with a <c>Z</c>), STRING, JSONB or INTERVAL (the text of an <c>INTERVAL '…'</c> literal) value; <see cref="Int"/>, <see cref="Float"/> and <see cref="Bool"/> hold the other types. A NULL
/// keeps its type, so a printer can write a typed NULL where the type matters.
/// </summary>
internal sealed record FuzzValue(FuzzType Type, bool IsNull, long Int = 0, double Float = 0, string? Text = null, bool Bool = false)
{
    public static FuzzValue Null(FuzzType type) => new(type, true);
    public static FuzzValue Of(long value) => new(FuzzType.Int64, false, Int: value);
    public static FuzzValue Of(double value) => new(FuzzType.Float64, false, Float: value);
    public static FuzzValue Of(bool value) => new(FuzzType.Bool, false, Bool: value);
    public static FuzzValue String(string value) => new(FuzzType.String, false, Text: value);
    public static FuzzValue Numeric(string value) => new(FuzzType.Numeric, false, Text: value);
    public static FuzzValue Date(string value) => new(FuzzType.Date, false, Text: value);
    public static FuzzValue DateTime(string value) => new(FuzzType.DateTime, false, Text: value);
    public static FuzzValue Json(string value) => new(FuzzType.Jsonb, false, Text: value);
    public static FuzzValue Interval(string value) => new(FuzzType.Interval, false, Text: value);
}

// ---------------------------------------------------------------------------------------------
// Expressions
// ---------------------------------------------------------------------------------------------

/// <summary>An expression node. <see cref="Type"/> is the static type the generator gave it.</summary>
internal abstract record Expr
{
    public abstract FuzzType Type { get; }
}

/// <summary>A column of a source in FROM. <see cref="Source"/> is the alias, or null for an unqualified column (DML).</summary>
internal sealed record ColumnExpr(string? Source, string Column, FuzzType ColumnType) : Expr
{
    public override FuzzType Type => ColumnType;
}

internal sealed record LiteralExpr(FuzzValue Value) : Expr
{
    public override FuzzType Type => Value.Type;
}

internal enum UnaryOp
{
    Not,
    Negate,
    IsNull,
    IsNotNull,
    IsTrue,
    IsFalse,
}

internal sealed record UnaryExpr(UnaryOp Op, Expr Operand) : Expr
{
    public override FuzzType Type => Op == UnaryOp.Negate ? Operand.Type : FuzzType.Bool;
}

internal enum BinaryOp
{
    And,
    Or,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Concat,
    JsonContains,
}

internal sealed record BinaryExpr(BinaryOp Op, Expr Left, Expr Right, FuzzType ResultType) : Expr
{
    public override FuzzType Type => ResultType;
}

internal sealed record CaseBranch(Expr When, Expr Then);

/// <summary>A searched CASE. <see cref="Else"/> null means ELSE NULL.</summary>
internal sealed record CaseExpr(IReadOnlyList<CaseBranch> Branches, Expr? Else, FuzzType ResultType) : Expr
{
    public override FuzzType Type => ResultType;
}

/// <summary>
/// A scalar function call. <see cref="Name"/> is a neutral name from <see cref="FunctionCatalog"/>;
/// the printer spells it for its dialect.
/// </summary>
internal sealed record FunctionExpr(string Name, IReadOnlyList<Expr> Args, FuzzType ResultType) : Expr
{
    public override FuzzType Type => ResultType;
}

internal sealed record InListExpr(Expr Value, IReadOnlyList<Expr> Items, bool Negated) : Expr
{
    public override FuzzType Type => FuzzType.Bool;
}

internal sealed record BetweenExpr(Expr Value, Expr Low, Expr High, bool Negated) : Expr
{
    public override FuzzType Type => FuzzType.Bool;
}

internal sealed record LikeExpr(Expr Value, Expr Pattern, bool Negated) : Expr
{
    public override FuzzType Type => FuzzType.Bool;
}

internal sealed record CastExpr(Expr Operand, FuzzType Target) : Expr
{
    public override FuzzType Type => Target;
}

/// <summary>A JSONB field read: <c>doc -&gt; key</c> (JSONB) or <c>doc -&gt;&gt; key</c> (STRING).</summary>
internal sealed record JsonFieldExpr(Expr Document, string Key, bool AsText) : Expr
{
    public override FuzzType Type => AsText ? FuzzType.String : FuzzType.Jsonb;
}

internal sealed record ExistsExpr(SelectStmt Subquery, bool Negated) : Expr
{
    public override FuzzType Type => FuzzType.Bool;
}

internal sealed record InSubqueryExpr(Expr Value, SelectStmt Subquery, bool Negated) : Expr
{
    public override FuzzType Type => FuzzType.Bool;
}

internal enum AggregateFunc
{
    CountStar,
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

/// <summary>An aggregate call. <see cref="Argument"/> is null for COUNT(*).</summary>
internal sealed record AggregateExpr(AggregateFunc Func, Expr? Argument, FuzzType ResultType) : Expr
{
    public override FuzzType Type => ResultType;
}

internal enum WindowFunc
{
    RowNumber,
    Rank,
    DenseRank,
    Sum,
    Count,
}

internal sealed record WindowExpr(WindowFunc Func, Expr? Argument, IReadOnlyList<Expr> PartitionBy, IReadOnlyList<OrderItem> OrderBy, FuzzType ResultType) : Expr
{
    public override FuzzType Type => ResultType;
}

// ---------------------------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------------------------

internal sealed record ColumnDef(string Name, FuzzType Type, bool NotNull, bool PrimaryKey);

/// <summary>A secondary index. <see cref="Include"/> lists the covered (non-key) columns.</summary>
internal sealed record IndexDef(string Name, IReadOnlyList<string> Columns, bool Unique, IReadOnlyList<string> Include);

internal sealed record TableDef(string Name, IReadOnlyList<ColumnDef> Columns)
{
    public ColumnDef PrimaryKey => Columns.First(c => c.PrimaryKey);

    public ColumnDef Column(string name) => Columns.First(c => c.Name == name);
}

// ---------------------------------------------------------------------------------------------
// FROM and SELECT
// ---------------------------------------------------------------------------------------------

internal abstract record FromItem;

/// <summary>A table in FROM. <see cref="IndexHint"/> names the index the scan must use, or null. A dialect without index hints prints nothing for it.</summary>
internal sealed record TableRef(string Table, string Alias, string? IndexHint = null) : FromItem
{
    /// <summary>
    /// True when the reference reads the table through the CTE <c>cte_{Table}</c> (<c>SELECT * FROM Table</c>)
    /// that an enclosing <see cref="SelectStmt.CteTables"/> declares. The printer uses the CTE only when a
    /// WITH it wrote declares that table, and never with an index hint, so a statement that an oracle builds
    /// from parts of another one prints the plain table. The rows are the table's rows either way.
    /// </summary>
    public bool ViaCte { get; init; }
}

/// <summary>
/// A <c>VALUES</c> list in FROM: <c>(VALUES (…), (…)) AS alias (v0, v1, …)</c>. Every item is a literal of its
/// column's type, and the first row holds no NULL, so each column has that type under the <c>UNION</c> rule.
/// The generator puts one only first in a FROM list, so it is never the right side of a join.
/// </summary>
internal sealed record ValuesRef(string Alias, IReadOnlyList<(string Name, FuzzType Type)> Columns, IReadOnlyList<IReadOnlyList<Expr>> Rows) : FromItem;

internal enum JoinKind
{
    Inner,
    Left,
    Right,
    Cross,
}

/// <summary>A join. <see cref="On"/> is null for a CROSS join only.</summary>
internal sealed record JoinFrom(FromItem Left, JoinKind Kind, TableRef Right, Expr? On) : FromItem
{
    /// <summary>
    /// True when a SELECT with <see cref="SelectStmt.LateralJoins"/> prints this join as a <c>LATERAL</c> derived
    /// table: <c>JOIN t AS b ON p</c> as <c>CROSS JOIN LATERAL (SELECT * FROM t AS b WHERE p) AS b</c>, and a
    /// LEFT JOIN as <c>LEFT JOIN LATERAL (…) AS b ON TRUE</c>. The rows are the same either way. The printer
    /// keeps the join form for any other join, an index hint on the right table, and an ON with a subquery.
    /// </summary>
    public bool ViaLateral { get; init; }
}

internal sealed record SelectItem(Expr Expr, string? Alias = null);

internal sealed record OrderItem(Expr Expr, bool Descending);

internal enum SetOpKind
{
    Union,
    UnionAll,
    Intersect,
    Except,
}

internal sealed record SetOperation(SetOpKind Kind, SelectStmt Right);

// ---------------------------------------------------------------------------------------------
// Statements
// ---------------------------------------------------------------------------------------------

internal abstract record Statement;

internal sealed record CreateTableStmt(TableDef Table) : Statement;

internal sealed record CreateIndexStmt(string Table, IndexDef Index) : Statement;

internal sealed record Assignment(string Column, Expr Value);

/// <summary>ON CONFLICT (target) DO NOTHING when <see cref="DoUpdate"/> is null, else DO UPDATE SET.</summary>
internal sealed record OnConflictClause(IReadOnlyList<string> Target, IReadOnlyList<Assignment>? DoUpdate);

internal sealed record InsertStmt(string Table, IReadOnlyList<string> Columns, IReadOnlyList<IReadOnlyList<Expr>> Rows, OnConflictClause? OnConflict = null) : Statement;

internal sealed record InsertSelectStmt(string Table, IReadOnlyList<string> Columns, SelectStmt Source) : Statement;

/// <summary>
/// An UPDATE. <see cref="Where"/> is never null: a full-table update is <c>WHERE TRUE</c>, because one
/// without WHERE is refused by design.
///
/// <para>The joined form is <c>UPDATE t [AS alias] SET … FROM list WHERE …</c>. <see cref="Alias"/> names
/// the target, and <see cref="From"/> is the relation list, or null. In both forms with an alias or a
/// list, every column of the statement is qualified: a target column by <see cref="Qualifier"/>, a column
/// of the list by its own alias. A bare name that the target and a source both have is ambiguous, so the
/// generator never writes a bare name there.</para>
/// </summary>
internal sealed record UpdateStmt(string Table, IReadOnlyList<Assignment> Set, Expr Where, string? Alias = null, FromItem? From = null) : Statement
{
    /// <summary>The name that qualifies a target column: the alias, else the table name in a joined form, else null (bare columns).</summary>
    public string? Qualifier => Alias ?? (From is null ? null : Table);
}

/// <summary>
/// A DELETE. <see cref="Where"/> is never null, for the reason of <see cref="UpdateStmt"/>. The joined form
/// is <c>DELETE FROM t [AS alias] USING list WHERE …</c>, with the column rule of <see cref="UpdateStmt"/>.
/// </summary>
internal sealed record DeleteStmt(string Table, Expr Where, string? Alias = null, FromItem? Using = null) : Statement
{
    /// <summary>The name that qualifies a target column; see <see cref="UpdateStmt.Qualifier"/>.</summary>
    public string? Qualifier => Alias ?? (Using is null ? null : Table);
}

internal sealed record AnalyzeStmt(string Table) : Statement;

/// <summary>
/// A SELECT. An empty <see cref="Items"/> list is <c>SELECT *</c>. <see cref="SetOp"/> appends a set
/// operation whose right operand has the same column count and types.
/// </summary>
internal sealed record SelectStmt(
    IReadOnlyList<SelectItem> Items,
    FromItem From,
    Expr? Where = null,
    bool Distinct = false,
    IReadOnlyList<Expr>? GroupBy = null,
    Expr? Having = null,
    IReadOnlyList<OrderItem>? OrderBy = null,
    long? Limit = null,
    long? Offset = null,
    SetOperation? SetOp = null) : Statement
{
    /// <summary>
    /// The tables that this SELECT declares in a WITH, each as <c>cte_{table} AS (SELECT * FROM table)</c>,
    /// for the references marked <see cref="TableRef.ViaCte"/>; null for no WITH. A table that the SELECT
    /// reads two times gives a CTE that the statement reads two times, which CamusDB computes once at the
    /// start of a statement and inlines in parentheses.
    /// </summary>
    public IReadOnlyList<string>? CteTables { get; init; }

    /// <summary>
    /// True when this SELECT prints each join of its FROM that is marked <see cref="JoinFrom.ViaLateral"/> as a
    /// <c>LATERAL</c> derived table. A statement that an oracle builds from parts of another one prints the
    /// joins as written.
    /// </summary>
    public bool LateralJoins { get; init; }
}

/// <summary>An EXPLAIN of a SELECT, for the plan names in a finding.</summary>
internal sealed record ExplainStmt(SelectStmt Query) : Statement;

/// <summary>Transaction markers. A statement list uses them to group statements into one explicit transaction.</summary>
internal sealed record BeginStmt : Statement;

internal sealed record CommitStmt : Statement;

internal sealed record RollbackStmt : Statement;
