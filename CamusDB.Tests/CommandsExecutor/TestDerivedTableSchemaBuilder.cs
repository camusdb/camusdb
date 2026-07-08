
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Globalization;

using NUnit.Framework;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Unit tests for <see cref="DerivedTableSchemaBuilder"/> column-type inference. These assert the
/// <b>declared</b> <see cref="DerivedColumnSchema.Type"/> a subquery-in-FROM exposes to the outer
/// query — which is what binding, planning and EXPLAIN consume. This is distinct from the runtime
/// <c>ColumnValue.Type</c> of a result row (that flows through from the inner evaluator regardless
/// of the declared schema), so an end-to-end value assertion cannot exercise this path: these tests
/// drive <see cref="DerivedTableSchemaBuilder.Build"/> directly and fail if compound-aggregate type
/// inference regresses.
/// </summary>
[TestFixture]
public class TestDerivedTableSchemaBuilder
{
    // ── AST helpers (mirror the grammar's node shapes) ───────────────────────

    private static NodeAst Ident(string name) =>
        new(NodeType.Identifier, null, null, null, null, null, null, null, name);

    private static NodeAst IntLit(long v) =>
        new(NodeType.Integer, null, null, null, null, null, null, null, v.ToString(CultureInfo.InvariantCulture));

    private static NodeAst FloatLit(double v) =>
        new(NodeType.Float, null, null, null, null, null, null, null, v.ToString(CultureInfo.InvariantCulture));

    private static NodeAst FuncCall(string name, NodeAst? args = null) =>
        new(NodeType.ExprFuncCall, Ident(name), args, null, null, null, null, null, null);

    private static NodeAst ArgList(NodeAst left, NodeAst right) =>
        new(NodeType.ExprArgumentList, left, right, null, null, null, null, null, null);

    private static NodeAst Add(NodeAst l, NodeAst r) =>
        new(NodeType.ExprAdd, l, r, null, null, null, null, null, null);

    private static NodeAst Div(NodeAst l, NodeAst r) =>
        new(NodeType.ExprDiv, l, r, null, null, null, null, null, null);

    // ── Bound-source scaffolding ─────────────────────────────────────────────

    private static BoundTableSource MakeSalesSource()
    {
        List<TableColumnSchema> columns =
        [
            new TableColumnSchema("col0", "amount",  ColumnType.Integer64, false, null),
            new TableColumnSchema("col1", "qty",     ColumnType.Integer64, false, null),
            new TableColumnSchema("col2", "famount", ColumnType.Float64,   false, null),
            new TableColumnSchema("col3", "label",   ColumnType.String,    false, null),
        ];

        TableSchema schema = new() { Id = "sales", Name = "sales", Columns = columns, Version = 0 };
        TableDescriptor table = new(schema.Id!, schema.Name!, schema, store: null!);
        return new BoundTableSource(new TableSource("sales", "sales"), table, "sales");
    }

    /// <summary>
    /// Runs <see cref="DerivedTableSchemaBuilder.Build"/> over a single-projection inner query
    /// <c>SELECT &lt;projection&gt; AS v FROM sales</c> and returns the inferred column type.
    /// </summary>
    private static ColumnType InferDerivedColumnType(NodeAst projectionExpr)
    {
        BoundTableSource sales = MakeSalesSource();
        SelectQuery inner = new(
            Source: new TableSource("sales", "sales"),
            Projections: [new ProjectionItem(projectionExpr, "v")]);
        BoundSelectQuery bound = new(inner, [sales], new QueryRowNameResolver([sales]));

        IReadOnlyList<DerivedColumnSchema> columns = DerivedTableSchemaBuilder.Build(inner, bound);

        Assert.AreEqual(1, columns.Count);
        return columns[0].Type;
    }

    // ── Compound aggregate typing (the NA5 derived-table fix) ─────────────────

    [Test]
    public void Coalesce_OfIntegerSum_TypedInteger64()
    {
        // COALESCE(SUM(amount), 0) — without compound inference this falls back to ColumnType.Null.
        NodeAst expr = FuncCall("coalesce", ArgList(FuncCall("sum", Ident("amount")), IntLit(0)));
        Assert.AreEqual(ColumnType.Integer64, InferDerivedColumnType(expr));
    }

    [Test]
    public void Coalesce_OfFloatSum_TypedFloat64()
    {
        // COALESCE(SUM(famount), 0.0) — proves inference is type-driven, not hardcoded to Integer64.
        NodeAst expr = FuncCall("coalesce", ArgList(FuncCall("sum", Ident("famount")), FloatLit(0.0)));
        Assert.AreEqual(ColumnType.Float64, InferDerivedColumnType(expr));
    }

    [Test]
    public void SumPlusOne_ArithmeticTopped_TypedInteger64()
    {
        // SUM(amount) + 1 — ExprAdd top; handled by the hoisted compound check, not the ExprFuncCall branch.
        NodeAst expr = Add(FuncCall("sum", Ident("amount")), IntLit(1));
        Assert.AreEqual(ColumnType.Integer64, InferDerivedColumnType(expr));
    }

    [Test]
    public void SumDivSum_ArithmeticTopped_TypedInteger64()
    {
        // SUM(amount) / SUM(qty) — ExprDiv top, both operands bare aggregates over Integer64 columns.
        NodeAst expr = Div(FuncCall("sum", Ident("amount")), FuncCall("sum", Ident("qty")));
        Assert.AreEqual(ColumnType.Integer64, InferDerivedColumnType(expr));
    }

    [Test]
    public void FloatSumDivIntegerSum_WidensToFloat64()
    {
        // SUM(famount) / SUM(qty) — mixed Float64/Integer64 operands widen to Float64.
        NodeAst expr = Div(FuncCall("sum", Ident("famount")), FuncCall("sum", Ident("qty")));
        Assert.AreEqual(ColumnType.Float64, InferDerivedColumnType(expr));
    }

    [Test]
    public void NestedCoalesce_ResolvesRecursively_TypedInteger64()
    {
        // COALESCE(COALESCE(SUM(amount), 0), 0) — inner compound resolved via InferType delegation.
        NodeAst inner = FuncCall("coalesce", ArgList(FuncCall("sum", Ident("amount")), IntLit(0)));
        NodeAst expr = FuncCall("coalesce", ArgList(inner, IntLit(0)));
        Assert.AreEqual(ColumnType.Integer64, InferDerivedColumnType(expr));
    }

    // ── Non-compound baselines (unchanged behavior) ───────────────────────────

    [Test]
    public void BareSum_TypedFromArgument()
    {
        Assert.AreEqual(ColumnType.Integer64, InferDerivedColumnType(FuncCall("sum", Ident("amount"))));
    }

    [Test]
    public void PlainColumn_TypedFromSchema()
    {
        Assert.AreEqual(ColumnType.String, InferDerivedColumnType(Ident("label")));
    }
}
