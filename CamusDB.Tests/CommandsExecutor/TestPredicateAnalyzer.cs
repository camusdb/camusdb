
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Predicates;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.CommandsExecutor;

public class TestPredicateAnalyzer
{
    private static QueryPlannerTestContext? context;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        context = QueryPlannerTestContext.Create();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (context is not null)
        {
            await context.DisposeAsync().ConfigureAwait(false);
            context = null;
        }
    }

    [Test]
    public void Analyze_RangeConjunction_producesTwoIndexableComparisons()
    {
        NodeAst where = SQLParserProcessor.Parse("SELECT * FROM robots WHERE year >= 2001 AND year < 2005").extendedOne!;

        PredicateAnalysis analysis = PredicateAnalyzer.Analyze(where, parameters: null);

        Assert.AreEqual(2, analysis.IndexableComparisons.Count);
        Assert.AreEqual(0, analysis.ResidualConjuncts.Count);
        Assert.AreEqual("year", analysis.IndexableComparisons[0].ColumnName);
        Assert.AreEqual(">=", analysis.IndexableComparisons[0].Operator);
        Assert.AreEqual(2001, analysis.IndexableComparisons[0].Constant.LongValue);
        Assert.AreEqual("<", analysis.IndexableComparisons[1].Operator);
        Assert.AreEqual(2005, analysis.IndexableComparisons[1].Constant.LongValue);
    }

    [Test]
    public void Analyze_ParameterizedEquality_producesIndexableComparison()
    {
        NodeAst where = SQLParserProcessor.Parse("SELECT * FROM robots WHERE id = @id").extendedOne!;
        Dictionary<string, ColumnValue> parameters = new()
        {
            { "@id", new(ColumnType.Id, QueryPlannerTestContext.SampleRowId) }
        };

        PredicateAnalysis analysis = PredicateAnalyzer.Analyze(where, parameters);

        Assert.AreEqual(1, analysis.IndexableComparisons.Count);
        Assert.AreEqual("=", analysis.IndexableComparisons[0].Operator);
        Assert.AreEqual(QueryPlannerTestContext.SampleRowId, analysis.IndexableComparisons[0].Constant.StrValue);
    }

    [Test]
    public void Analyze_OrExpression_keepsWholePredicateAsResidual()
    {
        NodeAst where = SQLParserProcessor.Parse("SELECT * FROM robots WHERE year = 2000 OR year = 2001").extendedOne!;

        PredicateAnalysis analysis = PredicateAnalyzer.Analyze(where, parameters: null);

        Assert.AreEqual(0, analysis.IndexableComparisons.Count);
        Assert.AreEqual(1, analysis.ResidualConjuncts.Count);
        Assert.AreEqual(NodeType.ExprOr, analysis.ResidualConjuncts[0].nodeType);
    }

    [Test]
    public void Analyze_LikeExpression_keepsPredicateAsResidual()
    {
        NodeAst where = SQLParserProcessor.Parse("SELECT * FROM robots WHERE name LIKE 'bot%'").extendedOne!;

        PredicateAnalysis analysis = PredicateAnalyzer.Analyze(where, parameters: null);

        Assert.AreEqual(0, analysis.IndexableComparisons.Count);
        Assert.AreEqual(1, analysis.ResidualConjuncts.Count);
        Assert.AreEqual(NodeType.ExprLike, analysis.ResidualConjuncts[0].nodeType);
    }

    [Test]
    public void BuildExecutionFilter_RangeIndexScan_omitsAbsorbedBounds()
    {
        NodeAst where = SQLParserProcessor.Parse("SELECT * FROM robots WHERE year >= 2001 AND year < 2005").extendedOne!;
        PredicateAnalysis analysis = PredicateAnalyzer.Analyze(where, parameters: null);

        QueryPlanStep rangeStep = new(
            QueryPlanStepType.RangeScanFromIndex,
            new("year_idx", ["year"], IndexType.Multi),
            new CompositeColumnValue(new[] { new ColumnValue(ColumnType.Integer64, 2001) }),
            fromInclusive: true,
            new CompositeColumnValue(new[] { new ColumnValue(ColumnType.Integer64, 2005) }),
            toInclusive: false);

        Assert.IsNull(PredicateAnalyzer.BuildExecutionFilter(analysis, rangeStep, context!.Table));
    }

    [Test]
    public void Analyze_BetweenExpandsToInclusiveRangeComparisons()
    {
        NodeAst where = SQLParserProcessor.Parse("SELECT * FROM robots WHERE year BETWEEN 2001 AND 2004").extendedOne!;
        PredicateAnalysis analysis = PredicateAnalyzer.Analyze(where, parameters: null);

        Assert.AreEqual(2, analysis.IndexableComparisons.Count);
        Assert.AreEqual(">=", analysis.IndexableComparisons[0].Operator);
        Assert.AreEqual(2001, analysis.IndexableComparisons[0].Constant.LongValue);
        Assert.AreEqual("<=", analysis.IndexableComparisons[1].Operator);
        Assert.AreEqual(2004, analysis.IndexableComparisons[1].Constant.LongValue);
        Assert.AreEqual(0, analysis.ResidualConjuncts.Count);
    }

    [Test]
    public void BuildExecutionFilter_BetweenRangeIndexScan_omitsAbsorbedBounds()
    {
        NodeAst where = SQLParserProcessor.Parse("SELECT * FROM robots WHERE year BETWEEN 2001 AND 2004").extendedOne!;
        PredicateAnalysis analysis = PredicateAnalyzer.Analyze(where, parameters: null);

        QueryPlanStep rangeStep = new(
            QueryPlanStepType.RangeScanFromIndex,
            new("year_idx", ["year"], IndexType.Multi),
            new CompositeColumnValue(new[] { new ColumnValue(ColumnType.Integer64, 2001) }),
            fromInclusive: true,
            new CompositeColumnValue(new[] { new ColumnValue(ColumnType.Integer64, 2004) }),
            toInclusive: true);

        Assert.IsNull(PredicateAnalyzer.BuildExecutionFilter(analysis, rangeStep, context!.Table));
    }

    [Test]
    public void BuildExecutionFilter_PointRangeScan_omitsMatchingEqualityOnly()
    {
        NodeAst where = SQLParserProcessor.Parse(
            "SELECT * FROM robots WHERE year = 2000 AND year > 2001").extendedOne!;
        PredicateAnalysis analysis = PredicateAnalyzer.Analyze(where, parameters: null);

        QueryPlanStep rangeStep = new(
            QueryPlanStepType.RangeScanFromIndex,
            new("year_idx", ["year"], IndexType.Multi),
            new CompositeColumnValue(new[] { new ColumnValue(ColumnType.Integer64, 2000) }),
            fromInclusive: true,
            new CompositeColumnValue(new[] { new ColumnValue(ColumnType.Integer64, 2001) }),
            toInclusive: false);

        NodeAst? executionFilter = PredicateAnalyzer.BuildExecutionFilter(analysis, rangeStep, context!.Table);

        Assert.IsNotNull(executionFilter);
        Assert.AreEqual(NodeType.ExprGreaterThan, executionFilter!.nodeType);
        Assert.AreEqual(2001, long.Parse(executionFilter.rightAst!.yytext!));
    }

    [Test]
    public void BuildExecutionFilter_PointRangeScan_keepsConflictingNotEquals()
    {
        NodeAst where = SQLParserProcessor.Parse(
            "SELECT * FROM robots WHERE year = 2000 AND year != 2000").extendedOne!;
        PredicateAnalysis analysis = PredicateAnalyzer.Analyze(where, parameters: null);

        QueryPlanStep rangeStep = new(
            QueryPlanStepType.RangeScanFromIndex,
            new("year_idx", ["year"], IndexType.Multi),
            new CompositeColumnValue(new[] { new ColumnValue(ColumnType.Integer64, 2000) }),
            fromInclusive: true,
            new CompositeColumnValue(new[] { new ColumnValue(ColumnType.Integer64, 2001) }),
            toInclusive: false);

        NodeAst? executionFilter = PredicateAnalyzer.BuildExecutionFilter(analysis, rangeStep, context!.Table);

        Assert.IsNotNull(executionFilter);
        Assert.AreEqual(NodeType.ExprNotEquals, executionFilter!.nodeType);
    }

    [Test]
    public void AnalyzeFilters_Equality_producesIndexableComparison()
    {
        List<QueryFilter> filters =
        [
            new("id", "=", new(ColumnType.Id, QueryPlannerTestContext.SampleRowId))
        ];

        PredicateAnalysis analysis = PredicateAnalyzer.AnalyzeFilters(filters);

        Assert.AreEqual(1, analysis.IndexableComparisons.Count);
        Assert.AreEqual("=", analysis.IndexableComparisons[0].Operator);
        Assert.AreEqual(QueryPlannerTestContext.SampleRowId, analysis.IndexableComparisons[0].Constant.StrValue);
        Assert.AreEqual(NodeType.ObjectIdLiteral, analysis.IndexableComparisons[0].Conjunct.rightAst!.nodeType);
    }

    [Test]
    public void BuildExecutionFilter_IdNotEquals_evaluatesWithoutTypeMismatch()
    {
        string excludedId = QueryPlannerTestContext.SampleRowId;
        string rowId = "0123456789abcdef01234568";

        List<QueryFilter> filters = [new("id", "!=", new(ColumnType.Id, excludedId))];
        PredicateAnalysis analysis = PredicateAnalyzer.AnalyzeFilters(filters);

        NodeAst? executionFilter = PredicateAnalyzer.BuildExecutionFilter(analysis, scanStep: null, context!.Table);

        Assert.IsNotNull(executionFilter);
        Assert.AreEqual(NodeType.ExprNotEquals, executionFilter!.nodeType);
        Assert.AreEqual(NodeType.ObjectIdLiteral, executionFilter.rightAst!.nodeType);

        Dictionary<string, ColumnValue> row = new()
        {
            { "id", new(ColumnType.Id, rowId) }
        };

        ColumnValue result = SqlExecutor.EvalExpr(executionFilter, row, parameters: null);
        Assert.IsTrue(result.BoolValue);

        row["id"] = new(ColumnType.Id, excludedId);
        result = SqlExecutor.EvalExpr(executionFilter, row, parameters: null);
        Assert.IsFalse(result.BoolValue);
    }

    [Test]
    public void AnalyzeTicket_MergesWhereAndFilters()
    {
        NodeAst where = SQLParserProcessor.Parse("SELECT * FROM robots WHERE year >= 2001").extendedOne!;
        List<QueryFilter> filters = [new("enabled", "=", new(ColumnType.Bool, true))];

        QueryTicket ticket = new(
            txnState: context!.Txn,
            databaseName: QueryPlannerTestContext.DatabaseName,
            tableName: "robots",
            index: null,
            projection: null,
            filters: filters,
            where: where,
            orderBy: null,
            limit: null,
            offset: null,
            parameters: null,
            analyzedWhere: PredicateAnalyzer.Analyze(where, parameters: null)
        );

        PredicateAnalysis analysis = PredicateAnalyzer.AnalyzeTicket(ticket);

        Assert.AreEqual(2, analysis.IndexableComparisons.Count);
    }

    [Test]
    public void Analyze_ColumnColumnComparison_extractsStructuredComparison()
    {
        NodeAst where = SQLParserProcessor.Parse("SELECT * FROM robots WHERE year > enabled").extendedOne!;

        PredicateAnalysis analysis = PredicateAnalyzer.Analyze(where, parameters: null);

        Assert.AreEqual(0, analysis.IndexableComparisons.Count);
        Assert.AreEqual(1, analysis.ColumnComparisons.Count);
        Assert.AreEqual(0, analysis.ResidualConjuncts.Count);
        Assert.AreEqual("year", analysis.ColumnComparisons[0].LeftColumnName);
        Assert.AreEqual(">", analysis.ColumnComparisons[0].Operator);
        Assert.AreEqual("enabled", analysis.ColumnComparisons[0].RightColumnName);
    }

    [Test]
    public void Analyze_MixedConstantAndColumnComparisons_classifiesEachConjunct()
    {
        NodeAst where = SQLParserProcessor.Parse(
            "SELECT * FROM robots WHERE year >= 2001 AND year > enabled").extendedOne!;

        PredicateAnalysis analysis = PredicateAnalyzer.Analyze(where, parameters: null);

        Assert.AreEqual(1, analysis.IndexableComparisons.Count);
        Assert.AreEqual(1, analysis.ColumnComparisons.Count);
        Assert.AreEqual(0, analysis.ResidualConjuncts.Count);
        Assert.AreEqual("year", analysis.IndexableComparisons[0].ColumnName);
        Assert.AreEqual("year", analysis.ColumnComparisons[0].LeftColumnName);
    }

    [Test]
    public void BuildExecutionFilter_IncludesColumnColumnComparison()
    {
        NodeAst where = SQLParserProcessor.Parse("SELECT * FROM robots WHERE year > enabled").extendedOne!;
        PredicateAnalysis analysis = PredicateAnalyzer.Analyze(where, parameters: null);

        NodeAst? executionFilter = PredicateAnalyzer.BuildExecutionFilter(analysis, scanStep: null, context!.Table);

        Assert.IsNotNull(executionFilter);
        Assert.AreEqual(NodeType.ExprGreaterThan, executionFilter!.nodeType);
        Assert.AreEqual("year", executionFilter.leftAst!.yytext);
        Assert.AreEqual("enabled", executionFilter.rightAst!.yytext);
    }

    [Test]
    public void NextSortValue_Float64_usesNextRepresentableValue()
    {
        double value = 1.0;
        ColumnValue? next = IndexScanSelector.NextSortValue(ColumnType.Float64, new ColumnValue(ColumnType.Float64, value));

        Assert.IsNotNull(next);
        Assert.AreEqual(Math.BitIncrement(value), next!.FloatValue);
    }

    [Test]
    public void NextSortValue_Float64_rejectsLargeMagnitudeWhereEpsilonWouldFail()
    {
        double value = 1_000_000_000_000.0;
        ColumnValue? next = IndexScanSelector.NextSortValue(ColumnType.Float64, new ColumnValue(ColumnType.Float64, value));

        Assert.IsNotNull(next);
        Assert.Greater(next!.FloatValue, value);
    }

    [Test]
    public void NextSortValue_Float64_returnsNullForNonFiniteValues()
    {
        Assert.IsNull(IndexScanSelector.NextSortValue(
            ColumnType.Float64,
            new ColumnValue(ColumnType.Float64, double.PositiveInfinity)));
        Assert.IsNull(IndexScanSelector.NextSortValue(
            ColumnType.Float64,
            new ColumnValue(ColumnType.Float64, double.NaN)));
    }

    [Test]
    public void BuildExecutionFilter_FullScan_appliesAllIndexableConjuncts()
    {
        NodeAst where = SQLParserProcessor.Parse("SELECT * FROM robots WHERE year >= 2001 AND year < 2005").extendedOne!;
        PredicateAnalysis analysis = PredicateAnalyzer.Analyze(where, parameters: null);

        NodeAst? executionFilter = PredicateAnalyzer.BuildExecutionFilter(analysis, scanStep: null, context!.Table);

        Assert.IsNotNull(executionFilter);
        Assert.AreEqual(NodeType.ExprAnd, executionFilter!.nodeType);
    }

    [Test]
    public void BuildExecutionFilter_MixedPredicate_keepsNonScannedColumnConjunct()
    {
        NodeAst where = SQLParserProcessor.Parse(
            "SELECT * FROM robots WHERE year >= 2001 AND enabled = true").extendedOne!;

        PredicateAnalysis analysis = PredicateAnalyzer.Analyze(where, parameters: null);

        QueryPlanStep rangeStep = new(
            QueryPlanStepType.RangeScanFromIndex,
            new("year_idx", ["year"], IndexType.Multi),
            new CompositeColumnValue(new[] { new ColumnValue(ColumnType.Integer64, 2001) }),
            fromInclusive: true,
            toBound: null,
            toInclusive: true);

        NodeAst? executionFilter = PredicateAnalyzer.BuildExecutionFilter(analysis, rangeStep, context!.Table);

        Assert.IsNotNull(executionFilter);
        Assert.AreEqual(NodeType.ExprEquals, executionFilter!.nodeType);
        Assert.AreEqual("enabled", executionFilter.leftAst!.yytext);
    }
}
