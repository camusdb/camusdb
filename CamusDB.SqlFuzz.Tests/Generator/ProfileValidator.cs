/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.SqlFuzz.Tests.Generator;

/// <summary>
/// Checks a finished statement tree against a <see cref="GeneratorProfile"/>: every operator, function,
/// expression form, type, join kind and statement form in it must be one the profile allows. A consumer
/// that compares CamusDB with a reference (an in-memory model, PostgreSQL) relies on the profile as a
/// contract, so the check looks at the whole tree, the helper expressions the generator adds on its
/// own (a key offset, a COALESCE for a NOT NULL column, an ORDER BY … LIMIT) included. A unit test runs
/// it over many seeds and profiles.
/// </summary>
internal static class ProfileValidator
{
    public static List<string> Violations(Statement statement, GeneratorProfile profile)
    {
        List<string> found = new();
        void Need(bool allowed, string what)
        {
            if (!allowed)
                found.Add(what);
        }

        switch (statement)
        {
            case InsertStmt insert:
                Need(insert.Rows.Count <= 1 || profile.Allows(StatementFeature.MultiRowInsert), "a multi-row INSERT");
                Need(insert.OnConflict is null || profile.Allows(StatementFeature.Upsert), "an UPSERT");
                break;
            case InsertSelectStmt:
                Need(profile.Allows(StatementFeature.InsertSelect), "an INSERT … SELECT");
                break;
            case UpdateStmt:
                Need(profile.Allows(StatementFeature.Update), "an UPDATE");
                break;
            case DeleteStmt:
                Need(profile.Allows(StatementFeature.Delete), "a DELETE");
                break;
            case AnalyzeStmt:
                Need(profile.Allows(StatementFeature.Analyze), "ANALYZE");
                break;
            case CreateIndexStmt index:
                Need(!index.Index.Unique || profile.Allows(StatementFeature.UniqueIndexes), "a unique index");
                Need(index.Index.Include.Count == 0 || profile.Allows(StatementFeature.CoveringIndexes), "a covering index");
                break;
            case CreateTableStmt create:
                foreach (ColumnDef column in create.Table.Columns)
                    Need(profile.Allows(column.Type), $"a column of type {column.Type}");
                break;
        }

        foreach (object node in ExprWalker.Nodes(statement))
        {
            switch (node)
            {
                case SelectStmt select:
                    CheckSelect(select, profile, Need);
                    break;
                case Expr expression:
                    Need(profile.Allows(expression.Type), $"the type {expression.Type} ({expression.GetType().Name})");
                    CheckExpr(expression, profile, Need);
                    break;
            }
        }
        return found;
    }

    private static void CheckSelect(SelectStmt select, GeneratorProfile profile, Action<bool, string> need)
    {
        FromItem current = select.From;
        while (current is JoinFrom join)
        {
            need(profile.Allows(StatementFeature.Joins), "a join");
            need(profile.Allows(join.Kind), $"a {join.Kind} join");
            need(join.Right.IndexHint is null || profile.Allows(StatementFeature.IndexHints), "an index hint");
            current = join.Left;
        }
        if (current is TableRef { IndexHint: not null })
            need(profile.Allows(StatementFeature.IndexHints), "an index hint");

        need(select.GroupBy is null or { Count: 0 } || profile.Allows(StatementFeature.GroupBy), "GROUP BY");
        need(select.Having is null || profile.Allows(StatementFeature.Having), "HAVING");
        need(!select.Distinct || profile.Allows(StatementFeature.Distinct), "DISTINCT");
        need((select.OrderBy is null && select.Limit is null) || profile.Allows(StatementFeature.OrderByLimit), "ORDER BY … LIMIT");
        if (select.SetOp is not null)
        {
            need(profile.Allows(StatementFeature.SetOperations), "a set operation");
            need(profile.SetOps.Contains(select.SetOp.Kind), $"the set operation {select.SetOp.Kind}");
        }
    }

    private static void CheckExpr(Expr expression, GeneratorProfile profile, Action<bool, string> need)
    {
        switch (expression)
        {
            case UnaryExpr u:
                need(profile.Allows(u.Op), $"the operator {u.Op}");
                break;
            case BinaryExpr b:
                need(profile.Allows(b.Op), $"the operator {b.Op}");
                break;
            case CaseExpr:
                need(profile.Allows(ExprFeature.Case), "CASE");
                break;
            case FunctionExpr { Name: "coalesce" }:
                need(profile.Allows(ExprFeature.Coalesce), "COALESCE");
                break;
            case FunctionExpr { Name: "nullif" }:
                need(profile.Allows(ExprFeature.NullIf), "NULLIF");
                break;
            case FunctionExpr f:
                need(profile.AllowsFunction(f.Name), $"the function {f.Name}");
                break;
            case InListExpr:
                need(profile.Allows(ExprFeature.InList), "IN (list)");
                break;
            case BetweenExpr:
                need(profile.Allows(ExprFeature.Between), "BETWEEN");
                break;
            case LikeExpr:
                need(profile.Allows(ExprFeature.Like), "LIKE");
                break;
            case CastExpr:
                need(profile.Allows(ExprFeature.Cast), "CAST");
                break;
            case JsonFieldExpr:
                need(profile.Allows(ExprFeature.JsonField), "a JSON field access");
                break;
            case ExistsExpr:
                need(profile.Allows(ExprFeature.Exists), "EXISTS");
                break;
            case InSubqueryExpr:
                need(profile.Allows(ExprFeature.InSubquery), "IN (subquery)");
                break;
            case WindowExpr:
                need(profile.Allows(StatementFeature.WindowFunctions), "a window function");
                break;
        }
    }
}
