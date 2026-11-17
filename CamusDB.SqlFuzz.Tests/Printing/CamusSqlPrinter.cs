/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using System.Text;

using CamusDB.SqlFuzz.Tests.Generator;

namespace CamusDB.SqlFuzz.Tests.Printing;

/// <summary>
/// Turns a statement tree into the SQL text of one dialect. Every dialect detail lives in an
/// implementation: type names, literal forms, hint syntax and function spellings. A differential
/// oracle against another database adds a second implementation and uses the same trees.
/// </summary>
internal interface ISqlPrinter
{
    string Print(Statement statement);

    string Print(Expr expression);

    string TypeName(FuzzType type);
}

/// <summary>
/// The CamusDB dialect.
///
/// <list type="bullet">
/// <item>Every compound expression is in parentheses, so the text never depends on operator precedence.</item>
/// <item>A NULL inside an expression is <c>CAST(NULL AS type)</c>, so it keeps its type. In a VALUES list it is a plain <c>NULL</c>.</item>
/// <item>NUMERIC, DATE, DATETIME and JSONB literals are casts of a string; NaN and the infinities are casts of their names. An INTERVAL literal is <c>INTERVAL '…'</c>.</item>
/// <item>The index hint follows the alias: <c>t0 AS a0@{FORCE_INDEX=t0_i1}</c>. CamusDB refuses the hint before the alias.</item>
/// <item>A SELECT with <see cref="SelectStmt.CteTables"/> starts with <c>WITH cte_t0 AS (SELECT * FROM t0)</c>, and each
/// reference marked <see cref="TableRef.ViaCte"/> under that WITH reads <c>cte_t0</c>. The right operand of a set operation
/// reads the CTEs of the WITH in front of the whole query, because it cannot start with a WITH of its own.</item>
/// </list>
/// </summary>
internal sealed class CamusSqlPrinter : ISqlPrinter
{
    public static CamusSqlPrinter Instance { get; } = new();

    public string TypeName(FuzzType type) => type switch
    {
        FuzzType.Int64 => "int64",
        FuzzType.Float64 => "float64",
        FuzzType.Numeric => "numeric",
        FuzzType.String => "string",
        FuzzType.Bool => "bool",
        FuzzType.Date => "date",
        FuzzType.DateTime => "datetime",
        FuzzType.Jsonb => "jsonb",
        FuzzType.Interval => "interval",
        _ => throw new ArgumentOutOfRangeException(nameof(type)),
    };

    public string Print(Statement statement)
    {
        StringBuilder sb = new();
        Write(sb, statement);
        return sb.ToString();
    }

    public string Print(Expr expression)
    {
        StringBuilder sb = new();
        Write(sb, expression);
        return sb.ToString();
    }

    private void Write(StringBuilder sb, Statement statement)
    {
        switch (statement)
        {
            case CreateTableStmt create:
                sb.Append("CREATE TABLE ").Append(create.Table.Name).Append(" (");
                for (int i = 0; i < create.Table.Columns.Count; i++)
                {
                    ColumnDef column = create.Table.Columns[i];
                    if (i > 0)
                        sb.Append(", ");
                    sb.Append(column.Name).Append(' ').Append(TypeName(column.Type));
                    if (column.PrimaryKey)
                        sb.Append(" PRIMARY KEY");
                    else if (column.NotNull)
                        sb.Append(" NOT NULL");
                }
                sb.Append(')');
                break;

            case CreateIndexStmt index:
                sb.Append(index.Index.Unique ? "CREATE UNIQUE INDEX " : "CREATE INDEX ")
                    .Append(index.Index.Name).Append(" ON ").Append(index.Table)
                    .Append(" (").Append(string.Join(", ", index.Index.Columns)).Append(')');
                if (index.Index.Include.Count > 0)
                    sb.Append(" INCLUDE (").Append(string.Join(", ", index.Index.Include)).Append(')');
                break;

            case InsertStmt insert:
                sb.Append("INSERT INTO ").Append(insert.Table).Append(" (").Append(string.Join(", ", insert.Columns)).Append(") VALUES ");
                for (int r = 0; r < insert.Rows.Count; r++)
                {
                    if (r > 0)
                        sb.Append(", ");
                    sb.Append('(');
                    for (int c = 0; c < insert.Rows[r].Count; c++)
                    {
                        if (c > 0)
                            sb.Append(", ");
                        Expr cell = insert.Rows[r][c];
                        if (cell is LiteralExpr { Value.IsNull: true })
                            sb.Append("NULL");
                        else
                            Write(sb, cell);
                    }
                    sb.Append(')');
                }
                if (insert.OnConflict is { } conflict)
                {
                    sb.Append(" ON CONFLICT (").Append(string.Join(", ", conflict.Target)).Append(')');
                    if (conflict.DoUpdate is null)
                        sb.Append(" DO NOTHING");
                    else
                    {
                        sb.Append(" DO UPDATE SET ");
                        WriteAssignments(sb, conflict.DoUpdate);
                    }
                }
                break;

            case InsertSelectStmt insertSelect:
                sb.Append("INSERT INTO ").Append(insertSelect.Table).Append(" (").Append(string.Join(", ", insertSelect.Columns)).Append(") ");
                Write(sb, insertSelect.Source);
                break;

            case UpdateStmt update:
                sb.Append("UPDATE ").Append(update.Table);
                if (update.Alias is { } updateAlias)
                    sb.Append(" AS ").Append(updateAlias);
                sb.Append(" SET ");
                WriteAssignments(sb, update.Set);
                if (update.From is { } from)
                {
                    sb.Append(" FROM ");
                    WriteFrom(sb, from);
                }
                sb.Append(" WHERE ");
                Write(sb, update.Where);
                break;

            case DeleteStmt delete:
                sb.Append("DELETE FROM ").Append(delete.Table);
                if (delete.Alias is { } deleteAlias)
                    sb.Append(" AS ").Append(deleteAlias);
                if (delete.Using is { } list)
                {
                    sb.Append(" USING ");
                    WriteFrom(sb, list);
                }
                sb.Append(" WHERE ");
                Write(sb, delete.Where);
                break;

            case AnalyzeStmt analyze:
                sb.Append("ANALYZE ").Append(analyze.Table);
                break;

            case ExplainStmt explain:
                sb.Append("EXPLAIN ");
                Write(sb, explain.Query);
                break;

            case SelectStmt select:
                WriteSelect(sb, select);
                break;

            case BeginStmt:
                sb.Append("BEGIN");
                break;

            case CommitStmt:
                sb.Append("COMMIT");
                break;

            case RollbackStmt:
                sb.Append("ROLLBACK");
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(statement), statement.GetType().Name);
        }
    }

    private void WriteAssignments(StringBuilder sb, IReadOnlyList<Assignment> assignments)
    {
        for (int i = 0; i < assignments.Count; i++)
        {
            if (i > 0)
                sb.Append(", ");
            sb.Append(assignments[i].Column).Append(" = ");
            Write(sb, assignments[i].Value);
        }
    }

    /// <summary>The name of the CTE through which a reference marked <see cref="TableRef.ViaCte"/> reads <paramref name="table"/>.</summary>
    internal static string CteName(string table) => "cte_" + table;

    private void WriteSelect(StringBuilder sb, SelectStmt select) => WriteSelect(sb, select, declared: null);

    /// <param name="declared">The tables of the WITH that covers this SELECT: the WITH that this method
    /// writes for <see cref="SelectStmt.CteTables"/>, or the WITH of the SELECT whose set operation this one
    /// is the right operand of. A right operand cannot start with its own WITH.</param>
    private void WriteSelect(StringBuilder sb, SelectStmt select, IReadOnlyList<string>? declared)
    {
        if (declared is null && select.CteTables is { Count: > 0 } cteTables)
        {
            sb.Append("WITH ");
            for (int i = 0; i < cteTables.Count; i++)
            {
                if (i > 0)
                    sb.Append(", ");
                sb.Append(CteName(cteTables[i])).Append(" AS (SELECT * FROM ").Append(cteTables[i]).Append(')');
            }
            sb.Append(' ');
            declared = cteTables;
        }

        sb.Append("SELECT ");
        if (select.Distinct)
            sb.Append("DISTINCT ");

        bool lateralItems = PrintsItemsAsLateral(select);

        if (select.Items.Count == 0)
            sb.Append('*');
        else
        {
            for (int i = 0; i < select.Items.Count; i++)
            {
                if (i > 0)
                    sb.Append(", ");
                if (lateralItems)
                    sb.Append(LateralItemsAlias).Append(".e").Append(i);
                else
                    Write(sb, select.Items[i].Expr);
                if (select.Items[i].Alias is { } alias)
                    sb.Append(" AS ").Append(alias);
            }
        }

        sb.Append(" FROM ");
        WriteFrom(sb, select.From, declared, select.LateralJoins);

        if (lateralItems)
        {
            sb.Append(" CROSS JOIN LATERAL (SELECT ");
            for (int i = 0; i < select.Items.Count; i++)
            {
                if (i > 0)
                    sb.Append(", ");
                Write(sb, select.Items[i].Expr);
                sb.Append(" AS e").Append(i);
            }
            sb.Append(") AS ").Append(LateralItemsAlias);
        }

        if (select.Where is not null)
        {
            sb.Append(" WHERE ");
            Write(sb, select.Where);
        }

        if (select.GroupBy is { Count: > 0 } groupBy)
        {
            sb.Append(" GROUP BY ");
            for (int i = 0; i < groupBy.Count; i++)
            {
                if (i > 0)
                    sb.Append(", ");
                Write(sb, groupBy[i]);
            }
        }

        if (select.Having is not null)
        {
            sb.Append(" HAVING ");
            Write(sb, select.Having);
        }

        if (select.SetOp is { } setOp)
        {
            sb.Append(setOp.Kind switch
            {
                SetOpKind.Union => " UNION ",
                SetOpKind.UnionAll => " UNION ALL ",
                SetOpKind.Intersect => " INTERSECT ",
                _ => " EXCEPT ",
            });
            WriteSelect(sb, setOp.Right, declared ?? []);
        }

        if (select.OrderBy is { Count: > 0 } orderBy)
        {
            sb.Append(" ORDER BY ");
            for (int i = 0; i < orderBy.Count; i++)
            {
                if (i > 0)
                    sb.Append(", ");
                Write(sb, orderBy[i].Expr);
                if (orderBy[i].Descending)
                    sb.Append(" DESC");
            }
        }

        if (select.Limit is { } limit)
            sb.Append(" LIMIT ").Append(limit.ToString(CultureInfo.InvariantCulture));
        if (select.Offset is { } offset)
            sb.Append(" OFFSET ").Append(offset.ToString(CultureInfo.InvariantCulture));
    }

    private void WriteFrom(StringBuilder sb, FromItem from) => WriteFrom(sb, from, declared: null, lateral: false);

    private void WriteFrom(StringBuilder sb, FromItem from, IReadOnlyList<string>? declared, bool lateral)
    {
        switch (from)
        {
            case JoinFrom join when lateral && PrintsAsLateral(join):
                WriteFrom(sb, join.Left, declared, lateral);
                sb.Append(join.Kind == JoinKind.Left ? " LEFT JOIN LATERAL (SELECT * FROM " : " CROSS JOIN LATERAL (SELECT * FROM ");
                WriteFrom(sb, join.Right, declared, lateral);
                sb.Append(" WHERE ");
                Write(sb, join.On!);
                sb.Append(") AS ").Append(join.Right.Alias);
                if (join.Kind == JoinKind.Left)
                    sb.Append(" ON TRUE");
                break;

            case TableRef table:
                bool viaCte = table.ViaCte && table.IndexHint is null && declared is not null && declared.Contains(table.Table);
                sb.Append(viaCte ? CteName(table.Table) : table.Table).Append(" AS ").Append(table.Alias);
                if (table.IndexHint is { } hint)
                    sb.Append("@{FORCE_INDEX=").Append(hint).Append('}');
                break;

            case ValuesRef values:
                sb.Append("(VALUES ");
                for (int r = 0; r < values.Rows.Count; r++)
                {
                    sb.Append(r == 0 ? "(" : ", (");
                    for (int i = 0; i < values.Rows[r].Count; i++)
                    {
                        if (i > 0)
                            sb.Append(", ");
                        Write(sb, values.Rows[r][i]);
                    }
                    sb.Append(')');
                }
                sb.Append(") AS ").Append(values.Alias).Append(" (")
                  .Append(string.Join(", ", values.Columns.Select(c => c.Name))).Append(')');
                break;

            case JoinFrom join:
                WriteFrom(sb, join.Left, declared, lateral);
                sb.Append(join.Kind switch
                {
                    JoinKind.Inner => " INNER JOIN ",
                    JoinKind.Left => " LEFT JOIN ",
                    JoinKind.Right => " RIGHT JOIN ",
                    _ => " CROSS JOIN ",
                });
                WriteFrom(sb, join.Right, declared, lateral);
                if (join.On is not null)
                {
                    sb.Append(" ON ");
                    Write(sb, join.On);
                }
                break;
        }
    }

    /// <summary>The alias of the <c>LATERAL</c> derived table of <see cref="SelectStmt.LateralItems"/>; no table alias starts with <c>lx</c>.</summary>
    internal const string LateralItemsAlias = "lx";

    /// <summary>
    /// True when <paramref name="select"/> prints its select-list expressions in a <c>LATERAL</c> derived table
    /// (see <see cref="SelectStmt.LateralItems"/>): each item has an alias, so the output names do not change, and
    /// no item holds an aggregate or a window call, which a derived table with no FROM cannot compute. A SELECT with
    /// GROUP BY, HAVING or a set operation prints as written.
    /// </summary>
    internal static bool PrintsItemsAsLateral(SelectStmt select)
    {
        if (!select.LateralItems || select.Items.Count == 0 || select.GroupBy is { Count: > 0 } || select.Having is not null || select.SetOp is not null)
            return false;

        foreach (SelectItem item in select.Items)
        {
            if (item.Alias is null || HoldsAggregateOrWindow(item.Expr))
                return false;
        }

        return true;
    }

    private static bool HoldsAggregateOrWindow(Expr expression)
    {
        Stack<object> pending = new();
        pending.Push(expression);

        while (pending.TryPop(out object? node))
        {
            if (node is AggregateExpr or WindowExpr)
                return true;

            // An aggregate inside a subquery belongs to that subquery.
            if (node is SelectStmt or ExistsExpr or InSubqueryExpr)
                continue;

            foreach (object child in ExprWalker.Children(node))
                pending.Push(child);
        }

        return false;
    }

    /// <summary>
    /// True when <paramref name="join"/> prints as a <c>LATERAL</c> derived table (see <see cref="JoinFrom.ViaLateral"/>):
    /// an inner or LEFT join with an ON, no index hint on the right table, and no subquery in the ON. A subquery in
    /// an ON is refused when it is correlated, and the WHERE of the derived table would accept it.
    /// </summary>
    internal static bool PrintsAsLateral(JoinFrom join) =>
        join.ViaLateral
        && join.Kind is JoinKind.Inner or JoinKind.Left
        && join.On is not null
        && join.Right.IndexHint is null
        && !HoldsSubquery(join.On);

    private static bool HoldsSubquery(Expr expression)
    {
        Stack<object> pending = new();
        pending.Push(expression);

        while (pending.TryPop(out object? node))
        {
            if (node is ExistsExpr or InSubqueryExpr or SelectStmt)
                return true;

            foreach (object child in ExprWalker.Children(node))
                pending.Push(child);
        }

        return false;
    }

    private void Write(StringBuilder sb, Expr expression)
    {
        switch (expression)
        {
            case ColumnExpr column:
                if (column.Source is not null)
                    sb.Append(column.Source).Append('.');
                sb.Append(column.Column);
                break;

            case LiteralExpr literal:
                WriteLiteral(sb, literal.Value);
                break;

            case UnaryExpr unary:
                sb.Append('(');
                switch (unary.Op)
                {
                    case UnaryOp.Not:
                        sb.Append("NOT ");
                        Write(sb, unary.Operand);
                        break;
                    case UnaryOp.Negate:
                        sb.Append("- ");
                        Write(sb, unary.Operand);
                        break;
                    default:
                        Write(sb, unary.Operand);
                        sb.Append(unary.Op switch
                        {
                            UnaryOp.IsNull => " IS NULL",
                            UnaryOp.IsNotNull => " IS NOT NULL",
                            UnaryOp.IsTrue => " IS TRUE",
                            _ => " IS FALSE",
                        });
                        break;
                }
                sb.Append(')');
                break;

            case BinaryExpr binary:
                sb.Append('(');
                Write(sb, binary.Left);
                sb.Append(binary.Op switch
                {
                    BinaryOp.And => " AND ",
                    BinaryOp.Or => " OR ",
                    BinaryOp.Eq => " = ",
                    BinaryOp.Ne => " <> ",
                    BinaryOp.Lt => " < ",
                    BinaryOp.Le => " <= ",
                    BinaryOp.Gt => " > ",
                    BinaryOp.Ge => " >= ",
                    BinaryOp.Add => " + ",
                    BinaryOp.Sub => " - ",
                    BinaryOp.Mul => " * ",
                    BinaryOp.Div => " / ",
                    BinaryOp.Mod => " % ",
                    BinaryOp.Concat => " || ",
                    BinaryOp.JsonContains => " @> ",
                    _ => throw new ArgumentOutOfRangeException(nameof(expression)),
                });
                Write(sb, binary.Right);
                sb.Append(')');
                break;

            case CaseExpr caseExpr:
                sb.Append("(CASE");
                foreach (CaseBranch branch in caseExpr.Branches)
                {
                    sb.Append(" WHEN ");
                    Write(sb, branch.When);
                    sb.Append(" THEN ");
                    Write(sb, branch.Then);
                }
                sb.Append(" ELSE ");
                if (caseExpr.Else is null)
                    WriteLiteral(sb, FuzzValue.Null(caseExpr.ResultType));
                else
                    Write(sb, caseExpr.Else);
                sb.Append(" END)");
                break;

            case FunctionExpr function:
                sb.Append(function.Name).Append('(');
                for (int i = 0; i < function.Args.Count; i++)
                {
                    if (i > 0)
                        sb.Append(", ");
                    Write(sb, function.Args[i]);
                }
                sb.Append(')');
                break;

            case InListExpr inList:
                sb.Append('(');
                Write(sb, inList.Value);
                sb.Append(inList.Negated ? " NOT IN (" : " IN (");
                for (int i = 0; i < inList.Items.Count; i++)
                {
                    if (i > 0)
                        sb.Append(", ");
                    if (inList.Items[i] is LiteralExpr item)
                        WriteSimpleLiteral(sb, item.Value);
                    else
                        Write(sb, inList.Items[i]);
                }
                sb.Append("))");
                break;

            case BetweenExpr between:
                sb.Append('(');
                Write(sb, between.Value);
                sb.Append(between.Negated ? " NOT BETWEEN " : " BETWEEN ");
                Write(sb, between.Low);
                sb.Append(" AND ");
                Write(sb, between.High);
                sb.Append(')');
                break;

            case LikeExpr like:
                sb.Append('(');
                Write(sb, like.Value);
                sb.Append(like.Negated ? " NOT LIKE " : " LIKE ");
                Write(sb, like.Pattern);
                sb.Append(')');
                break;

            case CastExpr cast:
                sb.Append("CAST(");
                Write(sb, cast.Operand);
                sb.Append(" AS ").Append(TypeName(cast.Target)).Append(')');
                break;

            case JsonFieldExpr field:
                sb.Append('(');
                Write(sb, field.Document);
                sb.Append(field.AsText ? " ->> " : " -> ");
                WriteString(sb, field.Key);
                sb.Append(')');
                break;

            case ExistsExpr exists:
                sb.Append(exists.Negated ? "(NOT EXISTS (" : "(EXISTS (");
                WriteSelect(sb, exists.Subquery);
                sb.Append("))");
                break;

            case InSubqueryExpr inSubquery:
                sb.Append('(');
                Write(sb, inSubquery.Value);
                sb.Append(inSubquery.Negated ? " NOT IN (" : " IN (");
                WriteSelect(sb, inSubquery.Subquery);
                sb.Append("))");
                break;

            case AggregateExpr aggregate:
                sb.Append(aggregate.Func switch
                {
                    AggregateFunc.CountStar or AggregateFunc.Count => "COUNT(",
                    AggregateFunc.Sum => "SUM(",
                    AggregateFunc.Min => "MIN(",
                    AggregateFunc.Max => "MAX(",
                    _ => "AVG(",
                });
                if (aggregate.Argument is null)
                    sb.Append('*');
                else
                    Write(sb, aggregate.Argument);
                sb.Append(')');
                break;

            case WindowExpr window:
                sb.Append(window.Func switch
                {
                    WindowFunc.RowNumber => "row_number()",
                    WindowFunc.Rank => "rank()",
                    WindowFunc.DenseRank => "dense_rank()",
                    WindowFunc.Count => "count(*)",
                    _ => "sum(",
                });
                if (window.Func == WindowFunc.Sum)
                {
                    Write(sb, window.Argument!);
                    sb.Append(')');
                }
                sb.Append(" OVER (");
                if (window.PartitionBy.Count > 0)
                {
                    sb.Append("PARTITION BY ");
                    for (int i = 0; i < window.PartitionBy.Count; i++)
                    {
                        if (i > 0)
                            sb.Append(", ");
                        Write(sb, window.PartitionBy[i]);
                    }
                }
                if (window.OrderBy.Count > 0)
                {
                    sb.Append(window.PartitionBy.Count > 0 ? " ORDER BY " : "ORDER BY ");
                    for (int i = 0; i < window.OrderBy.Count; i++)
                    {
                        if (i > 0)
                            sb.Append(", ");
                        Write(sb, window.OrderBy[i].Expr);
                        if (window.OrderBy[i].Descending)
                            sb.Append(" DESC");
                    }
                }
                sb.Append(')');
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(expression), expression.GetType().Name);
        }
    }

    private void WriteLiteral(StringBuilder sb, FuzzValue value)
    {
        if (value.IsNull)
        {
            sb.Append("CAST(NULL AS ").Append(TypeName(value.Type)).Append(')');
            return;
        }

        switch (value.Type)
        {
            case FuzzType.Int64:
                // A negative constant is written directly: "-5" is a constant, and the smallest INT64
                // has no positive form.
                sb.Append(value.Int.ToString(CultureInfo.InvariantCulture));
                break;
            case FuzzType.Float64:
                WriteFloat(sb, value.Float);
                break;
            case FuzzType.Numeric:
                sb.Append("CAST(");
                WriteString(sb, value.Text!);
                sb.Append(" AS numeric)");
                break;
            case FuzzType.String:
                WriteString(sb, value.Text!);
                break;
            case FuzzType.Bool:
                sb.Append(value.Bool ? "TRUE" : "FALSE");
                break;
            case FuzzType.Date:
                sb.Append("CAST(");
                WriteString(sb, value.Text!);
                sb.Append(" AS date)");
                break;
            case FuzzType.DateTime:
                sb.Append("CAST(");
                WriteString(sb, value.Text!);
                sb.Append(" AS datetime)");
                break;
            case FuzzType.Jsonb:
                sb.Append("CAST(");
                WriteString(sb, value.Text!);
                sb.Append(" AS jsonb)");
                break;
            case FuzzType.Interval:
                sb.Append("INTERVAL ");
                WriteString(sb, value.Text!);
                break;
        }
    }

    /// <summary>
    /// A literal in the form an IN-list item allows: CamusDB parses an item as a <c>simple_expr</c> (a
    /// name or a plain literal), so NULL is untyped, a NUMERIC is <c>NUMERIC '…'</c>, a DATE or a DATETIME
    /// is its string, which a comparison with a DATE or a DATETIME parses, and an INTERVAL is its
    /// <c>INTERVAL '…'</c> literal, which the grammar reads as a <c>simple_expr</c>.
    /// </summary>
    private void WriteSimpleLiteral(StringBuilder sb, FuzzValue value)
    {
        if (value.IsNull)
        {
            sb.Append("NULL");
            return;
        }
        switch (value.Type)
        {
            case FuzzType.Numeric:
                sb.Append("NUMERIC ");
                WriteString(sb, value.Text!);
                break;
            case FuzzType.Date:
            case FuzzType.DateTime:
                WriteString(sb, value.Text!);
                break;
            default:
                WriteLiteral(sb, value);
                break;
        }
    }

    private static void WriteFloat(StringBuilder sb, double value)
    {
        if (double.IsNaN(value))
        {
            sb.Append("CAST('NaN' AS float64)");
            return;
        }
        if (double.IsPositiveInfinity(value))
        {
            sb.Append("CAST('Infinity' AS float64)");
            return;
        }
        if (double.IsNegativeInfinity(value))
        {
            sb.Append("CAST('-Infinity' AS float64)");
            return;
        }

        string text = value.ToString("R", CultureInfo.InvariantCulture);
        if (text.Contains('E'))
            text = text.Replace("E+", "e").Replace("E-", "e-").Replace("E", "e");
        else if (!text.Contains('.'))
            text += ".0";
        // -0.0 prints as "-0" in R format: keep the sign and the decimal point.
        if (value == 0 && double.IsNegative(value))
            text = "-0.0";
        sb.Append(text);
    }

    private static void WriteString(StringBuilder sb, string text) =>
        sb.Append('\'').Append(text.Replace("'", "''")).Append('\'');
}
