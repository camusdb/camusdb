/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.SqlFuzz.Tests.Generator;

/// <summary>
/// Walks and rewrites statement trees. <see cref="Nodes"/> visits every expression and every nested
/// SELECT of a statement with an explicit stack, so a deep tree cannot overflow the call stack.
/// <see cref="Rewrite"/> builds a new expression with some nodes replaced; the generator caps
/// expression depth, so its recursion is bounded.
/// </summary>
internal static class ExprWalker
{
    /// <summary>Every expression node and every SELECT inside <paramref name="statement"/>, the statement's own SELECT included.</summary>
    public static IEnumerable<object> Nodes(Statement statement)
    {
        Stack<object> pending = new();
        foreach (object root in Roots(statement))
            pending.Push(root);

        while (pending.Count > 0)
        {
            object node = pending.Pop();
            yield return node;
            foreach (object child in Children(node))
                pending.Push(child);
        }
    }

    private static IEnumerable<object> Roots(Statement statement) => statement switch
    {
        SelectStmt select => [select],
        ExplainStmt explain => [explain.Query],
        InsertStmt insert => insert.Rows.SelectMany(r => r).Cast<object>()
            .Concat((insert.OnConflict?.DoUpdate ?? []).Select(a => (object)a.Value)),
        InsertSelectStmt insertSelect => [insertSelect.Source],
        UpdateStmt update => update.Set.Select(a => (object)a.Value).Append(update.Where),
        DeleteStmt delete => [delete.Where],
        _ => [],
    };

    /// <summary>The direct children of an expression or a SELECT: sub-expressions, nested SELECTs and the FROM items' ON predicates.</summary>
    public static IEnumerable<object> Children(object node)
    {
        switch (node)
        {
            case SelectStmt select:
                foreach (SelectItem item in select.Items)
                    yield return item.Expr;
                foreach (Expr on in JoinPredicates(select.From))
                    yield return on;
                if (select.Where is not null)
                    yield return select.Where;
                foreach (Expr g in select.GroupBy ?? [])
                    yield return g;
                if (select.Having is not null)
                    yield return select.Having;
                foreach (OrderItem o in select.OrderBy ?? [])
                    yield return o.Expr;
                if (select.SetOp is not null)
                    yield return select.SetOp.Right;
                break;
            case UnaryExpr u:
                yield return u.Operand;
                break;
            case BinaryExpr b:
                yield return b.Left;
                yield return b.Right;
                break;
            case CaseExpr c:
                foreach (CaseBranch branch in c.Branches)
                {
                    yield return branch.When;
                    yield return branch.Then;
                }
                if (c.Else is not null)
                    yield return c.Else;
                break;
            case FunctionExpr f:
                foreach (Expr a in f.Args)
                    yield return a;
                break;
            case InListExpr i:
                yield return i.Value;
                foreach (Expr item in i.Items)
                    yield return item;
                break;
            case BetweenExpr b:
                yield return b.Value;
                yield return b.Low;
                yield return b.High;
                break;
            case LikeExpr l:
                yield return l.Value;
                yield return l.Pattern;
                break;
            case CastExpr c:
                yield return c.Operand;
                break;
            case JsonFieldExpr j:
                yield return j.Document;
                break;
            case ExistsExpr e:
                yield return e.Subquery;
                break;
            case InSubqueryExpr s:
                yield return s.Value;
                yield return s.Subquery;
                break;
            case AggregateExpr a when a.Argument is not null:
                yield return a.Argument;
                break;
            case WindowExpr w:
                if (w.Argument is not null)
                    yield return w.Argument;
                foreach (Expr p in w.PartitionBy)
                    yield return p;
                foreach (OrderItem o in w.OrderBy)
                    yield return o.Expr;
                break;
        }
    }

    /// <summary>The ON predicates of the joins of a FROM clause.</summary>
    public static IEnumerable<Expr> JoinPredicates(FromItem from)
    {
        FromItem current = from;
        while (current is JoinFrom join)
        {
            if (join.On is not null)
                yield return join.On;
            current = join.Left;
        }
    }

    /// <summary>
    /// A copy of <paramref name="expression"/> where every node for which <paramref name="replace"/>
    /// returns an expression is replaced by it. A nested SELECT is kept as it is.
    /// </summary>
    public static Expr Rewrite(Expr expression, Func<Expr, Expr?> replace)
    {
        if (replace(expression) is { } replaced)
            return replaced;

        Expr R(Expr e) => Rewrite(e, replace);
        return expression switch
        {
            UnaryExpr u => u with { Operand = R(u.Operand) },
            BinaryExpr b => b with { Left = R(b.Left), Right = R(b.Right) },
            CaseExpr c => c with { Branches = c.Branches.Select(x => new CaseBranch(R(x.When), R(x.Then))).ToList(), Else = c.Else is null ? null : R(c.Else) },
            FunctionExpr f => f with { Args = f.Args.Select(R).ToList() },
            InListExpr i => i with { Value = R(i.Value), Items = i.Items.Select(R).ToList() },
            BetweenExpr b => b with { Value = R(b.Value), Low = R(b.Low), High = R(b.High) },
            LikeExpr l => l with { Value = R(l.Value), Pattern = R(l.Pattern) },
            CastExpr c => c with { Operand = R(c.Operand) },
            JsonFieldExpr j => j with { Document = R(j.Document) },
            InSubqueryExpr s => s with { Value = R(s.Value) },
            AggregateExpr a when a.Argument is not null => a with { Argument = R(a.Argument) },
            _ => expression,
        };
    }
}
