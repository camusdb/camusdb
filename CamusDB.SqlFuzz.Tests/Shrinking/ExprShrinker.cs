/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.SqlFuzz.Tests.Generator;

namespace CamusDB.SqlFuzz.Tests.Shrinking;

/// <summary>
/// Simpler forms of an expression, for the shrinker. Each candidate has the same type as the input
/// and fewer nodes: a descendant of the same type in place of the whole, TRUE or FALSE in place of a
/// predicate, or the input with one child replaced by a simpler form of that child.
///
/// <para>The walk recurses. That is safe because the generator caps expression depth
/// (<see cref="GeneratorProfile.MaxExpressionDepth"/>) and subqueries one level deep.</para>
/// </summary>
internal static class ExprShrinker
{
    public static IEnumerable<Expr> Candidates(Expr expression)
    {
        if (expression is LiteralExpr or ColumnExpr)
            yield break;

        // The cheapest reductions first: a literal predicate, then a same-type child.
        if (expression.Type == FuzzType.Bool)
        {
            yield return new LiteralExpr(FuzzValue.Of(true));
            yield return new LiteralExpr(FuzzValue.Of(false));
            yield return new LiteralExpr(FuzzValue.Null(FuzzType.Bool));
        }

        IReadOnlyList<Expr> children = Children(expression);
        foreach (Expr child in children)
        {
            if (child.Type == expression.Type)
                yield return child;
        }

        for (int i = 0; i < children.Count; i++)
        {
            foreach (Expr simpler in Candidates(children[i]))
                yield return WithChild(expression, i, simpler);
        }
    }

    /// <summary>The direct child expressions, in a fixed order that <see cref="WithChild"/> uses.</summary>
    public static IReadOnlyList<Expr> Children(Expr expression) => expression switch
    {
        UnaryExpr u => [u.Operand],
        BinaryExpr b => [b.Left, b.Right],
        CaseExpr c => c.Branches.SelectMany(br => new[] { br.When, br.Then }).Concat(c.Else is null ? [] : [c.Else]).ToList(),
        FunctionExpr f => f.Args,
        InListExpr i => new[] { i.Value }.Concat(i.Items).ToList(),
        BetweenExpr b => [b.Value, b.Low, b.High],
        LikeExpr l => [l.Value, l.Pattern],
        CastExpr c => [c.Operand],
        JsonFieldExpr j => [j.Document],
        InSubqueryExpr s => [s.Value],
        AggregateExpr a when a.Argument is not null => [a.Argument],
        _ => [],
    };

    public static Expr WithChild(Expr expression, int index, Expr child)
    {
        switch (expression)
        {
            case UnaryExpr u:
                return u with { Operand = child };
            case BinaryExpr b:
                return index == 0 ? b with { Left = child } : b with { Right = child };
            case CaseExpr c:
            {
                List<CaseBranch> branches = c.Branches.ToList();
                int branchSlots = branches.Count * 2;
                if (index < branchSlots)
                {
                    CaseBranch branch = branches[index / 2];
                    branches[index / 2] = index % 2 == 0 ? branch with { When = child } : branch with { Then = child };
                    return c with { Branches = branches };
                }
                return c with { Else = child };
            }
            case FunctionExpr f:
            {
                List<Expr> args = f.Args.ToList();
                args[index] = child;
                return f with { Args = args };
            }
            case InListExpr i:
            {
                if (index == 0)
                    return i with { Value = child };
                List<Expr> items = i.Items.ToList();
                items[index - 1] = child;
                return i with { Items = items };
            }
            case BetweenExpr b:
                return index switch { 0 => b with { Value = child }, 1 => b with { Low = child }, _ => b with { High = child } };
            case LikeExpr l:
                return index == 0 ? l with { Value = child } : l with { Pattern = child };
            case CastExpr c:
                return c with { Operand = child };
            case JsonFieldExpr j:
                return j with { Document = child };
            case InSubqueryExpr s:
                return s with { Value = child };
            case AggregateExpr a:
                return a with { Argument = child };
            default:
                return expression;
        }
    }

    /// <summary>The count of nodes, to tell a smaller expression from a larger one.</summary>
    public static int Size(Expr expression) => 1 + Children(expression).Sum(Size);
}
