/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

#if !CAMUSDB_BROWSER

using System.Globalization;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace CamusDB.Core.Extensions.Inline;

/// <summary>
/// Puts the budget checks into the body of an inline function, after the body passed the symbol
/// check. The result is compiled a second time, with the budget assembly as a reference.
///
/// <para><b>The rules.</b></para>
/// <list type="number">
/// <item><b>A tick at the start of every loop body, lambda and local function.</b> Each is a place
/// where the body can run without end: a loop by itself, a lambda or a local function by calling
/// itself. An expression-bodied lambda or local function becomes a block first.</item>
/// <item><b>Every <c>catch</c> gets a filter that lets the budget exceptions through.</b> A bare
/// <c>catch</c> becomes <c>catch (Exception e) when (!CallBudget.IsBudgetException(e))</c>, and an
/// existing filter is joined with <c>&amp;&amp;</c> after that test. Without the filter,
/// <c>while (true) { try { ... } catch { } }</c> would catch the exception of the tick and run on.</item>
/// <item><b>A tick at the start of every <c>catch</c> block.</b> The runtime runs a <c>catch</c>
/// block on top of the stack of the frame that threw, and gives that stack back only when the block
/// ends. A recursion that catches and throws again at every level therefore goes deeper with each
/// level on its way out, with no loop and no call in between. The tick checks the stack there.</item>
/// <item><b>A <c>finally</c> block does nothing after a budget stopped the call.</b>
/// <c>finally { B }</c> becomes <c>finally { if (!CallBudget.Stopped) { Tick(); B } }</c>. A
/// <c>finally</c> block runs on that same deep stack. A tick inside it would throw again, one more
/// exception below the last for every frame of a recursion, until the stack overflows. The tick at
/// its start is for a block that runs for another reason and throws by itself.</item>
/// <item><b>What the compiler concluded from a <c>finally</c> block stays true.</b> A block that can
/// be skipped no longer assigns for certain and no longer ends for certain, and the compiler reads
/// both from it. So, in a body that has a <c>finally</c> block: every local that is declared with no
/// value gets <c>= default!</c>; the path that skips a block assigns the <c>out</c> parameters the
/// block names (<see cref="InlineFunctionSymbolReport.FinallyOutParameters"/>); and a <c>try</c>
/// statement whose <c>finally</c> block cannot end is followed by <c>throw CallBudget.StopCause();</c>
/// (<see cref="InlineFunctionSymbolReport.EndlessFinallyBlocks"/>). None of the three changes what a
/// body computes: the first compile proved that no local is read before the body assigns it, the
/// second runs only in a call that already failed, and the third is reached only after a skipped
/// block. A local that is declared in one <c>switch</c> section and that only a <c>finally</c> block
/// of another section assigns is not covered: such a body fails its second compile.</item>
/// <item><b><c>new T[n]</c> becomes <c>CallBudget.NewArray&lt;T&gt;(n)</c></b>, which refuses a size
/// above the limit before it allocates.</item>
/// <item><b>A match timeout goes through <c>CallBudget.RegexTimeout</c></b>, which limits it to the
/// time the call has left.</item>
/// </list>
///
/// <para><b>It rewrites the tree the symbol check read.</b> The report names lambdas and timeout
/// arguments by their span in that tree, and each <c>Visit</c> method receives the original node, so
/// the lookup is exact. The new syntax nodes are never printed and parsed again: the compiler takes
/// the tree as it is, so their layout does not matter.</para>
/// </summary>
internal sealed class InlineFunctionBudgetRewriter : CSharpSyntaxRewriter
{
    private const string Budget = "global::CamusDB.Extensions.Inline.CallBudget";

    /// <summary>The field of the trigger wrapper that holds the budget object of the fire; the body cannot name it.</summary>
    public const string TriggerFrame = InlineFunctionSyntaxChecker.ReservedPrefix + "_frame";

    private readonly InlineFunctionSymbolReport report;

    /// <summary>What the checks are called on: the static helper for a function, the frame field for a trigger.</summary>
    private readonly string checks;

    private readonly StatementSyntax tick;

    /// <summary>True after a budget check of the call failed: <c>CallBudget.Stopped</c>, or the same on the frame field.</summary>
    private readonly ExpressionSyntax stopped;

    /// <summary><c>throw CallBudget.StopCause();</c>, or the same on the frame field.</summary>
    private readonly StatementSyntax throwStopCause;

    /// <summary>True when the body has a <c>finally</c> block, which is when its locals need a value at their declaration.</summary>
    private readonly bool initializeLocals;

    private int caught;

    private InlineFunctionBudgetRewriter(InlineFunctionSymbolReport report, InlineUnitKind kind, bool initializeLocals)
    {
        this.report = report;
        this.initializeLocals = initializeLocals;
        checks = kind == InlineUnitKind.Trigger ? TriggerFrame : Budget;
        tick = SyntaxFactory.ParseStatement(checks + ".Tick();");
        stopped = SyntaxFactory.ParseExpression(checks + ".Stopped");
        throwStopCause = SyntaxFactory.ParseStatement("throw " + checks + ".StopCause();");
    }

    /// <summary>Returns <paramref name="body"/> with the budget checks. <paramref name="body"/> must be the block the report was made from.</summary>
    public static BlockSyntax Rewrite(BlockSyntax body, InlineFunctionSymbolReport report) =>
        Rewrite(body, report, InlineUnitKind.Function);

    public static BlockSyntax Rewrite(BlockSyntax body, InlineFunctionSymbolReport report, InlineUnitKind kind)
    {
        bool hasFinally = false;

        foreach (SyntaxNode node in body.DescendantNodes())
        {
            if (node is FinallyClauseSyntax)
            {
                hasFinally = true;
                break;
            }
        }

        return (BlockSyntax)new InlineFunctionBudgetRewriter(report, kind, hasFinally).Visit(body)!;
    }

    // ─── Loops ───────────────────────────────────────────────────────────────────────────────────

    public override SyntaxNode? VisitWhileStatement(WhileStatementSyntax node)
    {
        WhileStatementSyntax visited = (WhileStatementSyntax)base.VisitWhileStatement(node)!;
        return visited.WithStatement(WithTick(visited.Statement));
    }

    public override SyntaxNode? VisitDoStatement(DoStatementSyntax node)
    {
        DoStatementSyntax visited = (DoStatementSyntax)base.VisitDoStatement(node)!;
        return visited.WithStatement(WithTick(visited.Statement));
    }

    public override SyntaxNode? VisitForStatement(ForStatementSyntax node)
    {
        ForStatementSyntax visited = (ForStatementSyntax)base.VisitForStatement(node)!;
        return visited.WithStatement(WithTick(visited.Statement));
    }

    public override SyntaxNode? VisitForEachStatement(ForEachStatementSyntax node)
    {
        ForEachStatementSyntax visited = (ForEachStatementSyntax)base.VisitForEachStatement(node)!;
        return visited.WithStatement(WithTick(visited.Statement));
    }

    public override SyntaxNode? VisitForEachVariableStatement(ForEachVariableStatementSyntax node)
    {
        ForEachVariableStatementSyntax visited = (ForEachVariableStatementSyntax)base.VisitForEachVariableStatement(node)!;
        return visited.WithStatement(WithTick(visited.Statement));
    }

    // ─── Lambdas and local functions ─────────────────────────────────────────────────────────────

    public override SyntaxNode? VisitSimpleLambdaExpression(SimpleLambdaExpressionSyntax node)
    {
        bool returnsVoid = report.VoidLambdas.Contains(node.Span);
        SimpleLambdaExpressionSyntax visited = (SimpleLambdaExpressionSyntax)base.VisitSimpleLambdaExpression(node)!;
        return visited.WithBody(TickedBody(visited.Block, visited.ExpressionBody, returnsVoid));
    }

    public override SyntaxNode? VisitParenthesizedLambdaExpression(ParenthesizedLambdaExpressionSyntax node)
    {
        bool returnsVoid = report.VoidLambdas.Contains(node.Span);
        ParenthesizedLambdaExpressionSyntax visited = (ParenthesizedLambdaExpressionSyntax)base.VisitParenthesizedLambdaExpression(node)!;
        return visited.WithBody(TickedBody(visited.Block, visited.ExpressionBody, returnsVoid));
    }

    public override SyntaxNode? VisitAnonymousMethodExpression(AnonymousMethodExpressionSyntax node)
    {
        AnonymousMethodExpressionSyntax visited = (AnonymousMethodExpressionSyntax)base.VisitAnonymousMethodExpression(node)!;
        return visited.WithBlock(WithTick(visited.Block));
    }

    public override SyntaxNode? VisitLocalFunctionStatement(LocalFunctionStatementSyntax node)
    {
        LocalFunctionStatementSyntax visited = (LocalFunctionStatementSyntax)base.VisitLocalFunctionStatement(node)!;

        if (visited.Body is { } block)
            return visited.WithBody(WithTick(block));

        if (visited.ExpressionBody is not { } arrow)
            return visited;

        bool returnsVoid = visited.ReturnType is PredefinedTypeSyntax predefined && predefined.Keyword.IsKind(SyntaxKind.VoidKeyword);

        return visited
            .WithExpressionBody(null)
            .WithSemicolonToken(default)
            .WithBody(TickedBody(null, arrow.Expression, returnsVoid));
    }

    // ─── Catch and finally clauses ───────────────────────────────────────────────────────────────

    public override SyntaxNode? VisitCatchClause(CatchClauseSyntax node)
    {
        CatchClauseSyntax visited = (CatchClauseSyntax)base.VisitCatchClause(node)!;

        // A name of its own for each clause: a catch block can hold another try.
        string added = InlineFunctionSyntaxChecker.ReservedPrefix + "_e" + (caught++).ToString(CultureInfo.InvariantCulture);
        CatchDeclarationSyntax declaration;
        string name;

        if (visited.Declaration is null)
        {
            name = added;
            declaration = SyntaxFactory.CatchDeclaration(SyntaxFactory.ParseTypeName("global::System.Exception"), SyntaxFactory.Identifier(name));
        }
        else if (visited.Declaration.Identifier.IsKind(SyntaxKind.None))
        {
            name = added;
            declaration = visited.Declaration.WithIdentifier(SyntaxFactory.Identifier(name));
        }
        else
        {
            name = visited.Declaration.Identifier.Text;
            declaration = visited.Declaration;
        }

        ExpressionSyntax test = SyntaxFactory.ParseExpression("!" + Budget + ".IsBudgetException(" + name + ")");

        // The budget test first: the body's own filter does not run for a budget exception.
        if (visited.Filter is { } filter)
            test = SyntaxFactory.BinaryExpression(
                SyntaxKind.LogicalAndExpression, test, SyntaxFactory.ParenthesizedExpression(filter.FilterExpression));

        return visited
            .WithDeclaration(declaration)
            .WithFilter(SyntaxFactory.CatchFilterClause(test))
            .WithBlock(WithTick(visited.Block));
    }

    public override SyntaxNode? VisitFinallyClause(FinallyClauseSyntax node)
    {
        FinallyClauseSyntax visited = (FinallyClauseSyntax)base.VisitFinallyClause(node)!;
        BlockSyntax ticked = WithTick(visited.Block);
        StatementSyntax guarded;

        if (report.FinallyOutParameters.TryGetValue(node.Block.Span, out List<string>? named))
        {
            // if (Stopped) { a = default!; b = default!; } else { Tick(); ... }
            List<StatementSyntax> defaults = new(named.Count);

            foreach (string name in named)
                defaults.Add(DefaultAssignment(name));

            guarded = SyntaxFactory.IfStatement(stopped, SyntaxFactory.Block(defaults), SyntaxFactory.ElseClause(ticked));
        }
        else
        {
            // if (!Stopped) { Tick(); ... }
            guarded = SyntaxFactory.IfStatement(SyntaxFactory.PrefixUnaryExpression(SyntaxKind.LogicalNotExpression, stopped), ticked);
        }

        return visited.WithBlock(SyntaxFactory.Block(guarded));
    }

    public override SyntaxNode? VisitTryStatement(TryStatementSyntax node)
    {
        TryStatementSyntax visited = (TryStatementSyntax)base.VisitTryStatement(node)!;

        if (node.Finally is not { } clause || !report.EndlessFinallyBlocks.Contains(clause.Block.Span))
            return visited;

        // The finally block never ended, so the code after this statement could not run. Skipped, the
        // block ends, and the only way to get here is a stop whose exception was lost on the way.
        return SyntaxFactory.Block(visited, throwStopCause);
    }

    public override SyntaxNode? VisitVariableDeclarator(VariableDeclaratorSyntax node)
    {
        VariableDeclaratorSyntax visited = (VariableDeclaratorSyntax)base.VisitVariableDeclarator(node)!;

        if (!initializeLocals || visited.Initializer is not null || visited.ArgumentList is not null)
            return visited;

        return visited.WithInitializer(SyntaxFactory.EqualsValueClause(DefaultValue()));
    }

    /// <summary><c>default!</c></summary>
    private static ExpressionSyntax DefaultValue() =>
        SyntaxFactory.PostfixUnaryExpression(
            SyntaxKind.SuppressNullableWarningExpression,
            SyntaxFactory.LiteralExpression(SyntaxKind.DefaultLiteralExpression));

    /// <summary>
    /// <c>name = default!;</c>. The identifier is built as a token, so a name that is also a keyword
    /// (<c>@class</c> in the source) needs no escape.
    /// </summary>
    private static StatementSyntax DefaultAssignment(string name) =>
        SyntaxFactory.ExpressionStatement(
            SyntaxFactory.AssignmentExpression(
                SyntaxKind.SimpleAssignmentExpression,
                SyntaxFactory.IdentifierName(SyntaxFactory.Identifier(name)),
                DefaultValue()));

    // ─── Arrays and match timeouts ───────────────────────────────────────────────────────────────

    public override SyntaxNode? VisitArrayCreationExpression(ArrayCreationExpressionSyntax node)
    {
        ArrayCreationExpressionSyntax visited = (ArrayCreationExpressionSyntax)base.VisitArrayCreationExpression(node)!;
        SyntaxList<ArrayRankSpecifierSyntax> ranks = visited.Type.RankSpecifiers;

        // "new T[n]" and "new T[n][]". An initializer fixes the length in the source, and an array with
        // more dimensions has constant sizes, which the symbol check verified.
        if (visited.Initializer is not null || ranks.Count == 0 || ranks[0].Sizes.Count != 1 ||
            ranks[0].Sizes[0] is OmittedArraySizeExpressionSyntax)
            return visited;

        TypeSyntax element = ranks.Count == 1
            ? visited.Type.ElementType
            : SyntaxFactory.ArrayType(visited.Type.ElementType, SyntaxFactory.List(ranks.Skip(1)));

        return SyntaxFactory.InvocationExpression(
                SyntaxFactory.MemberAccessExpression(
                    SyntaxKind.SimpleMemberAccessExpression,
                    SyntaxFactory.ParseName(checks),
                    SyntaxFactory.GenericName(
                        SyntaxFactory.Identifier("NewArray"),
                        SyntaxFactory.TypeArgumentList(SyntaxFactory.SingletonSeparatedList(element.WithoutTrivia())))),
                SyntaxFactory.ArgumentList(SyntaxFactory.SingletonSeparatedList(SyntaxFactory.Argument(ranks[0].Sizes[0].WithoutTrivia()))))
            .WithTriviaFrom(visited);
    }

    public override SyntaxNode? VisitArgument(ArgumentSyntax node)
    {
        bool timeout = report.MatchTimeoutArguments.Contains(node.Expression.Span);
        ArgumentSyntax visited = (ArgumentSyntax)base.VisitArgument(node)!;

        if (!timeout)
            return visited;

        return visited.WithExpression(
            SyntaxFactory.InvocationExpression(
                SyntaxFactory.ParseExpression(checks + ".RegexTimeout"),

                SyntaxFactory.ArgumentList(SyntaxFactory.SingletonSeparatedList(SyntaxFactory.Argument(visited.Expression.WithoutTrivia())))));
    }

    // ─── Parts ───────────────────────────────────────────────────────────────────────────────────

    private BlockSyntax WithTick(StatementSyntax statement) =>
        statement is BlockSyntax block
            ? block.WithStatements(block.Statements.Insert(0, tick))
            : SyntaxFactory.Block(tick, statement);

    /// <summary>
    /// The block of a lambda or a local function with a tick first. An expression body becomes
    /// <c>{ Tick(); return expression; }</c>, or <c>{ Tick(); expression; }</c> when there is no
    /// value to return. A <c>throw</c> expression becomes a <c>throw</c> statement in both cases.
    /// </summary>
    private BlockSyntax TickedBody(BlockSyntax? block, ExpressionSyntax? expression, bool returnsVoid)
    {
        if (block is not null)
            return WithTick(block);

        ExpressionSyntax value = expression!.WithoutTrivia();

        StatementSyntax statement = value switch
        {
            ThrowExpressionSyntax thrown => SyntaxFactory.ThrowStatement(thrown.Expression),
            _ when returnsVoid => SyntaxFactory.ExpressionStatement(value),
            _ => SyntaxFactory.ReturnStatement(value),
        };

        return SyntaxFactory.Block(tick, statement.NormalizeWhitespace());

    }
}

#endif
