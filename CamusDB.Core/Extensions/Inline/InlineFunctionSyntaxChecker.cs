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
using Microsoft.CodeAnalysis.Text;

namespace CamusDB.Core.Extensions.Inline;

/// <summary>One problem in the body of an inline function, at a position in the body's own text.</summary>
/// <param name="Line">The 1-based line of the body.</param>
/// <param name="Column">The 1-based column.</param>
/// <param name="Message">What is refused, and where it helps, what to write instead.</param>
internal readonly record struct InlineFunctionFinding(int Line, int Column, string Message)
{
    public override string ToString() => $"({Line},{Column}): {Message}";
}

/// <summary>
/// The first check of the body of an inline function: it parses the text as one block of statements
/// and refuses the language forms that the later stages cannot make safe. It runs before any semantic
/// work, so it also bounds what the compiler is asked to do.
///
/// <para><b>The body cannot leave its block.</b> The text is parsed alone, as <c>{ body }</c>, and
/// must be exactly one complete block: text that closes the block early and goes on (a second method,
/// an attribute, a type) leaves tokens after the block and is refused. The server then puts the parsed
/// block, as a syntax node, into the method it writes itself. The text is never pasted into the
/// method's source, so no text can change the shape of the code around it.</para>
///
/// <para><b>Why each form is refused.</b></para>
/// <list type="bullet">
/// <item>Attributes and <c>extern</c>: a local function can carry <c>DllImport</c>, which is a native
/// call inside a body.</item>
/// <item><c>unsafe</c>, <c>fixed</c>, pointers and <c>stackalloc</c>: raw memory. <c>stackalloc</c> is
/// legal in safe code with a size the body computes, which is a way to overflow the stack.</item>
/// <item><c>await</c>, <c>async</c>, <c>yield</c>, <c>lock</c> and <c>using</c>: the budget checks
/// assume that the body runs on one thread from start to end, and nothing on the allowed list needs
/// disposal.</item>
/// <item><c>goto</c> and labels: a jump makes a loop that has no loop statement, so no budget check
/// would run in it.</item>
/// <item><c>typeof</c>, <c>sizeof</c> and the <c>__makeref</c> family: the door to reflection.</item>
/// <item>Query expressions: the compiler turns their clauses into lambdas after the stage that
/// inserts the budget checks, so those lambdas would have none. Method syntax is accepted.</item>
/// <item>Generic local functions: a generic function that calls itself with a larger type argument
/// makes the runtime build types without limit.</item>
/// <item>Anonymous types, <c>with</c>, ranges and indexes from the end: each makes the compiler
/// call helpers that are not on the allowed list.</item>
/// <item>Preprocessor directives: <c>#r</c> and <c>#load</c> bring in other code, and the others hide
/// text from the checks.</item>
/// </list>
///
/// <para><b>Nesting depth.</b> The depth of the syntax tree is measured with an explicit stack before
/// any recursive walk, and a tree deeper than <see cref="MaxDepth"/> is refused. Every later stage,
/// and the compiler itself, walks the tree by recursion.</para>
/// </summary>
internal static class InlineFunctionSyntaxChecker
{
    /// <summary>The deepest syntax nesting a body may have.</summary>
    public const int MaxDepth = 128;

    /// <summary>The largest alignment an interpolation hole may ask for, in characters.</summary>
    public const int MaxAlignment = 1024;

    /// <summary>The longest format text of an interpolation hole or a <c>ToString</c> call.</summary>
    public const int MaxFormatLength = 64;

    /// <summary>The prefix of the names the server's rewrite adds; the body may not use it.</summary>
    public const string ReservedPrefix = "__camus";

    /// <summary>
    /// The most work the lambdas of a body may give the compiler, in units of one syntax node bound
    /// one time. A lambda is an argument of a call, and to choose between the overloads of the called
    /// method the compiler binds the lambda's body once for each candidate. A lambda inside it is then
    /// bound once for each of its own candidates, for each of those times:
    /// <c>xs.Sum(a =&gt; xs.Sum(b =&gt; xs.Sum(c =&gt; ...)))</c> costs about seven times more with each
    /// level, the compiler does not look at the cancellation token while it does this, and ten levels
    /// hold a compile thread for hours. At this limit the worst body compiles in about a second.
    /// </summary>
    public const long MaxLambdaCost = 500_000;

    /// <summary>The candidates that the compiler can bind one lambda for: the selector overloads of <c>Sum</c> and <c>Max</c>, measured.</summary>
    private const int LambdaFanOut = 8;

    /// <summary>The cost of the call that takes a lambda, in nodes: its overload resolution.</summary>
    private const int LambdaCallCost = 500;

    /// <summary>
    /// Parses <paramref name="body"/> as one block. Returns the block, or null with the reasons in
    /// <paramref name="errors"/> when the text is not exactly one complete block.
    ///
    /// <para>A trigger body (<paramref name="async"/>) is parsed as the body of an async method. The
    /// parser reads <c>await</c> as a keyword only there: outside an async method, <c>await t;</c>
    /// is a declaration of a local <c>t</c> of a type named <c>await</c>, and the body would not
    /// compile.</para>
    /// </summary>
    public static BlockSyntax? Parse(string body, CSharpParseOptions options, List<InlineFunctionFinding> errors, bool async = false)
    {
        // The line breaks keep a "//" comment on the last line of the body from taking the closing
        // brace, and make line 1 of the body line 1 of the text after the opening brace. The method
        // head of the async form is on the line of the brace, so the lines are the same in both forms.
        string head = async ? "async System.Threading.Tasks.Task " + ReservedPrefix + "_body() " : "";
        string text = head + "{\n" + body + "\n}";

        SyntaxNode? parsed = async
            ? SyntaxFactory.ParseMemberDeclaration(text, offset: 0, options, consumeFullText: true)
            : SyntaxFactory.ParseStatement(text, offset: 0, options, consumeFullText: true);

        if (parsed is null)
        {
            errors.Add(new InlineFunctionFinding(1, 1, "the body is not one complete block of statements; check for a brace, a string or a comment that does not close"));
            return null;
        }

        foreach (Diagnostic diagnostic in parsed.GetDiagnostics())
        {
            if (diagnostic.Severity == DiagnosticSeverity.Error)
                errors.Add(At(diagnostic.Location, parsed.SyntaxTree, diagnostic.GetMessage(CultureInfo.InvariantCulture)));
        }

        BlockSyntax? found = parsed switch
        {
            BlockSyntax plain when !async => plain,
            MethodDeclarationSyntax method when async => method.Body,
            _ => null,
        };

        if (found is not { } block || block.CloseBraceToken.IsMissing ||
            block.CloseBraceToken.SpanStart != text.Length - 1 || block.FullSpan.Length != text.Length - head.Length ||
            parsed.FullSpan.Length != text.Length)
        {
            if (errors.Count == 0)
                errors.Add(new InlineFunctionFinding(1, 1, "the body is not one complete block of statements; check for a brace, a string or a comment that does not close"));

            return null;
        }

        return errors.Count == 0 ? block : null;
    }

    /// <summary>True when <paramref name="node"/> is inside a lambda, an anonymous method or a local function of <paramref name="block"/>.</summary>
    private static bool IsInsideFunction(SyntaxNode node, BlockSyntax block)
    {
        for (SyntaxNode? current = node.Parent; current is not null && current != block; current = current.Parent)
        {
            if (current is AnonymousFunctionExpressionSyntax or LocalFunctionStatementSyntax)
                return true;
        }

        return false;
    }

    /// <summary>
    /// The nesting depth of <paramref name="root"/>, counted with an explicit stack. Stops and

    /// returns a value above <paramref name="limit"/> as soon as the depth passes it.
    /// </summary>
    public static int MeasureDepth(SyntaxNode root, int limit)
    {
        int deepest = 0;
        Stack<(SyntaxNode Node, int Depth)> pending = new();
        pending.Push((root, 1));

        while (pending.Count > 0)
        {
            (SyntaxNode node, int depth) = pending.Pop();

            if (depth > deepest)
                deepest = depth;

            if (deepest > limit)
                return deepest;

            foreach (SyntaxNode child in node.ChildNodes())
                pending.Push((child, depth + 1));
        }

        return deepest;
    }

    /// <summary>
    /// Returns a finding when the lambdas of <paramref name="block"/> cost the compiler more than
    /// <see cref="MaxLambdaCost"/>. A node inside <c>d</c> lambdas counts <see cref="LambdaFanOut"/>
    /// to the power <c>d</c>, and a lambda counts <see cref="LambdaCallCost"/> times the count of the
    /// place it is in. The limit allows hundreds of lambdas side by side and four inside one another;
    /// a local function is bound one time wherever it is called from, so it is the way out.
    /// </summary>
    public static InlineFunctionFinding? CheckLambdaCost(BlockSyntax block)
    {
        long cost = 0;
        Stack<(SyntaxNode Node, int Lambdas)> pending = new();
        pending.Push((block, 0));

        while (pending.Count > 0)
        {
            (SyntaxNode node, int lambdas) = pending.Pop();
            long weight = Weight(lambdas);

            if (node is AnonymousFunctionExpressionSyntax)
            {
                cost += weight * LambdaCallCost;
                lambdas++;
            }
            else if (lambdas > 0)
                cost += weight;

            if (cost > MaxLambdaCost)
            {
                return At(node.GetLocation(), block.SyntaxTree,
                    "the lambdas are nested too deeply, or there are too many of them, for the compiler to bind in a bounded time; " +
                    "move the body of an inner lambda to a local function, or use a loop");
            }

            foreach (SyntaxNode child in node.ChildNodes())
                pending.Push((child, lambdas));
        }

        return null;

        // Beyond eight levels one node alone is over the limit.
        static long Weight(int lambdas) => lambdas > 8 ? MaxLambdaCost + 1 : (long)Math.Pow(LambdaFanOut, lambdas);
    }

    /// <summary>
    /// Returns every refused form in <paramref name="block"/>. The caller has checked the depth, so
    /// the walk can recurse.
    /// </summary>

    public static List<InlineFunctionFinding> Check(BlockSyntax block) => Check(block, InlineUnitKind.Function);

    /// <summary>
    /// The refused forms for a body of <paramref name="kind"/>. A trigger body may <c>await</c> in its
    /// own statements, where the server's asynchronous wrapper carries the budget object; not inside a
    /// lambda or a local function, which would need an <c>async</c> of its own.
    /// </summary>
    public static List<InlineFunctionFinding> Check(BlockSyntax block, InlineUnitKind kind)
    {
        List<InlineFunctionFinding> findings = [];
        SyntaxTree tree = block.SyntaxTree;

        void Refuse(SyntaxNodeOrToken where, string message) => findings.Add(At(where.GetLocation()!, tree, message));

        if (block.ContainsDirectives)
        {
            foreach (SyntaxTrivia trivia in block.DescendantTrivia(descendIntoTrivia: true))
            {
                if (trivia.IsDirective)
                    findings.Add(At(trivia.GetLocation(), tree, "preprocessor directives are not allowed"));
            }
        }

        foreach (SyntaxToken token in block.DescendantTokens())
        {
            if (token.IsKind(SyntaxKind.IdentifierToken) && token.ValueText.StartsWith(ReservedPrefix, StringComparison.Ordinal))
                Refuse(token, $"the name '{token.ValueText}' is reserved: a name cannot start with '{ReservedPrefix}'");
        }

        foreach (SyntaxNode node in block.DescendantNodes())
        {
            switch (node)
            {
                case AttributeListSyntax:
                    Refuse(node, "attributes are not allowed");
                    break;

                case UnsafeStatementSyntax or FixedStatementSyntax or PointerTypeSyntax or FunctionPointerTypeSyntax:
                    Refuse(node, "unsafe code and pointers are not allowed");
                    break;

                case StackAllocArrayCreationExpressionSyntax or ImplicitStackAllocArrayCreationExpressionSyntax:
                    Refuse(node, "stackalloc is not allowed; use an array");
                    break;

                case AwaitExpressionSyntax when kind != InlineUnitKind.Trigger:
                    Refuse(node, "await is not allowed: a body runs on one thread from start to end");
                    break;

                case AwaitExpressionSyntax when IsInsideFunction(node, block):
                    Refuse(node, "await is allowed in the statements of the trigger body only, not inside a lambda or a local function");
                    break;

                case YieldStatementSyntax:
                    Refuse(node, "yield is not allowed; build a List and return it");
                    break;

                case GotoStatementSyntax or LabeledStatementSyntax:
                    Refuse(node, "goto and labels are not allowed; use a loop");
                    break;

                case LockStatementSyntax:
                    Refuse(node, "lock is not allowed: a body runs on one thread");
                    break;

                case UsingStatementSyntax:
                    Refuse(node, "using is not allowed: nothing a body can create needs disposal");
                    break;

                case TypeOfExpressionSyntax:
                    Refuse(node, "typeof is not allowed");
                    break;

                case SizeOfExpressionSyntax:
                    Refuse(node, "sizeof is not allowed");
                    break;

                case MakeRefExpressionSyntax or RefTypeExpressionSyntax or RefValueExpressionSyntax:
                    Refuse(node, "__makeref, __reftype and __refvalue are not allowed");
                    break;

                case QueryExpressionSyntax:
                    Refuse(node, "query expressions (from ... select) are not allowed; use the method syntax, as in items.Where(...).Select(...)");
                    break;

                case AnonymousObjectCreationExpressionSyntax:
                    Refuse(node, "anonymous types are not allowed; use a tuple");
                    break;

                case WithExpressionSyntax:
                    Refuse(node, "with expressions are not allowed");
                    break;

                case RangeExpressionSyntax:
                    Refuse(node, "ranges (a..b) are not allowed; use Substring, Skip and Take");
                    break;

                case PrefixUnaryExpressionSyntax prefix when prefix.IsKind(SyntaxKind.IndexExpression):
                    Refuse(node, "an index from the end (^n) is not allowed; use Length - n");
                    break;

                case ForEachStatementSyntax { AwaitKeyword.RawKind: not 0 } or ForEachVariableStatementSyntax { AwaitKeyword.RawKind: not 0 }:
                    Refuse(node, "await foreach is not allowed");
                    break;

                case LocalDeclarationStatementSyntax declaration:
                    if (!declaration.UsingKeyword.IsKind(SyntaxKind.None) || !declaration.AwaitKeyword.IsKind(SyntaxKind.None))
                        Refuse(node, "using declarations are not allowed: nothing a body can create needs disposal");
                    break;

                case LocalFunctionStatementSyntax function:
                    CheckModifiers(function.Modifiers, Refuse);

                    if (function.TypeParameterList is not null)
                        Refuse(function.TypeParameterList, "a generic local function is not allowed; write one function for each type");

                    if (function.Body is null && function.ExpressionBody is null)
                        Refuse(node, "a local function needs a body");

                    break;

                case AnonymousFunctionExpressionSyntax lambda:
                    CheckModifiers(lambda.Modifiers, Refuse);
                    break;

                case InterpolationAlignmentClauseSyntax alignment:
                    CheckAlignment(alignment, Refuse);
                    break;

                case InterpolationFormatClauseSyntax format:
                    if (format.FormatStringToken.ValueText.Length > MaxFormatLength)
                        Refuse(node, $"a format of more than {MaxFormatLength} characters is not allowed");
                    break;

                case ArgumentSyntax { Expression: var expression } when IsArgList(expression):
                case ExpressionStatementSyntax { Expression: var expression2 } when IsArgList(expression2):
                    Refuse(node, "__arglist is not allowed");
                    break;
            }
        }

        return findings;
    }

    private static bool IsArgList(ExpressionSyntax expression) =>
        expression.IsKind(SyntaxKind.ArgListExpression) ||
        expression is InvocationExpressionSyntax { Expression: var callee } && callee.IsKind(SyntaxKind.ArgListExpression);

    private static void CheckModifiers(SyntaxTokenList modifiers, Action<SyntaxNodeOrToken, string> refuse)
    {
        foreach (SyntaxToken modifier in modifiers)
        {
            if (modifier.IsKind(SyntaxKind.ExternKeyword))
                refuse(modifier, "extern is not allowed: a body cannot call native code");
            else if (modifier.IsKind(SyntaxKind.UnsafeKeyword))
                refuse(modifier, "unsafe code and pointers are not allowed");
            else if (modifier.IsKind(SyntaxKind.AsyncKeyword))
                refuse(modifier, "async is not allowed: a body runs on one thread from start to end");
        }
    }

    /// <summary>
    /// An alignment is a constant, so the limit is a syntax rule: the hole pads its text to that many
    /// characters, and <c>{x,2000000000}</c> would be a 4 GB string from a 16-character body.
    /// </summary>
    private static void CheckAlignment(InterpolationAlignmentClauseSyntax alignment, Action<SyntaxNodeOrToken, string> refuse)
    {
        ExpressionSyntax value = alignment.Value;

        while (value is ParenthesizedExpressionSyntax parenthesized)
            value = parenthesized.Expression;

        if (value is PrefixUnaryExpressionSyntax sign &&
            (sign.IsKind(SyntaxKind.UnaryMinusExpression) || sign.IsKind(SyntaxKind.UnaryPlusExpression)))
            value = sign.Operand;

        if (value is LiteralExpressionSyntax { Token.Value: int width } && width <= MaxAlignment)
            return;

        refuse(alignment, $"an interpolation alignment must be a number from -{MaxAlignment} to {MaxAlignment}");
    }

    /// <summary>
    /// Converts a location of the parsed text into a position of the body: the text has one line, the
    /// opening brace, before line 1 of the body.
    /// </summary>
    private static InlineFunctionFinding At(Location location, SyntaxTree tree, string message)
    {
        LinePosition start = tree.GetLineSpan(location.SourceSpan).StartLinePosition;
        return new InlineFunctionFinding(Math.Max(1, start.Line), start.Character + 1, message);
    }
}

#endif
