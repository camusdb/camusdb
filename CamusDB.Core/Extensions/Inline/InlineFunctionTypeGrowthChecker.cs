/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

#if !CAMUSDB_BROWSER

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace CamusDB.Core.Extensions.Inline;

/// <summary>
/// Bounds the size of the types that the compiler must infer for a body, before the compiler sees
/// the body. It reads syntax only.
///
/// <para><b>Why it exists.</b> The type of an inferred name can double with each line:
/// <code>
/// var t1 = (n, n);
/// var t2 = (t1, t1);     // 4 elements
/// var t3 = (t2, t2);     // 8 elements; 30 lines make a type with a billion elements
/// </code>
/// or with each call of a chain: <c>xs.Select(a =&gt; (a, a)).Select(b =&gt; (b, b))</c>. The compiler
/// walks such a type element by element, in loops that do not look at the cancellation token, so a
/// body of thirty short lines holds a compile thread for hours and the compile timeout cannot end
/// it. Nothing that runs after the first compile can protect the first compile, so this check runs
/// before it.</para>
///
/// <para><b>How it bounds.</b> The size of a type is the number of type names in it:
/// <c>Dictionary&lt;string, List&lt;long&gt;&gt;</c> is 4. Every expression gets an upper bound for the
/// size of its type, computed from its parts: a tuple is the sum of its elements, a call is at most
/// the sum of its receiver, its arguments and the results of its lambdas plus
/// <see cref="MemberOverhead"/>, a member is at most its receiver plus <see cref="MemberOverhead"/>.
/// A name with a written type has the size of what was written, whatever it is assigned. A name with
/// an inferred type (<c>var</c>, a lambda parameter with no type, <c>out var</c>, a pattern variable)
/// has the bound of the expression it comes from. One bound over <see cref="MaxTypeSize"/>, or all
/// bounds together over <see cref="MaxTotalSize"/>, refuses the body.</para>
///
/// <para><b>Why the sum is an upper bound.</b> The body cannot declare a type or a generic function,
/// so every type it can make is a framework type built by a framework member. In the assemblies the
/// compiler gets, a member's result names each type parameter at most once and adds at most nine
/// names of its own; a parameter of a delegate, an <c>out</c> parameter and a type argument of an
/// implemented interface obey the same rule with a smaller constant. The only exceptions are three
/// numeric members whose type parameter can only be a number. A test measures this on the running
/// framework, so a framework that breaks the rule fails the test and not the node.</para>
///
/// <para><b>The bound is loose on purpose, and so are the limits.</b> The bound counts some calls as
/// larger than they are (<c>OrderBy(x =&gt; x.Key)</c> returns the type of its receiver, whatever the
/// key is), so the limits sit far above what real code reaches, and still far below what makes the
/// compiler slow. This check only keeps the first compile short. The exact limits on the types of a
/// body are those of <see cref="InlineFunctionSymbolChecker"/>, which sees the real types. A body
/// that reaches a limit here gets a message that says what to do: write the type of the local or of
/// the lambda parameter, which resets the bound to the size of what was written.</para>
///
/// <para>The walk recurses over the syntax tree; the caller has checked the depth of the tree.</para>
/// </summary>
internal sealed class InlineFunctionTypeGrowthChecker
{
    /// <summary>The largest bound one type may have. A type that real code infers stays under 100.</summary>
    public const int MaxTypeSize = 65_536;

    /// <summary>The largest sum of the bounds of all expressions of a body.</summary>
    public const int MaxTotalSize = 4_194_304;

    /// <summary>
    /// What a member can add to the types it is given. A result of a framework member holds at most
    /// nine type names apart from its type parameters, and a type argument that a call infers from
    /// an interface of its receiver holds at most three (<c>GroupCollection</c> is a sequence of
    /// <c>KeyValuePair&lt;string, Group&gt;</c>).
    /// </summary>
    public const int MemberOverhead = 12;

    /// <summary>The size of a parameter of the function: a list of a nullable SQL value at most.</summary>
    private const int ParameterSize = 3;

    private readonly List<Dictionary<string, int>> scopes = [];

    /// <summary>The largest bound of a <c>return</c> of each open lambda or local function, innermost last.</summary>
    private readonly List<int> returned = [];

    /// <summary>The bound of the receiver of each open <c>?.</c> access, innermost last.</summary>
    private readonly List<int> receivers = [];

    private readonly SyntaxTree tree;

    private InlineFunctionFinding? finding;

    /// <summary>
    /// The largest bound seen so far. A lambda parameter or an <c>out var</c> whose type comes from a
    /// conversion target, and not from a call, has a type that the body wrote or measured earlier, so
    /// this value bounds it.
    /// </summary>
    private int largest = ParameterSize;

    private long total;

    private InlineFunctionTypeGrowthChecker(SyntaxTree tree)
    {
        this.tree = tree;
    }

    /// <summary>
    /// Returns a finding when a name or an expression of <paramref name="block"/> can have a type
    /// larger than <see cref="MaxTypeSize"/>, or when the types together pass
    /// <see cref="MaxTotalSize"/>. <paramref name="parameters"/> are the parameter names of the function.
    /// </summary>
    public static List<InlineFunctionFinding> Check(BlockSyntax block, IEnumerable<string> parameters)
    {
        InlineFunctionTypeGrowthChecker checker = new(block.SyntaxTree);
        Dictionary<string, int> top = new(StringComparer.Ordinal);

        foreach (string parameter in parameters)
            top[parameter] = ParameterSize;

        checker.scopes.Add(top);
        checker.Statement(block);

        return checker.finding is { } found ? [found] : [];
    }

    // ─── Statements ──────────────────────────────────────────────────────────────────────────────

    private void Statement(StatementSyntax? statement)
    {
        if (statement is null || finding is not null)
            return;

        switch (statement)
        {
            case BlockSyntax block:
                Push();

                // A local function can be called, or named as a value, before its declaration.
                foreach (StatementSyntax inner in block.Statements)
                {
                    if (inner is LocalFunctionStatementSyntax function)
                        Declare(function.Identifier, SignatureSize(function));
                }

                foreach (StatementSyntax inner in block.Statements)
                    Statement(inner);

                Pop();
                break;

            case LocalDeclarationStatementSyntax local:
                Declaration(local.Declaration);
                break;

            case ExpressionStatementSyntax expression:
                Size(expression.Expression);
                break;

            case ReturnStatementSyntax @return:
                if (@return.Expression is not null)
                {
                    int size = Size(@return.Expression);

                    if (returned.Count > 0 && size > returned[^1])
                        returned[^1] = size;
                }

                break;

            case IfStatementSyntax @if:
                Size(@if.Condition);
                Statement(@if.Statement);
                Statement(@if.Else?.Statement);
                break;

            case WhileStatementSyntax @while:
                Size(@while.Condition);
                Statement(@while.Statement);
                break;

            case DoStatementSyntax @do:
                Statement(@do.Statement);
                Size(@do.Condition);
                break;

            case ForStatementSyntax @for:
                Push();

                if (@for.Declaration is not null)
                    Declaration(@for.Declaration);

                foreach (ExpressionSyntax initializer in @for.Initializers)
                    Size(initializer);

                Size(@for.Condition);

                foreach (ExpressionSyntax incrementor in @for.Incrementors)
                    Size(incrementor);

                Statement(@for.Statement);
                Pop();
                break;

            case ForEachStatementSyntax @foreach:
            {
                // The element comes from GetEnumerator().Current: two members of the source.
                int element = Add(Add(Size(@foreach.Expression), MemberOverhead), MemberOverhead);

                Push();
                Declare(@foreach.Identifier, IsInferred(@foreach.Type) ? element : Written(@foreach.Type));
                Statement(@foreach.Statement);
                Pop();
                break;
            }

            case ForEachVariableStatementSyntax deconstructing:
            {
                int element = Add(Add(Size(deconstructing.Expression), MemberOverhead), MemberOverhead);

                Push();
                Target(deconstructing.Variable, Add(element, MemberOverhead));
                Statement(deconstructing.Statement);
                Pop();
                break;
            }

            case SwitchStatementSyntax @switch:
            {
                int governing = Size(@switch.Expression);

                Push();

                foreach (SwitchSectionSyntax section in @switch.Sections)
                {
                    foreach (SwitchLabelSyntax label in section.Labels)
                    {
                        if (label is CasePatternSwitchLabelSyntax pattern)
                        {
                            Pattern(pattern.Pattern, governing);
                            Size(pattern.WhenClause?.Condition);
                        }
                        else if (label is CaseSwitchLabelSyntax value)
                            Size(value.Value);
                    }

                    foreach (StatementSyntax inner in section.Statements)
                        Statement(inner);
                }

                Pop();
                break;
            }

            case TryStatementSyntax @try:
                Statement(@try.Block);

                foreach (CatchClauseSyntax clause in @try.Catches)
                {
                    Push();

                    if (clause.Declaration is { } declaration)
                    {
                        int size = Written(declaration.Type);

                        if (!declaration.Identifier.IsKind(SyntaxKind.None))
                            Declare(declaration.Identifier, size);
                    }

                    Size(clause.Filter?.FilterExpression);
                    Statement(clause.Block);
                    Pop();
                }

                Statement(@try.Finally?.Block);
                break;

            case ThrowStatementSyntax @throw:
                Size(@throw.Expression);
                break;

            case LocalFunctionStatementSyntax function:
                Push();

                foreach (ParameterSyntax parameter in function.ParameterList.Parameters)
                {
                    Declare(parameter.Identifier, Written(parameter.Type));
                    Size(parameter.Default?.Value);
                }

                // The returns of a local function have its written return type; they are not returns
                // of a lambda that holds the function.
                returned.Add(1);
                Statement(function.Body);
                Size(function.ExpressionBody?.Expression);
                returned.RemoveAt(returned.Count - 1);
                Pop();
                break;

            case CheckedStatementSyntax @checked:
                Statement(@checked.Block);
                break;

            case LabeledStatementSyntax labeled:
                Statement(labeled.Statement);
                break;

            default:
                // A statement form with no rule of its own (the refused forms among them): bound every
                // expression in it, so no declaration inside is missed.
                foreach (SyntaxNode child in statement.ChildNodes())
                {
                    if (child is ExpressionSyntax expression)
                        Size(expression);
                    else if (child is StatementSyntax inner)
                        Statement(inner);
                    else if (child is VariableDeclarationSyntax declaration)
                        Declaration(declaration);
                }

                break;
        }
    }

    private void Declaration(VariableDeclarationSyntax declaration)
    {
        bool inferred = IsInferred(declaration.Type);

        // The written type is measured first: it is the target of a lambda in the initializer.
        int written = inferred ? 0 : Written(declaration.Type);

        foreach (VariableDeclaratorSyntax declarator in declaration.Variables)
        {
            int value = Size(declarator.Initializer?.Value);
            Declare(declarator.Identifier, inferred ? value : written);
        }
    }

    // ─── Expressions ─────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// The bound of the type of an expression, 1 for no expression. Every bound counts toward the
    /// total, and a bound over the limit refuses the body at once: an expression of a small type can
    /// hold one of a large type (<c>(long)chain.Count()</c>), and the compiler works on both.
    /// </summary>
    private int Size(ExpressionSyntax? expression)
    {
        if (expression is null || finding is not null)
            return 1;

        int size = Measure(expression);

        if (size > largest)
            largest = size;

        total += size;

        if (finding is null && size > MaxTypeSize)
        {
            Refuse(expression.Span,
                "the type of this expression can be too large to compile; write the types of the lambda parameters in it, " +
                "or put a part of it in a local with a written type");
        }
        else if (finding is null && total > MaxTotalSize)
        {
            Refuse(expression.Span,
                "the types of the body are too large together to compile; write the types of the locals and of the " +
                "lambda parameters in place of 'var' and of inferred types");
        }

        return size;
    }

    private int Measure(ExpressionSyntax expression)
    {
        switch (expression)
        {
            case LiteralExpressionSyntax:
                return 1;

            case InterpolatedStringExpressionSyntax:
                Children(expression);
                return 1;

            case IdentifierNameSyntax name:
                return Lookup(name.Identifier.ValueText);

            case GenericNameSyntax generic:
                return Add(1, Arguments(generic.TypeArgumentList));

            case TypeSyntax type:
                return Written(type);

            case ParenthesizedExpressionSyntax parenthesized:
                return Size(parenthesized.Expression);

            case CastExpressionSyntax cast:
            {
                int target = Written(cast.Type);
                Size(cast.Expression);
                return target;
            }

            case BinaryExpressionSyntax binary:
                return Binary(binary);

            case IsPatternExpressionSyntax matched:
                Pattern(matched.Pattern, Size(matched.Expression));
                return 1;

            case PrefixUnaryExpressionSyntax prefix:
                return Size(prefix.Operand);

            case PostfixUnaryExpressionSyntax postfix:
                return Size(postfix.Operand);

            case AssignmentExpressionSyntax assignment:
                return Assignment(assignment);

            case ConditionalExpressionSyntax conditional:
                Size(conditional.Condition);
                return Math.Max(Size(conditional.WhenTrue), Size(conditional.WhenFalse));

            case SwitchExpressionSyntax @switch:
            {
                int governing = Size(@switch.GoverningExpression);
                int result = 1;

                foreach (SwitchExpressionArmSyntax arm in @switch.Arms)
                {
                    Push();
                    Pattern(arm.Pattern, governing);
                    Size(arm.WhenClause?.Condition);
                    result = Math.Max(result, Size(arm.Expression));
                    Pop();
                }

                return result;
            }

            case TupleExpressionSyntax tuple:
            {
                // A tuple of more than seven elements nests one more tuple for each seven.
                int sum = 1 + tuple.Arguments.Count / 7;

                foreach (ArgumentSyntax element in tuple.Arguments)
                    sum = Add(sum, Size(element.Expression));

                return sum;
            }

            case ArrayCreationExpressionSyntax array:
            {
                int type = Written(array.Type);

                foreach (ArrayRankSpecifierSyntax rank in array.Type.RankSpecifiers)
                {
                    foreach (ExpressionSyntax length in rank.Sizes)
                    {
                        if (length is not OmittedArraySizeExpressionSyntax)
                            Size(length);
                    }
                }

                Size(array.Initializer);
                return type;
            }

            case ImplicitArrayCreationExpressionSyntax implicitArray:
                return Add(1, Largest(implicitArray.Initializer.Expressions));

            case InitializerExpressionSyntax initializer:
                return Add(1, Largest(initializer.Expressions));

            case CollectionExpressionSyntax collection:
            {
                int element = 1;

                foreach (CollectionElementSyntax item in collection.Elements)
                {
                    element = Math.Max(element, item switch
                    {
                        ExpressionElementSyntax single => Size(single.Expression),
                        SpreadElementSyntax spread => Size(spread.Expression),
                        _ => 1,
                    });
                }

                return Add(1, element);
            }

            case ObjectCreationExpressionSyntax creation:
            {
                // The type is measured first: it is the target of a lambda among the arguments.
                int type = Written(creation.Type);

                if (creation.ArgumentList is not null)
                {
                    foreach (ArgumentSyntax argument in creation.ArgumentList.Arguments)
                        Size(argument.Expression);
                }

                Size(creation.Initializer);
                return type;
            }

            case ImplicitObjectCreationExpressionSyntax creation:
                foreach (ArgumentSyntax argument in creation.ArgumentList.Arguments)
                    Size(argument.Expression);

                Size(creation.Initializer);

                // The type is the target, which the body wrote or measured earlier.
                return 1;

            case MemberAccessExpressionSyntax member:
                // "x.Select<A, B>": the written type arguments count; the name alone does not.
                return Add(Add(Size(member.Expression), MemberOverhead), NameArguments(member.Name));

            case ConditionalAccessExpressionSyntax conditional:
            {
                int receiver = Size(conditional.Expression);

                receivers.Add(receiver);
                int result = Size(conditional.WhenNotNull);
                receivers.RemoveAt(receivers.Count - 1);

                // One more name: the result of "?." on a value type is nullable.
                return Add(Math.Max(receiver, result), 1);
            }

            case MemberBindingExpressionSyntax binding:
                return Add(Add(Receiver(), MemberOverhead), NameArguments(binding.Name));

            case ElementBindingExpressionSyntax binding:
                foreach (ArgumentSyntax index in binding.ArgumentList.Arguments)
                    Size(index.Expression);

                return Add(Receiver(), MemberOverhead);

            case ElementAccessExpressionSyntax element:
            {
                int receiver = Size(element.Expression);

                foreach (ArgumentSyntax index in element.ArgumentList.Arguments)
                    Size(index.Expression);

                return Add(receiver, MemberOverhead);
            }

            case InvocationExpressionSyntax invocation:
                return Invocation(invocation);

            case AnonymousFunctionExpressionSyntax lambda:
                // Not an argument of a call: the parameter types come from a conversion target.
                return Lambda(lambda, Add(largest, MemberOverhead), out _);

            case DeclarationExpressionSyntax declaration:
            {
                // Not an argument of a call (those are bound with the call): the type comes from a
                // constructor or an indexer of a type that the body wrote or measured earlier.
                int size = IsInferred(declaration.Type) ? Add(largest, MemberOverhead) : Written(declaration.Type);
                Designate(declaration.Designation, size);
                return size;
            }

            case DefaultExpressionSyntax @default:
                return Written(@default.Type);

            case CheckedExpressionSyntax @checked:
                return Size(@checked.Expression);

            case RefExpressionSyntax @ref:
                return Size(@ref.Expression);

            case ThrowExpressionSyntax @throw:
                Size(@throw.Expression);
                return 1;

            default:
                // No rule of its own: at most the sum of its parts. Every part is visited, so a
                // declaration inside it is not missed.
                return Add(1, Children(expression));
        }
    }

    private int Binary(BinaryExpressionSyntax binary)
    {
        switch (binary.Kind())
        {
            case SyntaxKind.AsExpression:
                Size(binary.Left);
                return binary.Right is TypeSyntax target ? Written(target) : 1;

            case SyntaxKind.IsExpression:
                Size(binary.Left);
                Size(binary.Right);
                return 1;

            // A comparison of framework types is a bool, and so is "&&" and "||" on them.
            case SyntaxKind.EqualsExpression:
            case SyntaxKind.NotEqualsExpression:
            case SyntaxKind.LessThanExpression:
            case SyntaxKind.LessThanOrEqualExpression:
            case SyntaxKind.GreaterThanExpression:
            case SyntaxKind.GreaterThanOrEqualExpression:
            case SyntaxKind.LogicalAndExpression:
            case SyntaxKind.LogicalOrExpression:
                Size(binary.Left);
                Size(binary.Right);
                return 1;

            // The result of another operator of a framework type is no larger than an operand.
            default:
                return Math.Max(Size(binary.Left), Size(binary.Right));
        }
    }

    private int Assignment(AssignmentExpressionSyntax assignment)
    {
        // "var (a, b) = e" and "(var a, b) = e": each new name is a part of e.
        if (assignment.Left is DeclarationExpressionSyntax or TupleExpressionSyntax)
        {
            int right = Size(assignment.Right);
            Target(assignment.Left, Add(right, MemberOverhead));
            return right;
        }

        // The left side is measured first: it is the target of a lambda on the right.
        int left = Size(assignment.Left);
        return Math.Max(left, Size(assignment.Right));
    }

    /// <summary>
    /// A call is at most its receiver, its arguments, its written type arguments and the results of
    /// its lambdas together, plus what a member adds.
    ///
    /// <para>A parameter of a lambda with no written type is built from type arguments of the call,
    /// and those come from the receiver, from the other arguments and from the results of the lambdas
    /// that the method takes before it. The framework's methods take their delegates in that order,
    /// so the lambdas are bound in the order of the text, each with the results of the ones before
    /// it. A call that names an argument (<c>resultSelector: ...</c>) can put them in another order;
    /// then each lambda is bound again with the results of all the others, as many times as there
    /// are lambdas.</para>
    /// </summary>
    private int Invocation(InvocationExpressionSyntax invocation)
    {
        SeparatedSyntaxList<ArgumentSyntax> arguments = invocation.ArgumentList.Arguments;

        // The name of a method is no value of its own: "x.M(...)" is given x and M's written type
        // arguments, and what a member adds is counted once, for the result.
        int given = invocation.Expression switch
        {
            MemberAccessExpressionSyntax member => Add(Size(member.Expression), NameArguments(member.Name)),
            MemberBindingExpressionSyntax binding => Add(Receiver(), NameArguments(binding.Name)),
            _ => Size(invocation.Expression),
        };

        int lambdas = 0;

        bool named = false;

        foreach (ArgumentSyntax argument in arguments)
        {
            named |= argument.NameColon is not null;

            if (argument.Expression is AnonymousFunctionExpressionSyntax)
                lambdas++;
            else if (argument.Expression is not DeclarationExpressionSyntax)
                given = Add(given, Size(argument.Expression));
        }

        int results = 0;

        if (lambdas > 0 && !named)
        {
            foreach (ArgumentSyntax argument in arguments)
            {
                if (argument.Expression is AnonymousFunctionExpressionSyntax lambda)
                {
                    Lambda(lambda, Add(Add(given, results), MemberOverhead), out int result);
                    results = Add(results, result);
                }
            }
        }
        else if (lambdas > 0)
        {
            for (int pass = 0; pass < lambdas && finding is null; pass++)
            {
                int inferred = Add(Add(given, results), MemberOverhead);
                int sum = 0;

                foreach (ArgumentSyntax argument in arguments)
                {
                    if (argument.Expression is AnonymousFunctionExpressionSyntax lambda)
                    {
                        Lambda(lambda, inferred, out int result);
                        sum = Add(sum, result);
                    }
                }

                if (sum <= results)
                    break;

                results = sum;
            }
        }

        int all = Add(given, results);

        // "out var x": x has a type that the method builds from what it was given.
        foreach (ArgumentSyntax argument in arguments)
        {
            if (argument.Expression is DeclarationExpressionSyntax declaration)
            {
                int declared = IsInferred(declaration.Type) ? Add(all, MemberOverhead) : Written(declaration.Type);
                Designate(declaration.Designation, declared);
                all = Add(all, declared);
            }
        }

        return Add(all, MemberOverhead);
    }

    /// <summary>
    /// Bounds the body of a lambda. Returns the bound of the lambda's own delegate type, and gives in
    /// <paramref name="result"/> what the lambda adds to a call that takes it: the bound of its
    /// result, with its written parameter types, which a call can also infer type arguments from.
    /// </summary>
    private int Lambda(AnonymousFunctionExpressionSyntax lambda, int inferredParameter, out int result)
    {
        int written = 0;
        int inferred = 0;

        Push();

        if (lambda is SimpleLambdaExpressionSyntax simple)
            Parameter(simple.Parameter, inferredParameter, ref written, ref inferred);
        else if (lambda is ParenthesizedLambdaExpressionSyntax parenthesized)
        {
            foreach (ParameterSyntax parameter in parenthesized.ParameterList.Parameters)
                Parameter(parameter, inferredParameter, ref written, ref inferred);
        }
        else if (lambda is AnonymousMethodExpressionSyntax { ParameterList: { } list })
        {
            foreach (ParameterSyntax parameter in list.Parameters)
                Parameter(parameter, inferredParameter, ref written, ref inferred);
        }

        returned.Add(1);

        if (lambda.ExpressionBody is not null)
            returned[^1] = Size(lambda.ExpressionBody);
        else
            Statement(lambda.Block);

        int produced = returned[^1];
        returned.RemoveAt(returned.Count - 1);

        Pop();

        if (lambda is ParenthesizedLambdaExpressionSyntax { ReturnType: { } declared })
            produced = Math.Max(produced, Written(declared));

        result = Add(produced, written);
        return Add(Add(result, inferred), 1);
    }

    private void Parameter(ParameterSyntax parameter, int inferredParameter, ref int written, ref int inferred)
    {
        if (parameter.Type is null)
        {
            inferred = Add(inferred, inferredParameter);
            Declare(parameter.Identifier, inferredParameter);
        }
        else
        {
            int size = Written(parameter.Type);
            written = Add(written, size);
            Declare(parameter.Identifier, size);
        }
    }

    /// <summary>
    /// Declares the variables of a pattern that is matched against a value whose bound is
    /// <paramref name="input"/>. A variable of a sub-pattern is a member of the value, or a part that
    /// a <c>Deconstruct</c> method or an indexer gives.
    /// </summary>
    private void Pattern(PatternSyntax? pattern, int input)
    {
        switch (pattern)
        {
            case DeclarationPatternSyntax declaration:
                Designate(declaration.Designation, Written(declaration.Type));
                break;

            case VarPatternSyntax inferred:
                Designate(inferred.Designation, input);
                break;

            case RecursivePatternSyntax recursive:
            {
                int subject = recursive.Type is null ? input : Written(recursive.Type);

                if (recursive.Designation is not null)
                    Designate(recursive.Designation, subject);

                if (recursive.PositionalPatternClause is not null)
                {
                    foreach (SubpatternSyntax part in recursive.PositionalPatternClause.Subpatterns)
                        Pattern(part.Pattern, Add(subject, MemberOverhead));
                }

                if (recursive.PropertyPatternClause is not null)
                {
                    foreach (SubpatternSyntax part in recursive.PropertyPatternClause.Subpatterns)
                    {
                        // "{ A.B.C: var x }" reads one member for each name.
                        int member = Add(subject, MemberOverhead);

                        if (part.ExpressionColon is not null)
                        {
                            foreach (SyntaxNode node in part.ExpressionColon.Expression.DescendantNodes())
                            {
                                if (node is SimpleNameSyntax)
                                    member = Add(member, MemberOverhead);
                            }
                        }

                        Pattern(part.Pattern, member);
                    }
                }

                break;
            }

            case ListPatternSyntax list:
                if (list.Designation is not null)
                    Designate(list.Designation, input);

                foreach (PatternSyntax item in list.Patterns)
                    Pattern(item, Add(input, MemberOverhead));

                break;

            case SlicePatternSyntax slice:
                Pattern(slice.Pattern, Add(input, MemberOverhead));
                break;

            case BinaryPatternSyntax binary:
                Pattern(binary.Left, input);
                Pattern(binary.Right, input);
                break;

            case UnaryPatternSyntax unary:
                Pattern(unary.Pattern, input);
                break;

            case ParenthesizedPatternSyntax parenthesized:
                Pattern(parenthesized.Pattern, input);
                break;

            case ConstantPatternSyntax constant:
                Size(constant.Expression);
                break;

            case RelationalPatternSyntax relational:
                Size(relational.Expression);
                break;

            case TypePatternSyntax type:
                Written(type.Type);
                break;
        }
    }

    /// <summary>Declares the names of a designation; each level of parentheses is one deconstruction.</summary>
    private void Designate(VariableDesignationSyntax designation, int size)
    {
        switch (designation)
        {
            case SingleVariableDesignationSyntax single:
                Declare(single.Identifier, size);
                break;

            case ParenthesizedVariableDesignationSyntax several:
                foreach (VariableDesignationSyntax part in several.Variables)
                    Designate(part, Add(size, MemberOverhead));
                break;
        }
    }

    /// <summary>The left side of a deconstruction: new names get <paramref name="size"/>, other targets are only visited.</summary>
    private void Target(ExpressionSyntax target, int size)
    {
        switch (target)
        {
            case DeclarationExpressionSyntax declaration:
                Designate(declaration.Designation, IsInferred(declaration.Type) ? size : Written(declaration.Type));
                break;

            case TupleExpressionSyntax tuple:
                foreach (ArgumentSyntax element in tuple.Arguments)
                    Target(element.Expression, Add(size, MemberOverhead));
                break;

            default:
                Size(target);
                break;
        }
    }

    private int Children(SyntaxNode node)
    {
        int sum = 0;

        foreach (SyntaxNode child in node.ChildNodes())
        {
            if (child is ExpressionSyntax expression)
                sum = Add(sum, Size(expression));
            else if (child is not StatementSyntax)
                sum = Add(sum, Children(child));
        }

        return sum;
    }

    private int Largest(SeparatedSyntaxList<ExpressionSyntax> expressions)
    {
        int size = 1;

        foreach (ExpressionSyntax expression in expressions)
            size = Math.Max(size, Size(expression));

        return size;
    }

    private int Receiver() => receivers.Count > 0 ? receivers[^1] : largest;

    // ─── Types and names ─────────────────────────────────────────────────────────────────────────

    /// <summary>The size of a written type, which also becomes a known size for later conversion targets.</summary>
    private int Written(TypeSyntax? type)
    {
        int size = TypeSize(type);

        if (size > largest)
            largest = size;

        if (finding is null && size > MaxTypeSize && type is not null)
            Refuse(type.Span, "this type is too large to compile");

        return size;
    }

    /// <summary>The number of type names in a written type: <c>Dictionary&lt;string, long[]&gt;</c> is 4.</summary>
    private static int TypeSize(TypeSyntax? type)
    {
        switch (type)
        {
            case null:
                return 1;

            case GenericNameSyntax generic:
                return Add(1, Arguments(generic.TypeArgumentList));

            case QualifiedNameSyntax qualified:
                return TypeSize(qualified.Right);

            case AliasQualifiedNameSyntax alias:
                return TypeSize(alias.Name);

            case ArrayTypeSyntax array:
                return Add(TypeSize(array.ElementType), array.RankSpecifiers.Count);

            case NullableTypeSyntax nullable:
                return Add(TypeSize(nullable.ElementType), 1);

            case TupleTypeSyntax tuple:
            {
                int sum = 1 + tuple.Elements.Count / 7;

                foreach (TupleElementSyntax element in tuple.Elements)
                    sum = Add(sum, TypeSize(element.Type));

                return sum;
            }

            case RefTypeSyntax @ref:
                return TypeSize(@ref.Type);

            case ScopedTypeSyntax scoped:
                return TypeSize(scoped.Type);

            default:
                return 1;
        }
    }

    private static int Arguments(TypeArgumentListSyntax arguments)
    {
        int sum = 0;

        foreach (TypeSyntax argument in arguments.Arguments)
            sum = Add(sum, TypeSize(argument));

        return sum;
    }

    private static int NameArguments(SimpleNameSyntax name) =>
        name is GenericNameSyntax generic ? Arguments(generic.TypeArgumentList) : 0;

    /// <summary>
    /// The size of a local function as a value: a name of a local function with no call is a delegate
    /// of its parameter types and its return type.
    /// </summary>
    private int SignatureSize(LocalFunctionStatementSyntax function)
    {
        int size = Add(1, Written(function.ReturnType));

        foreach (ParameterSyntax parameter in function.ParameterList.Parameters)
            size = Add(size, Written(parameter.Type));

        return size;
    }

    /// <summary>True for <c>var</c>, also behind <c>ref</c> or <c>scoped</c>.</summary>
    private static bool IsInferred(TypeSyntax type)
    {
        while (true)
        {
            if (type is RefTypeSyntax @ref)
                type = @ref.Type;
            else if (type is ScopedTypeSyntax scoped)
                type = scoped.Type;
            else
                return type.IsVar;
        }
    }

    /// <summary>Adds two bounds, and stops just over the limit so the numbers never overflow.</summary>
    private static int Add(int left, int right) => (int)Math.Min(MaxTypeSize + 1L, (long)left + right);

    private void Push() => scopes.Add(new Dictionary<string, int>(StringComparer.Ordinal));

    private void Pop() => scopes.RemoveAt(scopes.Count - 1);

    private void Declare(SyntaxToken name, int size)
    {
        scopes[^1][name.ValueText] = size;

        if (size > largest)
            largest = size;

        if (finding is null && size > MaxTypeSize)
        {
            Refuse(name.Span,
                $"the type of '{name.ValueText}' can be too large to compile; write its type in place of 'var', " +
                "or the types of the lambda parameters it comes from");
        }
    }

    /// <summary>A name that is no local in scope is a type, a member or a method: its size is one.</summary>
    private int Lookup(string name)
    {
        for (int i = scopes.Count - 1; i >= 0; i--)
        {
            if (scopes[i].TryGetValue(name, out int size))
                return size;
        }

        return 1;
    }

    private void Refuse(TextSpan span, string message)
    {
        LinePosition start = tree.GetLineSpan(span).StartLinePosition;
        finding = new InlineFunctionFinding(Math.Max(1, start.Line), start.Character + 1, message);
    }
}

#endif
