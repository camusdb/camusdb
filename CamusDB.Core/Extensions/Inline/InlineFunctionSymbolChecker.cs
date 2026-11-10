/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

#if !CAMUSDB_BROWSER

using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Operations;
using Microsoft.CodeAnalysis.Text;

namespace CamusDB.Core.Extensions.Inline;

/// <summary>What the symbol check of a body found, and what the budget rewrite needs from it.</summary>
internal sealed class InlineFunctionSymbolReport
{
    /// <summary>Every refused symbol, type and operation, in source order. Empty when the body passes.</summary>
    public List<InlineFunctionFinding> Findings { get; } = [];

    /// <summary>
    /// The spans of the expressions the body passes as the match timeout of a regular expression. The
    /// rewrite puts each one through the budget's limit, so one match cannot outlive the call. Spans
    /// and not nodes: the syntax tree may build a node object again, and two argument expressions
    /// never have the same span.
    /// </summary>
    public HashSet<TextSpan> MatchTimeoutArguments { get; } = [];

    /// <summary>
    /// The spans of the lambdas whose delegate returns no value. The rewrite turns an
    /// expression-bodied lambda into a block, and a block needs <c>return</c> only when there is a
    /// value.
    /// </summary>
    public HashSet<TextSpan> VoidLambdas { get; } = [];
}

/// <summary>
/// The second check of the body of an inline function, and the one that makes it safe to compile
/// source from a user who is not a superuser: every symbol the body binds must be on
/// <see cref="InlineFunctionAllowList"/>. It reads the compiler's semantic model, so it sees what a
/// name means and not how it is spelled: an alias, a <c>var</c>, an extension method call and an
/// implicit conversion all resolve to the symbol they use.
///
/// <para><b>Two walks, one rule.</b> The first walk reads the operation tree of the body, which names
/// the method of every call, the constructor of every creation and the operator of every expression,
/// including the ones the source does not spell (a conversion, a collection initializer's
/// <c>Add</c>). The second walk asks the semantic model about every syntax node: its symbol, its type
/// and its converted type, and the hidden symbols of <c>foreach</c>, deconstruction and collection
/// initializers. A symbol either walk finds must pass. An operation kind that the first walk does not
/// know is refused, so a new language feature is closed until someone reviews it.</para>
///
/// <para><b>Rules beyond the list.</b></para>
/// <list type="bullet">
/// <item><b>Signatures.</b> Every type in the definition of a called member must be listed. That
/// takes out the overloads that hand over a span, a pointer, a <c>Type</c> or a stream, on every
/// listed type, without naming them.</item>
/// <item><b><c>object</c> never inside another type.</b> A tuple or a pair with an <c>object</c>
/// element can hold another boxed tuple, and so on to any depth at run time; their <c>ToString</c>,
/// <c>Equals</c> and hash then recurse through every level inside the framework, where no budget
/// check runs, until the stack overflows and the process ends. With <c>object</c> allowed only as
/// the type of a local or a parameter, no value can hold another to an unbounded depth.</item>
/// <item><b>A lazy sequence cannot be kept.</b> <c>IEnumerable&lt;T&gt;</c> from a LINQ operator is
/// computed when it is read, through every operator it was built from. A loop that stores the
/// sequence and wraps it again builds a chain that is deeper than the stack. A lazy sequence is
/// therefore allowed inside one expression only: not as a local, not as a parameter or a result of
/// a local function, not as a type argument. <c>ToList()</c> or <c>ToArray()</c> ends the chain.</item>
/// <item><b>No cast to an interface.</b> Some LINQ operators return objects that also implement
/// <c>IList&lt;T&gt;</c>. A cast would let the body keep one under a type that is not lazy.</item>
/// <item><b>Exceptions stay exceptions.</b> An exception cannot be converted to <c>object</c>, be a
/// type argument or fill an interpolation hole: each would call its <c>ToString</c>, which prints
/// the server's stack.</item>
/// <item><b>Formats are constants.</b> A format text given to <c>ToString</c> or an interpolation
/// hole must be a constant, short, with a precision of at most two digits (.NET accepts a precision
/// of 999,999,999, which is a 1 GB string), and a date format cannot print the machine's time
/// zone.</item>
/// <item><b>Regular expressions need a timeout</b>, and the result of <c>Matches</c> can only be read
/// with <c>foreach</c>, so the budget check of the loop runs between two matches.</item>
/// <item><b>Own code.</b> A symbol of the compiled unit itself passes only when it is a local, a
/// parameter, a lambda or a local function of the body. A body cannot call the method it is in: that
/// would be a recursion with no budget check.</item>
/// </list>
/// </summary>
internal sealed class InlineFunctionSymbolChecker
{
    /// <summary>The most findings one report lists; a body with more is refused with these and a count.</summary>
    private const int MaxFindings = 40;

    private const int MaxMessageLength = 400;

    private enum TypeUse
    {
        /// <summary>The type of an expression: a value that exists for the length of one expression.</summary>
        Value,

        /// <summary>The type of something the body keeps or names: a local, a creation, a type argument.</summary>
        Kept,

        /// <summary>A type in the definition of a framework member.</summary>
        Signature,

        /// <summary>The type of a lambda parameter, which the called framework method chooses.</summary>
        LambdaParameter,
    }

    private static readonly SymbolDisplayFormat DisplayFormat = SymbolDisplayFormat.CSharpShortErrorMessageFormat;

    private static readonly HashSet<string> MatchTimeoutMembers =
        new(StringComparer.Ordinal) { ".ctor", "IsMatch", "Match", "Matches", "Replace", "Split", "Count" };

    private static readonly HashSet<string> FormattableTypes = new(StringComparer.Ordinal)
    {
        "System.Byte", "System.SByte", "System.Int16", "System.UInt16", "System.Int32", "System.UInt32", "System.Int64",
        "System.UInt64", "System.Single", "System.Double", "System.Decimal", "System.DateTime", "System.DateOnly",
        "System.TimeOnly", "System.TimeSpan", "System.Guid",
    };

    private readonly SemanticModel model;

    private readonly IAssemblySymbol ownAssembly;

    private readonly IMethodSymbol bodyMethod;

    private readonly BlockSyntax bodyBlock;

    private readonly InlineUnitKind kind;

    /// <summary>The server-written class that holds the body; its own members pass for a trigger body (see <see cref="IsGenerated(ISymbol)"/>).</summary>
    private readonly INamedTypeSymbol wrapper;

    private readonly int bodyStartLine;

    private readonly int arrayLimit;

    private readonly CancellationToken cancellation;

    private readonly InlineFunctionSymbolReport report = new();

    private readonly HashSet<(int, string)> reported = [];

    private readonly InlineFunctionTypeShape shapes = new();

    /// <summary>
    /// The bytes of values that each method of the body works with, by the syntax node that owns the
    /// stack frame: the body's block, a lambda or a local function.
    /// </summary>
    private readonly Dictionary<SyntaxNode, int> frames = [];

    private int suppressed;

    private InlineFunctionSymbolChecker(
        SemanticModel model, IMethodSymbol bodyMethod, BlockSyntax bodyBlock, int bodyStartLine, int arrayLimit, CancellationToken cancellation, InlineUnitKind kind)
    {
        this.model = model;
        this.bodyMethod = bodyMethod;
        this.bodyBlock = bodyBlock;
        this.kind = kind;
        wrapper = bodyMethod.ContainingType;
        this.bodyStartLine = bodyStartLine;
        this.arrayLimit = arrayLimit;
        this.cancellation = cancellation;
        ownAssembly = model.Compilation.Assembly;
    }

    /// <summary>
    /// Checks the block of <paramref name="method"/>, the server-written method that holds the body.
    /// <paramref name="arrayLimit"/> bounds an array that is created with constant sizes in more than
    /// one dimension; the budget rewrite checks every other sized array at run time.
    /// </summary>
    public static InlineFunctionSymbolReport Check(
        SemanticModel model, MethodDeclarationSyntax method, int arrayLimit, CancellationToken cancellation) =>
        Check(model, method, arrayLimit, cancellation, InlineUnitKind.Function);

    /// <summary>
    /// The check for a body of <paramref name="kind"/>. A trigger body may use the members the server
    /// wrote into its class (the row objects, the session, the function stubs), the implicit instance
    /// they hang on, and <c>await</c> on the session's tasks.
    /// </summary>
    public static InlineFunctionSymbolReport Check(
        SemanticModel model, MethodDeclarationSyntax method, int arrayLimit, CancellationToken cancellation, InlineUnitKind kind)
    {
        BlockSyntax block = method.Body!;
        IMethodSymbol symbol = model.GetDeclaredSymbol(method, cancellation)!;
        int startLine = block.SyntaxTree.GetLineSpan(block.OpenBraceToken.Span).StartLinePosition.Line;

        InlineFunctionSymbolChecker checker = new(model, symbol, block, startLine, arrayLimit, cancellation, kind);


        // First, alone: reading a constant string below makes the compiler build it, and a chain of
        // "const string b = a + a;" doubles the text with each line.
        if (!checker.CheckConstantStrings(block))
            return checker.report;

        checker.WalkOperations(block);
        checker.WalkSyntax(block);
        checker.CheckFrames(block);

        if (checker.suppressed > 0)
            checker.report.Findings.Add(new InlineFunctionFinding(1, 1, $"and {checker.suppressed} more problems"));

        return checker.report;
    }

    // ─── Walks ───────────────────────────────────────────────────────────────────────────────────

    private bool CheckConstantStrings(BlockSyntax block)
    {
        bool passed = true;

        foreach (SyntaxNode node in block.DescendantNodes())
        {
            if (node is VariableDeclaratorSyntax declarator &&
                model.GetDeclaredSymbol(declarator, cancellation) is ILocalSymbol { IsConst: true, Type.SpecialType: SpecialType.System_String })
            {
                Refuse(declarator, "a const string is not allowed; declare it without const");
                passed = false;
            }
        }

        return passed;
    }

    private void WalkOperations(BlockSyntax block)
    {
        IOperation? root = model.GetOperation(block, cancellation);

        if (root is null)
        {
            Refuse(block, "the body has no operation tree");
            return;
        }

        foreach (IOperation operation in root.DescendantsAndSelf())
        {
            cancellation.ThrowIfCancellationRequested();
            Visit(operation);
            CountValue(operation);
        }
    }

    /// <summary>
    /// Adds the value that an operation makes to the frame of the method it is in. A read of a local,
    /// of a parameter or of a field makes no new value; every other operation of a value type can
    /// leave one in a slot of its own, because the runtime's compiler does not share the slots of
    /// such values.
    /// </summary>
    private void CountValue(IOperation operation)
    {
        if (operation.Type is not { IsValueType: true } type ||
            operation is ILocalReferenceOperation or IParameterReferenceOperation or IFieldReferenceOperation or
                IDiscardOperation or IInstanceReferenceOperation or IDeclarationExpressionOperation or
                ILiteralOperation or IAssignmentOperation or IParenthesizedOperation)
            return;

        // A conversion that changes nothing, and one the compiler folds into a constant, make no value.
        if (operation is IConversionOperation conversion &&
            (conversion.Conversion.IsIdentity || conversion.Operand.ConstantValue.HasValue))
            return;


        int bytes = shapes.Of(type).Bytes;

        // A value of one register needs no slot.
        if (bytes > 8)
            AddToFrame(operation.Syntax, bytes);
    }

    private void AddToFrame(SyntaxNode where, int bytes)
    {
        SyntaxNode owner = bodyBlock;

        for (SyntaxNode? node = where; node is not null && node != bodyBlock; node = node.Parent)
        {
            if (node is AnonymousFunctionExpressionSyntax or LocalFunctionStatementSyntax)
            {
                owner = node;
                break;
            }
        }

        frames[owner] = Math.Min(InlineFunctionTypeShape.MaxFrameBytes + 1, frames.GetValueOrDefault(owner) + bytes);
    }

    private void CheckFrames(BlockSyntax block)
    {
        foreach ((SyntaxNode owner, int bytes) in frames)
        {
            if (bytes <= InlineFunctionTypeShape.MaxFrameBytes)
                continue;

            string what = owner == block ? "the body" : owner is LocalFunctionStatementSyntax ? "this local function" : "this lambda";

            Refuse(owner == block ? block.Statements.FirstOrDefault() ?? (SyntaxNode)block : owner,
                $"{what} works with more than {InlineFunctionTypeShape.MaxFrameBytes} bytes of values at one time, which its stack frame " +
                "cannot hold safely; move a part of it to a local function, or keep large values in a list or an array");
        }
    }

    private void WalkSyntax(BlockSyntax block)
    {
        foreach (SyntaxNode node in block.DescendantNodes())
        {
            cancellation.ThrowIfCancellationRequested();

            switch (node)
            {
                case VariableDeclaratorSyntax or SingleVariableDesignationSyntax or CatchDeclarationSyntax or ForEachStatementSyntax:
                    if (model.GetDeclaredSymbol(node, cancellation) is ILocalSymbol local)
                    {
                        CheckType(local.Type, node, TypeUse.Kept);
                        AddToFrame(node, Math.Max(8, shapes.Of(local.Type).Bytes));
                    }

                    break;

                case LocalFunctionStatementSyntax function:
                    if (model.GetDeclaredSymbol(function, cancellation) is IMethodSymbol declared)
                    {
                        CheckType(declared.ReturnType, function.ReturnType, TypeUse.Kept);

                        foreach (IParameterSymbol parameter in declared.Parameters)
                            CheckType(parameter.Type, function.ParameterList, TypeUse.Kept);
                    }

                    break;
            }

            if (node is CommonForEachStatementSyntax loop)
                CheckForEach(loop);

            if (node is ForEachVariableStatementSyntax deconstructingLoop)
                CheckDeconstruction(model.GetDeconstructionInfo(deconstructingLoop), node);

            if (node is AssignmentExpressionSyntax assignment && assignment.IsKind(SyntaxKind.SimpleAssignmentExpression) &&
                assignment.Left is TupleExpressionSyntax or DeclarationExpressionSyntax)
                CheckDeconstruction(model.GetDeconstructionInfo(assignment), node);

            if (node is InitializerExpressionSyntax initializer && initializer.IsKind(SyntaxKind.CollectionInitializerExpression))
            {
                foreach (ExpressionSyntax element in initializer.Expressions)
                    CheckSymbolInfo(model.GetCollectionInitializerSymbolInfo(element, cancellation), element);
            }

            // The empty size in "long[]" and the empty argument in "List<>" are expressions in the
            // syntax tree and have no type.
            if (node is OmittedArraySizeExpressionSyntax or OmittedTypeArgumentSyntax)
                continue;

            // The word "nameof" is no symbol and has no type; its argument is checked as a name.
            if (node is IdentifierNameSyntax { Identifier.ValueText: "nameof", Parent: InvocationExpressionSyntax nameOf } &&
                model.GetConstantValue(nameOf, cancellation).HasValue)
                continue;


            if (node is ExpressionSyntax or TypeSyntax or OrderingSyntax or ArgumentSyntax)
            {
                CheckSymbolInfo(model.GetSymbolInfo(node, cancellation), node);

                if (node is ExpressionSyntax expression)
                {
                    TypeInfo type = model.GetTypeInfo(expression, cancellation);

                    // A type name is checked as something the body names; a value as a value.
                    bool namesType = expression is TypeSyntax && model.GetSymbolInfo(expression, cancellation).Symbol is ITypeSymbol;
                    CheckType(type.Type, node, namesType ? TypeUse.Kept : TypeUse.Value);
                    CheckType(type.ConvertedType, node, TypeUse.Value);

                    if (model.GetConversion(expression, cancellation).MethodSymbol is { } conversion)
                        CheckSymbol(conversion, node, write: false);
                }
            }
        }
    }

    private void CheckSymbolInfo(SymbolInfo info, SyntaxNode where)
    {
        if (info.Symbol is { } symbol)
        {
            CheckSymbol(symbol, where, write: false);
            return;
        }

        foreach (ISymbol candidate in info.CandidateSymbols)
            CheckSymbol(candidate, where, write: false);
    }

    private void CheckForEach(CommonForEachStatementSyntax loop)
    {
        ForEachStatementInfo info = model.GetForEachStatementInfo(loop);

        if (info.IsAsynchronous)
            Refuse(loop, "await foreach is not allowed");

        CheckOptional(info.GetEnumeratorMethod, loop.Expression);
        CheckOptional(info.MoveNextMethod, loop.Expression);
        CheckOptional(info.CurrentProperty, loop.Expression);
        CheckOptional(info.DisposeMethod, loop.Expression);
        CheckOptional(info.ElementConversion.MethodSymbol, loop.Expression);
        CheckOptional(info.CurrentConversion.MethodSymbol, loop.Expression);
        CheckType(info.ElementType, loop.Expression, TypeUse.Value);

        // The enumerator of a list or a dictionary is a struct that the loop keeps in its frame.
        if (info.GetEnumeratorMethod?.ReturnType is { IsValueType: true } enumerator)
            AddToFrame(loop, Math.Max(8, shapes.Of(enumerator).Bytes));

        // The element conversion of "foreach (IList<int> x in objects)" is a cast like any other.

        if (info.ElementConversion.IsExplicit && info.ElementType is not null &&
            model.GetDeclaredSymbol(loop, cancellation) is ILocalSymbol { Type.TypeKind: TypeKind.Interface })
            Refuse(loop, "a cast to an interface is not allowed");
    }

    private void CheckDeconstruction(DeconstructionInfo info, SyntaxNode where)
    {
        CheckOptional(info.Method, where);
        CheckOptional(info.Conversion?.MethodSymbol, where);

        foreach (DeconstructionInfo nested in info.Nested)
            CheckDeconstruction(nested, where);
    }

    private void CheckOptional(ISymbol? symbol, SyntaxNode where)
    {
        if (symbol is not null)
            CheckSymbol(symbol, where, write: false);
    }

    // ─── Operations ──────────────────────────────────────────────────────────────────────────────

    private void Visit(IOperation operation)
    {
        SyntaxNode where = operation.Syntax;

        switch (operation)
        {
            case IInvocationOperation invocation:
                CheckSymbol(invocation.TargetMethod, where, write: false);
                CheckOptionalType(invocation.ConstrainedToType, where);
                CheckCallArguments(invocation.TargetMethod, invocation.Arguments, invocation.Instance?.Type);
                break;

            case IObjectCreationOperation creation:
                CheckOptional(creation.Constructor, where);
                CheckType(creation.Type, where, TypeUse.Kept);

                if (creation.Constructor is { } constructor)
                    CheckCallArguments(constructor, creation.Arguments, receiverType: null);

                break;

            case IPropertyReferenceOperation property:
                CheckSymbol(property.Property, where, IsWritten(property));
                CheckOptionalType(property.ConstrainedToType, where);
                break;

            case IFieldReferenceOperation field:
                CheckSymbol(field.Field, where, IsWritten(field));
                break;

            case IMethodReferenceOperation method:
                CheckSymbol(method.Method, where, write: false);
                break;

            case ILocalReferenceOperation local:
                CheckSymbol(local.Local, where, write: false);
                break;

            case IParameterReferenceOperation parameter:
                CheckSymbol(parameter.Parameter, where, write: false);
                break;

            case IUnaryOperation unary:
                CheckOptional(unary.OperatorMethod, where);
                break;

            case IBinaryOperation binary:
                CheckOptional(binary.OperatorMethod, where);

                RefuseDelegateArithmetic(binary.LeftOperand.Type, where);
                break;

            case ICompoundAssignmentOperation compound:
                CheckOptional(compound.OperatorMethod, where);
                CheckOptional(compound.InConversion.MethodSymbol, where);
                CheckOptional(compound.OutConversion.MethodSymbol, where);
                RefuseDelegateArithmetic(compound.Target.Type, where);
                break;

            case IIncrementOrDecrementOperation increment:
                CheckOptional(increment.OperatorMethod, where);
                break;

            case IConversionOperation conversion:
                CheckConversion(conversion);
                break;

            case ICoalesceOperation coalesce:
                CheckOptional(coalesce.ValueConversion.MethodSymbol, where);
                break;

            case IArgumentOperation argument:
                CheckOptional(argument.InConversion.MethodSymbol, where);
                CheckOptional(argument.OutConversion.MethodSymbol, where);
                break;

            case IAnonymousFunctionOperation lambda:
                foreach (IParameterSymbol parameter in lambda.Symbol.Parameters)
                    CheckType(parameter.Type, where, TypeUse.LambdaParameter);

                if (lambda.Symbol.ReturnsVoid && where is AnonymousFunctionExpressionSyntax)
                    report.VoidLambdas.Add(where.Span);

                break;

            case ILocalFunctionOperation function:
                CheckType(function.Symbol.ReturnType, where, TypeUse.Kept);

                foreach (IParameterSymbol parameter in function.Symbol.Parameters)
                    CheckType(parameter.Type, where, TypeUse.Kept);

                break;

            case IVariableDeclaratorOperation declarator:
                CheckType(declarator.Symbol.Type, where, TypeUse.Kept);
                break;

            case IDeclarationExpressionOperation or IDiscardOperation or IDefaultValueOperation:
                CheckType(operation.Type, where, TypeUse.Kept);
                break;

            case IArrayCreationOperation array:
                CheckType(array.Type, where, TypeUse.Kept);
                CheckArraySizes(array);
                break;

            case ITupleOperation tuple:
                CheckType(tuple.NaturalType, where, TypeUse.Kept);
                break;

            case ICollectionExpressionOperation collection:
                CheckOptional(collection.ConstructMethod, where);
                break;

            case ISpreadOperation spread:
                CheckOptional(spread.ElementConversion.MethodSymbol, where);
                CheckOptionalType(spread.ElementType, where);
                break;

            // A catch with no type has the type object here; it catches everything.
            case ICatchClauseOperation clause:
                if (clause.ExceptionType.SpecialType != SpecialType.System_Object && !IsExceptionType(clause.ExceptionType))
                    Refuse(where, $"a catch clause cannot catch {Display(clause.ExceptionType)}");

                break;

            case IInterpolationOperation hole:
                CheckInterpolationHole(hole);
                break;

            case IIsTypeOperation test:
                CheckTypeTest(test.TypeOperand, where);
                break;

            case ITypePatternOperation pattern:
                CheckTypeTest(pattern.MatchedType, where);
                break;

            case IDeclarationPatternOperation pattern:
                CheckTypeTest(pattern.MatchedType, where);

                if (pattern.DeclaredSymbol is ILocalSymbol declared)
                    CheckType(declared.Type, where, TypeUse.Kept);

                break;

            case IRecursivePatternOperation pattern:
                CheckOptional(pattern.DeconstructSymbol, where);
                CheckTypeTest(pattern.MatchedType, where);

                if (pattern.DeclaredSymbol is ILocalSymbol captured)
                    CheckType(captured.Type, where, TypeUse.Kept);

                break;

            case IListPatternOperation list:
                CheckOptional(list.LengthSymbol, where);
                CheckOptional(list.IndexerSymbol, where);

                if (list.DeclaredSymbol is ILocalSymbol listLocal)
                    CheckType(listLocal.Type, where, TypeUse.Kept);

                break;

            case IInstanceReferenceOperation instance:
                // A trigger body hangs on an instance the server made: NEW, OLD and Sql are its fields.
                if (kind != InlineUnitKind.Trigger &&
                    instance.ReferenceKind is not (InstanceReferenceKind.ImplicitReceiver or InstanceReferenceKind.PatternInput))
                    Refuse(where, "'this' is not allowed");

                break;

            case IAwaitOperation:
                // The syntax check refused await outside a trigger body and inside its lambdas.
                break;

            case IBranchOperation branch:
                if (branch.BranchKind == BranchKind.GoTo)
                    Refuse(where, "goto and labels are not allowed; use a loop");

                break;

            case IForEachLoopOperation loop:
                if (loop.IsAsynchronous)
                    Refuse(where, "await foreach is not allowed");

                break;

            default:
                if (!IsPlainOperation(operation.Kind))
                    Refuse(where, $"this language form is not allowed ({operation.Kind})");

                break;
        }

        // A lambda's own type holds the types of the delegate it is given to, which the called
        // framework method chooses; its parameters are checked above.
        if (operation.Type is not null && operation is not (IAnonymousFunctionOperation or IDelegateCreationOperation))
            CheckType(operation.Type, where, TypeUse.Value);
    }

    /// <summary>
    /// The operation kinds that carry no symbol of their own and need no rule: statements, literals,
    /// references the cases above do not need, and the parts of patterns and initializers. Every kind
    /// that is not here and has no case above is refused.
    /// </summary>
    private static bool IsPlainOperation(OperationKind kind) => kind is
        OperationKind.Block or OperationKind.VariableDeclarationGroup or OperationKind.VariableDeclaration or
        OperationKind.VariableInitializer or OperationKind.Switch or OperationKind.SwitchCase or OperationKind.CaseClause or
        OperationKind.Loop or OperationKind.Empty or OperationKind.Return or OperationKind.Conditional or
        OperationKind.ExpressionStatement or OperationKind.Try or OperationKind.Throw or OperationKind.Literal or
        OperationKind.ArrayElementReference or OperationKind.DelegateCreation or OperationKind.ArrayInitializer or
        OperationKind.SimpleAssignment or OperationKind.CoalesceAssignment or OperationKind.ConditionalAccess or
        OperationKind.ConditionalAccessInstance or OperationKind.InterpolatedString or OperationKind.InterpolatedStringText or
        OperationKind.InterpolatedStringAddition or OperationKind.IsPattern or OperationKind.ConstantPattern or
        OperationKind.RelationalPattern or OperationKind.NegatedPattern or OperationKind.BinaryPattern or
        OperationKind.PropertySubpattern or OperationKind.DiscardPattern or OperationKind.TupleBinary or
        OperationKind.DeconstructionAssignment or OperationKind.NameOf or OperationKind.SwitchExpression or
        OperationKind.SwitchExpressionArm or OperationKind.ObjectOrCollectionInitializer or OperationKind.MemberInitializer;

    private void CheckCallArguments(IMethodSymbol method, System.Collections.Immutable.ImmutableArray<IArgumentOperation> arguments, ITypeSymbol? receiverType)
    {
        bool regex = method.ContainingType is { } containing &&
                     TypeKey(containing.OriginalDefinition) == "System.Text.RegularExpressions.Regex";

        foreach (IArgumentOperation argument in arguments)
        {
            if (argument.Parameter is not { } parameter)
                continue;

            if (regex && parameter.Name == "matchTimeout" && argument.Syntax is ArgumentSyntax { Expression: var timeout })
                report.MatchTimeoutArguments.Add(timeout.Span);

            // The format of a ToString call. A parse method reads its format and makes no text from it.
            if (parameter.Name == "format" && parameter.Type.SpecialType == SpecialType.System_String && method.Name == "ToString")
                CheckFormat(argument.Value, receiverType ?? method.ContainingType, argument.Syntax);
        }
    }

    private void CheckConversion(IConversionOperation conversion)
    {
        SyntaxNode where = conversion.Syntax;
        ITypeSymbol? from = conversion.Operand.Type;
        ITypeSymbol? to = conversion.Type;

        CheckOptional(conversion.OperatorMethod, where);
        CheckOptionalType(conversion.ConstrainedToType, where);

        if (to is null)
            return;

        Conversion kind = conversion.GetConversion();

        // "as" is a cast that answers null instead of throwing.
        if ((kind.IsExplicit || (conversion.IsTryCast && !kind.IsImplicit)) && to.TypeKind == TypeKind.Interface)
            Refuse(where, $"a cast to the interface {Display(to)} is not allowed");

        if (from is not null && IsExceptionType(from) && !IsExceptionType(to))
            Refuse(where, $"an exception cannot be converted to {Display(to)}; read its Message");

        // A match collection runs one match for each element it is asked for. As a sequence, a single
        // LINQ call would run them all with no budget check in between.
        if (from is INamedTypeSymbol source && to.TypeKind == TypeKind.Interface &&
            TypeKey(source.OriginalDefinition) == "System.Text.RegularExpressions.MatchCollection")
            Refuse(where, "the result of Matches can only be read with foreach");
    }

    private void CheckTypeTest(ITypeSymbol? tested, SyntaxNode where)
    {
        if (tested is null)
            return;

        CheckType(tested, where, TypeUse.Kept);

        if (tested.TypeKind == TypeKind.Interface)
            Refuse(where, $"a test for the interface {Display(tested)} is not allowed");
    }

    private void CheckInterpolationHole(IInterpolationOperation hole)
    {
        ITypeSymbol? type = hole.Expression.Type;

        if (type is not null && IsExceptionType(type))
            Refuse(hole.Syntax, "an exception cannot be put in a string; read its Message");

        if (hole.FormatString is not { } format)
            return;

        ITypeSymbol? formatted = type is INamedTypeSymbol { OriginalDefinition.SpecialType: SpecialType.System_Nullable_T } nullable
            ? nullable.TypeArguments[0]
            : type;

        if (formatted is not INamedTypeSymbol named || !FormattableTypes.Contains(TypeKey(named)))
        {
            Refuse(hole.Syntax, $"a format is allowed only for a number, a date, a time, a duration or a UUID, not for {Display(type)}");
            return;
        }

        CheckFormat(format, named, hole.Syntax);
    }

    /// <summary>
    /// The rule for a format text: a constant of at most
    /// <see cref="InlineFunctionSyntaxChecker.MaxFormatLength"/> characters. A standard format (one
    /// letter and a precision) may have a precision of at most two digits, because the precision is
    /// the length of the text it makes. A date format may not print the time zone: <c>z</c> and
    /// <c>U</c> read the zone of the machine for a time with no zone of its own.
    /// </summary>
    private void CheckFormat(IOperation format, ITypeSymbol? receiver, SyntaxNode where)
    {
        if (!format.ConstantValue.HasValue)
        {
            Refuse(where, "a format must be a constant text");
            return;
        }

        if (format.ConstantValue.Value is not string text)
            return;

        if (text.Length > InlineFunctionSyntaxChecker.MaxFormatLength)
        {
            Refuse(where, $"a format of more than {InlineFunctionSyntaxChecker.MaxFormatLength} characters is not allowed");
            return;
        }

        if (text.Length > 3 && char.IsAsciiLetter(text[0]) && text.AsSpan(1).IndexOfAnyExceptInRange('0', '9') < 0)
            Refuse(where, $"the format '{text}' has a precision of more than two digits");

        if (receiver is INamedTypeSymbol named && TypeKey(named.OriginalDefinition) == "System.DateTime" &&
            (text == "U" || text.Contains('z')))
            Refuse(where, $"the date format '{text}' prints the time zone of the machine, which differs between nodes");
    }

    private void CheckArraySizes(IArrayCreationOperation array)
    {
        if (array.DimensionSizes.Length < 2)
            return;

        long elements = 1;

        foreach (IOperation size in array.DimensionSizes)
        {
            if (size.ConstantValue is not { HasValue: true, Value: int length } || length < 0)
            {
                Refuse(array.Syntax, "an array with more than one dimension needs constant sizes");
                return;
            }

            elements = Math.Min(long.MaxValue / int.MaxValue, elements) * length;
        }

        if (arrayLimit > 0 && elements > arrayLimit)
            Refuse(array.Syntax, $"an array of {elements} elements is larger than the limit of {arrayLimit} elements");
    }

    private void RefuseDelegateArithmetic(ITypeSymbol? operand, SyntaxNode where)
    {
        if (operand?.TypeKind == TypeKind.Delegate && where is BinaryExpressionSyntax or AssignmentExpressionSyntax &&
            (where.IsKind(SyntaxKind.AddExpression) || where.IsKind(SyntaxKind.SubtractExpression) ||
             where.IsKind(SyntaxKind.AddAssignmentExpression) || where.IsKind(SyntaxKind.SubtractAssignmentExpression)))
            Refuse(where, "delegates cannot be combined with + or -");
    }

    private static bool IsWritten(IOperation reference) => reference.Parent switch
    {
        ISimpleAssignmentOperation assignment => ReferenceEquals(assignment.Target, reference),
        ICompoundAssignmentOperation compound => ReferenceEquals(compound.Target, reference),
        ICoalesceAssignmentOperation coalesce => ReferenceEquals(coalesce.Target, reference),
        IIncrementOrDecrementOperation => true,
        IDeconstructionAssignmentOperation => true,
        ITupleOperation tuple => IsWritten(tuple),
        IArgumentOperation argument => argument.Parameter?.RefKind is RefKind.Out or RefKind.Ref,
        IMemberInitializerOperation initializer => ReferenceEquals(initializer.InitializedMember, reference),
        _ => false,
    };

    // ─── Symbols ─────────────────────────────────────────────────────────────────────────────────

    private void CheckSymbol(ISymbol symbol, SyntaxNode where, bool write)
    {
        switch (symbol)
        {
            case ILocalSymbol local:
                if (!IsInBody(local.ContainingSymbol))
                    Refuse(where, $"'{local.Name}' is not a local of the body");

                break;

            case IParameterSymbol parameter:
                if (!IsInBody(parameter.ContainingSymbol))
                    Refuse(where, $"'{parameter.Name}' is not a parameter of the function");

                break;

            case IDiscardSymbol or INamespaceSymbol or ILabelSymbol or IRangeVariableSymbol:
                break;

            case IAliasSymbol alias:
                CheckSymbol(alias.Target, where, write);
                break;

            case ITypeSymbol type:
                CheckType(type, where, TypeUse.Kept);
                break;

            case IMethodSymbol method:
                CheckMethod(method, where);
                break;

            case IPropertySymbol property:
                CheckMember(property, property.MetadataName + (write ? "=" : ""), property.OriginalDefinition.Parameters, property.OriginalDefinition.Type, where);
                CheckTypeArguments(property.ContainingType, where);
                break;

            case IFieldSymbol field:
                CheckMember(field, field.CorrespondingTupleField?.Name ?? field.Name, default, field.OriginalDefinition.Type, where);
                CheckTypeArguments(field.ContainingType, where);
                break;

            default:
                Refuse(where, $"'{symbol.Name}' is not allowed");
                break;
        }
    }

    private void CheckMethod(IMethodSymbol method, SyntaxNode where)
    {
        switch (method.MethodKind)
        {
            case MethodKind.LocalFunction or MethodKind.AnonymousFunction:
                if (!IsInBody(method))
                    Refuse(where, $"'{method.Name}' is not a function of the body");

                return;

            // The operators of the language on the numeric types, bool, char, string and enums.
            case MethodKind.BuiltinOperator:
                return;
        }

        if (SymbolEqualityComparer.Default.Equals(method.ContainingAssembly, ownAssembly))
        {
            // The function stubs and the row accessors of a trigger class are the server's.
            if (IsGenerated(method))
            {
                CheckTypeArguments(method.ContainingType, where);
                return;
            }

            Refuse(where, "a body cannot call the function it defines; use a local function for a recursion");

            return;
        }

        IMethodSymbol target = method.ReducedFrom ?? method;

        // An accessor reached as a method is the property it belongs to.
        if (target.AssociatedSymbol is IPropertySymbol property)
        {
            CheckSymbol(property, where, write: target.MethodKind == MethodKind.PropertySet);
            return;
        }

        IMethodSymbol definition = target.OriginalDefinition;
        CheckMember(target, definition.Name, definition.Parameters, definition.ReturnType, where);

        foreach (ITypeSymbol argument in target.TypeArguments)
            CheckTypeArgument(argument, where);

        CheckTypeArguments(target.ContainingType, where);

        if (target.ContainingType is { } containing && InlineFunctionAllowList.TryGetRule(TypeKey(containing.OriginalDefinition), kind, out InlineFunctionAllowList.TypeRule? rule))
        {
            if (rule.RequiresMatchTimeout && (target.IsStatic || target.MethodKind == MethodKind.Constructor) &&
                MatchTimeoutMembers.Contains(definition.Name) && !HasParameter(definition, "matchTimeout"))
                Refuse(where, $"{Display(containing)}.{DisplayName(definition)} needs a match timeout: use the overload with a matchTimeout parameter");

            if (rule.IsException)
            {
                foreach (IParameterSymbol parameter in definition.Parameters)
                {
                    if (IsExceptionType(parameter.Type))
                        Refuse(where, "an exception cannot hold another exception");
                }
            }
        }
    }

    private static bool HasParameter(IMethodSymbol method, string name)
    {
        foreach (IParameterSymbol parameter in method.Parameters)
        {
            if (parameter.Name == name)
                return true;
        }

        return false;
    }

    /// <summary>
    /// The list check of one member: its declaring type is listed, the list allows its key or its
    /// signature, and every type in its definition is listed.
    /// </summary>
    private void CheckMember(
        ISymbol member, string key, System.Collections.Immutable.ImmutableArray<IParameterSymbol> parameters, ITypeSymbol? resultType, SyntaxNode where)
    {
        INamedTypeSymbol? containing = member.ContainingType?.OriginalDefinition;

        if (containing is null)
        {
            Refuse(where, $"'{member.Name}' is not allowed");
            return;
        }

        string typeName = TypeKey(containing);
        string shown = Display(member.ContainingType) + "." + DisplayName(member);

        if (InlineFunctionAllowList.DeniedEverywhere.Contains(member.Name))
        {
            Refuse(where, $"{shown} is not allowed");
            return;
        }

        if (SymbolEqualityComparer.Default.Equals(containing.ContainingAssembly, ownAssembly))
        {
            if (IsGenerated(member))
                return;

            Refuse(where, $"{shown} is not allowed: a body can use its own locals, parameters and local functions only");
            return;
        }

        // The same text as the type check gives, so one use of an unlisted type is one finding.
        if (!InlineFunctionAllowList.TryGetRule(typeName, kind, out InlineFunctionAllowList.TypeRule? rule))
        {
            Refuse(where, $"the type {Display(member.ContainingType)} is not on the list of allowed types");
            return;
        }

        string signature = Signature(key, parameters);

        if (!InlineFunctionAllowList.Allows(rule, key, signature))
        {
            Refuse(where, $"{shown} is not allowed");
            return;
        }

        if (!parameters.IsDefaultOrEmpty)
        {
            foreach (IParameterSymbol parameter in parameters)
            {
                if (!IsListedSignatureType(parameter.Type))
                {
                    Refuse(where, $"{shown} is not allowed: its parameter '{parameter.Name}' has the type {Display(parameter.Type)}");
                    return;
                }
            }
        }

        if (resultType is not null && !IsListedSignatureType(resultType))
            Refuse(where, $"{shown} is not allowed: it gives a value of the type {Display(resultType)}");
    }

    private bool IsInBody(ISymbol? symbol)
    {
        for (ISymbol? current = symbol; current is not null; current = current.ContainingSymbol)
        {
            if (SymbolEqualityComparer.Default.Equals(current, bodyMethod))
                return true;

            if (current is not IMethodSymbol { MethodKind: MethodKind.LocalFunction or MethodKind.AnonymousFunction })
                return false;
        }

        return false;
    }

    // ─── Types ───────────────────────────────────────────────────────────────────────────────────

    private void CheckOptionalType(ITypeSymbol? type, SyntaxNode where)
    {
        if (type is not null)
            CheckType(type, where, TypeUse.Value);
    }

    private void CheckType(ITypeSymbol? type, SyntaxNode where, TypeUse use)
    {
        if (type is null)
            return;

        // First, and alone: a type over these limits is not shown in a message and not walked.
        if (!CheckShape(type, where))
            return;

        if (use == TypeUse.Kept && IsLazySequence(type))
        {
            Refuse(where, $"a lazy sequence ({Display(type)}) cannot be kept; end the expression with ToList() or ToArray()");
            return;
        }

        CheckTypeParts(type, where, use, depth: 0);
    }

    /// <summary>
    /// Refuses a type that is too large, too deep, or too large as one value. See
    /// <see cref="InlineFunctionTypeShape"/> for why: such a type overflows the stack when the body
    /// runs, and no budget check can stop that.
    /// </summary>
    private bool CheckShape(ITypeSymbol type, SyntaxNode where)
    {
        InlineFunctionTypeShape.Shape shape = shapes.Of(type);

        if (shape.IsWithinLimits)
            return true;

        if (shape.Depth > InlineFunctionTypeShape.MaxDepth)
            Refuse(where, "the type is nested too deeply");
        else if (shape.Names > InlineFunctionTypeShape.MaxNames)
            Refuse(where, $"the type is too large: it holds more than {InlineFunctionTypeShape.MaxNames} type names");
        else
        {
            Refuse(where,
                $"a value of this type is larger than {InlineFunctionTypeShape.MaxValueBytes} bytes, which can overflow the stack; " +
                "keep its parts in a list, an array or a dictionary");
        }

        return false;
    }

    private void CheckTypeParts(ITypeSymbol type, SyntaxNode where, TypeUse use, int depth)
    {
        // A type as deep as this does not come from a body that passed the syntax depth check.
        if (depth > InlineFunctionSyntaxChecker.MaxDepth)
        {
            Refuse(where, "the type is nested too deeply");
            return;
        }

        switch (type)
        {
            case IArrayTypeSymbol array:
                CheckElementType(array.ElementType, where, use, depth);
                return;

            // Only a framework definition declares one: a generic local function is refused.
            case ITypeParameterSymbol:
                return;

            case INamedTypeSymbol { SpecialType: SpecialType.System_Void }:
                return;


            case IPointerTypeSymbol or IFunctionPointerTypeSymbol:
                Refuse(where, "pointers are not allowed");
                return;

            case IDynamicTypeSymbol:
                Refuse(where, "dynamic is not allowed");
                return;

            case IErrorTypeSymbol:
                Refuse(where, $"the type {Display(type)} is not known");
                return;

            case INamedTypeSymbol named:
                if (named.IsAnonymousType || SymbolEqualityComparer.Default.Equals(named.ContainingAssembly, ownAssembly))
                {
                    if (!named.IsAnonymousType && IsGenerated(named))
                        return;

                    Refuse(where, $"the type {Display(type)} is not allowed");
                    return;
                }

                if (!InlineFunctionAllowList.IsListed(TypeKey(named.OriginalDefinition), kind))
                {
                    Refuse(where, $"the type {Display(type)} is not on the list of allowed types");
                    return;
                }

                for (INamedTypeSymbol? current = named; current is not null; current = current.ContainingType)
                {
                    foreach (ITypeSymbol argument in current.TypeArguments)
                        CheckElementType(argument, where, use, depth);
                }

                return;

            default:
                Refuse(where, $"the type {Display(type)} is not allowed");
                return;
        }
    }

    /// <summary>A type inside another type: an array element, a type argument, a tuple element.</summary>
    private void CheckElementType(ITypeSymbol element, SyntaxNode where, TypeUse use, int depth)
    {
        if (element.SpecialType == SpecialType.System_Object)
        {
            Refuse(where, "object cannot be an element type or a type argument; use the exact type");
            return;
        }

        if (use != TypeUse.Signature && IsExceptionType(element))
        {
            Refuse(where, "an exception cannot be an element or a type argument");
            return;
        }

        if (use is TypeUse.Kept or TypeUse.LambdaParameter && IsLazySequence(element))
        {
            Refuse(where, $"a lazy sequence ({Display(element)}) cannot be an element or a type argument; end the expression with ToList() or ToArray()");
            return;
        }

        CheckTypeParts(element, where, use, depth + 1);
    }

    /// <summary>The type arguments of a generic type whose member the body uses.</summary>
    private void CheckTypeArguments(INamedTypeSymbol? type, SyntaxNode where)
    {
        for (INamedTypeSymbol? current = type; current is not null; current = current.ContainingType)
        {
            foreach (ITypeSymbol argument in current.TypeArguments)
                CheckTypeArgument(argument, where);
        }
    }

    private void CheckTypeArgument(ITypeSymbol argument, SyntaxNode where)
    {
        if (argument is ITypeParameterSymbol)
            return;

        CheckElementType(argument, where, TypeUse.Kept, depth: 0);
    }

    /// <summary>
    /// True when every type in <paramref name="type"/>, a type of a member's definition, is listed.
    /// A type parameter passes: the arguments given for it are checked where the member is used.
    /// </summary>
    private bool IsListedSignatureType(ITypeSymbol type) => type switch
    {
        IArrayTypeSymbol array => array.ElementType.SpecialType != SpecialType.System_Object && IsListedSignatureType(array.ElementType),
        ITypeParameterSymbol => true,
        INamedTypeSymbol named => IsListedNamedSignatureType(named),
        _ => false,
    };

    private bool IsListedNamedSignatureType(INamedTypeSymbol named)
    {
        if (IsGenerated(named))
            return true;

        if (!InlineFunctionAllowList.IsListed(TypeKey(named.OriginalDefinition), kind))
            return false;

        for (INamedTypeSymbol? current = named; current is not null; current = current.ContainingType)
        {
            foreach (ITypeSymbol argument in current.TypeArguments)
            {
                if (argument.SpecialType == SpecialType.System_Object || !IsListedSignatureType(argument))
                    return false;
            }
        }

        return true;
    }

    private static bool IsLazySequence(ITypeSymbol type) =>
        type is INamedTypeSymbol named && InlineFunctionAllowList.LazySequenceTypes.Contains(TypeKey(named.OriginalDefinition));

    private static bool IsExceptionType(ITypeSymbol? type)
    {
        for (ITypeSymbol? current = type; current is not null; current = current.BaseType)
        {
            if (current is INamedTypeSymbol { Name: "Exception", ContainingNamespace: { Name: "System", ContainingNamespace.IsGlobalNamespace: true } })
                return true;
        }

        return false;
    }

    // ─── Names ───────────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// The full metadata name of a type definition, the key of <see cref="InlineFunctionAllowList"/>:
    /// <c>System.Collections.Generic.Dictionary`2+KeyCollection</c>.
    /// </summary>
    internal static string TypeKey(INamedTypeSymbol type)
    {
        if (type.ContainingType is { } outer)
            return TypeKey(outer) + "+" + type.MetadataName;

        INamespaceSymbol? space = type.ContainingNamespace;
        return space is null || space.IsGlobalNamespace ? type.MetadataName : space.ToDisplayString() + "." + type.MetadataName;
    }

    /// <summary>
    /// A member key with the parameter types of the definition: <c>.ctor(char,int)</c>,
    /// <c>Join(string,object[])</c>, <c>.ctor(int,IEqualityComparer&lt;TKey&gt;)</c>.
    /// </summary>
    internal static string Signature(string key, System.Collections.Immutable.ImmutableArray<IParameterSymbol> parameters)
    {
        StringBuilder text = new(key.Length + 16);
        text.Append(key).Append('(');

        if (!parameters.IsDefaultOrEmpty)
        {
            for (int i = 0; i < parameters.Length; i++)
            {
                if (i > 0)
                    text.Append(',');

                AppendShortName(text, parameters[i].Type);
            }
        }

        return text.Append(')').ToString();
    }

    private static void AppendShortName(StringBuilder text, ITypeSymbol type)
    {
        switch (type)
        {
            case IArrayTypeSymbol array:
                AppendShortName(text, array.ElementType);
                text.Append('[').Append(',', array.Rank - 1).Append(']');
                return;

            case IPointerTypeSymbol pointer:
                AppendShortName(text, pointer.PointedAtType);
                text.Append('*');
                return;

            // By position, because the image check reads a signature from metadata, which has no
            // names: !0 is the first type parameter of the type, !!0 of the method.
            case ITypeParameterSymbol parameter:
                text.Append(parameter.TypeParameterKind == TypeParameterKind.Method ? "!!" : "!").Append(parameter.Ordinal);
                return;


            case INamedTypeSymbol named:
                string? keyword = named.SpecialType switch
                {
                    SpecialType.System_Object => "object",
                    SpecialType.System_String => "string",
                    SpecialType.System_Boolean => "bool",
                    SpecialType.System_Char => "char",
                    SpecialType.System_Byte => "byte",
                    SpecialType.System_SByte => "sbyte",
                    SpecialType.System_Int16 => "short",
                    SpecialType.System_UInt16 => "ushort",
                    SpecialType.System_Int32 => "int",
                    SpecialType.System_UInt32 => "uint",
                    SpecialType.System_Int64 => "long",
                    SpecialType.System_UInt64 => "ulong",
                    SpecialType.System_Single => "float",
                    SpecialType.System_Double => "double",
                    SpecialType.System_Decimal => "decimal",
                    _ => null,
                };

                if (keyword is not null)
                {
                    text.Append(keyword);
                    return;
                }

                text.Append(named.Name);

                if (named.TypeArguments.Length > 0)
                {
                    text.Append('<');

                    for (int i = 0; i < named.TypeArguments.Length; i++)
                    {
                        if (i > 0)
                            text.Append(',');

                        AppendShortName(text, named.TypeArguments[i]);
                    }

                    text.Append('>');
                }

                return;

            default:
                text.Append(type.Name);
                return;
        }
    }

    private static string Display(ITypeSymbol? type) => type?.ToDisplayString(DisplayFormat) ?? "this type";

    private static string DisplayName(ISymbol member) => member switch
    {
        IMethodSymbol { MethodKind: MethodKind.Constructor } => "new",
        IPropertySymbol { IsIndexer: true } => "this[]",
        _ => member.Name,
    };

    /// <summary>
    /// True for a symbol the server wrote into the class of a trigger body: the class itself and its
    /// nested row classes, and their members apart from the two methods that run the body and the
    /// constructors. The body cannot declare a member, so everything else of the class is the server's.
    /// A function body has no such members: its class holds the two methods only.
    /// </summary>
    private bool IsGenerated(ISymbol symbol)
    {
        if (kind != InlineUnitKind.Trigger)
            return false;

        INamedTypeSymbol? type = symbol as INamedTypeSymbol ?? symbol.ContainingType;

        for (INamedTypeSymbol? current = type?.OriginalDefinition; current is not null; current = current.ContainingType)
        {
            if (!SymbolEqualityComparer.Default.Equals(current, wrapper.OriginalDefinition))
                continue;

            if (symbol is IMethodSymbol method &&
                (method.MethodKind == MethodKind.Constructor || method.Name is "Body" or "Run"))
                return false;

            return true;
        }

        return false;
    }

    private void Refuse(SyntaxNode where, string message)
    {

        LinePosition start = where.SyntaxTree.GetLineSpan(where.Span).StartLinePosition;
        int line = Math.Max(1, start.Line - bodyStartLine);
        int column = start.Character + 1;

        // A message can show a type or a member, and a type can be long.
        if (message.Length > MaxMessageLength)
            message = string.Concat(message.AsSpan(0, MaxMessageLength), "...");

        // Both walks find most problems, at the start of the expression and at the name. One finding
        // for each problem on a line is enough.
        if (!reported.Add((line, message)))
            return;



        if (report.Findings.Count >= MaxFindings)
        {
            suppressed++;
            return;
        }

        report.Findings.Add(new InlineFunctionFinding(line, column, message));
    }
}

#endif
