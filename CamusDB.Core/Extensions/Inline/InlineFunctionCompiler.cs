/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

#if !CAMUSDB_BROWSER

using System.Globalization;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using CamusDB.Core.Catalogs.Models;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.Extensions.Inline;


/// <summary>The compiled code of one inline function source, loaded and ready to bind.</summary>
/// <param name="Method">The public method the server binds: it calls the body and normalizes the result.</param>
/// <param name="Context">The collectible load context that holds the code.</param>
/// <param name="ImageBytes">The size of the compiled image, for diagnostics.</param>
internal sealed record CompiledInlineFunction(MethodInfo Method, InlineFunctionLoadContext Context, int ImageBytes);

/// <summary>
/// Compiles the body of an inline SQL function into loaded code. One call runs the whole pipeline, in
/// an order where each stage bounds the next:
///
/// <list type="number">
/// <item>Size and text checks, before the compiler reads a character.</item>
/// <item><see cref="InlineFunctionSyntaxChecker"/>: the body is one block, not too deep, with no
/// refused language form and with lambdas the compiler can bind in a bounded time.</item>
/// <item><see cref="InlineFunctionTypeGrowthChecker"/>: no inferred type can grow past what the
/// compiler can walk in a bounded time.</item>
/// <item>A first compile of the body inside a method the server writes, with no budget code and with
/// no reference to the budget assembly. Compiler errors end here, with positions in the body.</item>
/// <item><see cref="InlineFunctionSymbolChecker"/> on the semantic model of that compile: the allowed
/// list, and the limits on the real types (<see cref="InlineFunctionTypeShape"/>).</item>
/// <item><see cref="InlineFunctionBudgetRewriter"/>, then a second compile, which emits the image.</item>
/// <item><see cref="InlineFunctionImageVerifier"/> on the image, then the load into a collectible
/// <see cref="InlineFunctionLoadContext"/>.</item>
/// </list>
///
/// <para><b>The compiler is a target too.</b> A body is at most
/// <see cref="CamusDBOptions.InlineFunctionSourceLimitBytes"/> long and at most
/// <see cref="InlineFunctionSyntaxChecker.MaxDepth"/> deep, but short source can still make a compiler
/// work for hours: lambdas nested in calls of overloaded methods, and inferred types that double with
/// each line. The compiler looks at its cancellation token between stages and not inside them, so a
/// deadline alone does not end such a compile. Three things do, together. The two syntax checks above
/// refuse the bodies of both kinds before the compiler sees them. Every stage runs under one token
/// with a deadline, <see cref="CamusDBOptions.InlineFunctionCompileTimeoutMs"/>. And the pipeline runs
/// on a thread of its own that the caller stops waiting for at the deadline, under a semaphore that
/// lets <see cref="CamusDBOptions.InlineFunctionCompileConcurrency"/> compiles run at a time, each on
/// one core. A compile that times out fails its statement, never the node; one that does not even
/// stop keeps its slot, so the damage of a body that finds a new way to be slow has a ceiling.</para>

///
/// <para><b>Pinned options.</b> The language version is one fixed value, so a newer compiler in a
/// newer server build still reads a stored body the same way. It is C# 12 and not 13 for a reason:
/// from 13 on, a call such as <c>s.Split(',', ';')</c> binds to an overload that takes a
/// <c>params ReadOnlySpan</c>, and the code the compiler emits for that needs <c>Unsafe.As</c>, which
/// would then have to be on the list of the image check. Optimized, deterministic, no unsafe code,
/// nullable annotations on. The body is compiled as an ordinary method; the scripting API, with its
/// <c>#r</c> and <c>#load</c>, is never used.</para>
///
/// <para><b>References.</b> The compiler gets the framework assemblies that hold the listed types and
/// nothing else, so most of the framework does not even bind. The allowed list is still the boundary:
/// the core library alone holds file, reflection and process types.</para>
/// </summary>
internal sealed class InlineFunctionCompiler : IDisposable
{
    /// <summary>The language version every body is compiled with, and its name on a function record.</summary>
    public const LanguageVersion Language = LanguageVersion.CSharp12;

    /// <summary>The text form of <see cref="Language"/> that a function record stores.</summary>
    public const string LanguageName = nameof(LanguageVersion.CSharp12);

    private const string WrapperNamespace = "CamusDB.Inline";

    private const string BodyMethodName = "Body";

    private const string RunMethodName = "Run";

    private const int MaxDiagnostics = 20;

    private static readonly CSharpParseOptions ParseOptions = new(Language, DocumentationMode.None, SourceCodeKind.Regular);

    private static readonly CSharpCompilationOptions CompilationOptions = new(
        OutputKind.DynamicallyLinkedLibrary,
        optimizationLevel: OptimizationLevel.Release,
        allowUnsafe: false,
        deterministic: true,
        concurrentBuild: false,
        nullableContextOptions: NullableContextOptions.Enable,
        generalDiagnosticOption: ReportDiagnostic.Default);

    private static readonly Lazy<MetadataReference[]> FrameworkReferences = new(LoadFrameworkReferences);

    private static readonly Lazy<MetadataReference> BudgetReference =
        new(() => ReferenceTo(typeof(CamusDB.Extensions.Inline.CallBudget).Assembly));

    /// <summary>
    /// The stack of a compile thread. The compiler walks a type by recursion, once for each level of
    /// nesting, and a body can nest a type one level with each line: a few thousand levels in a body
    /// of the largest size. That overflows the stack of a pool thread, and a stack overflow ends the
    /// process. The memory is address space only; pages that are not touched are not used.
    /// </summary>
    private const int CompileStackBytes = 128 * 1024 * 1024;

    /// <summary>How long a caller waits past the time limit for the compile thread to see its cancelled token.</summary>
    private const int DefaultStopGraceMs = 2_000;

    private readonly int stopGraceMs;

    private readonly SemaphoreSlim slots;

    private readonly ILogger? logger;

    private int abandoned;

    private readonly int sourceLimitBytes;

    private readonly int timeoutMs;

    private readonly int arrayLimit;

    private long compilations;

    private int running;

    private int peakRunning;

    /// <param name="sourceLimitBytes">The longest body, in UTF-8 bytes.</param>
    /// <param name="compileTimeoutMs">The time one compile has, with its wait for a slot.</param>
    /// <param name="concurrency">How many compiles run at the same time.</param>
    /// <param name="arrayLimit">The most elements of an array that a body creates with constant sizes.</param>
    /// <param name="logger">Where a compile that did not stop is reported.</param>
    /// <param name="stopGraceMs">
    /// How long a caller waits past the time limit before it leaves the compile thread. Only a test
    /// passes a value: no body that the checks accept keeps the compiler busy for the default.
    /// </param>
    public InlineFunctionCompiler(
        int sourceLimitBytes, int compileTimeoutMs, int concurrency, int arrayLimit, ILogger? logger = null, int stopGraceMs = DefaultStopGraceMs)
    {
        this.logger = logger;
        this.stopGraceMs = Math.Max(0, stopGraceMs);
        this.sourceLimitBytes = sourceLimitBytes;
        this.arrayLimit = arrayLimit;
        timeoutMs = Math.Max(1, compileTimeoutMs);
        slots = new SemaphoreSlim(Math.Max(1, concurrency));
    }

    /// <summary>How many compiles this instance ran to the end or to an error. For tests and diagnostics.</summary>
    public long Compilations => Interlocked.Read(ref compilations);

    /// <summary>The most compiles that ran at the same time. For tests of the concurrency limit.</summary>
    public int PeakConcurrentCompilations => Volatile.Read(ref peakRunning);

    /// <summary>
    /// How many compiles a caller stopped waiting for because the compiler did not stop at the time
    /// limit. Each one held a compile slot until its thread ended. More than zero means that a body
    /// found work the checks before the compiler do not bound.
    /// </summary>
    public int AbandonedCompilations => Volatile.Read(ref abandoned);

    /// <summary>
    /// The identity of the code a function compiles to: the SHA-256 of its signature, its NULL rule and
    /// its body, with the language version and the rule revision. Two functions with the same key, in
    /// any database and under any name, share one compiled unit; the name is not part of the code.
    /// </summary>
    public static string UnitKey(InlineFunctionDefinition function)
    {
        StringBuilder text = new(function.Source.Length + 128);
        text.Append(LanguageName).Append('\n').Append(InlineFunctionAllowList.Revision.ToString(CultureInfo.InvariantCulture)).Append('\n');
        text.Append(function.NullInput == InlineFunctionNullInput.Called ? "called" : "strict").Append('\n');
        AppendSignature(text, function, BodyMethodName, parameterName: (parameter, _) => parameter.Name);
        text.Append('\n').Append(function.Source);

        return Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(text.ToString())));
    }

    /// <summary>The lowercase hex SHA-256 of the UTF-8 bytes of a body, as a function record stores it.</summary>
    public static string SourceHash(string source) => Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(source)));

    /// <summary>
    /// Compiles <paramref name="function"/> and loads the result. Throws
    /// <see cref="CamusDBErrorCodes.InlineFunctionRefused"/> for a refused body or signature and
    /// <see cref="CamusDBErrorCodes.InlineFunctionCompileFailed"/> for a compiler error or a timeout.
    /// The caller owns the returned context and unloads it.
    ///
    /// <para>The pipeline runs on a thread of its own and the caller waits for it. The thread has a
    /// stack large enough for the deepest type a body can make, and the wait has an end: the compiler
    /// looks at the cancellation token between its stages, not inside them, so a caller that only
    /// passed a token would wait for as long as the compiler works. A caller that stops waiting gets
    /// the timeout error; the thread keeps its compile slot until it ends, so runaway compiles can
    /// never use more cores than <see cref="CamusDBOptions.InlineFunctionCompileConcurrency"/>.</para>
    /// </summary>
    public CompiledInlineFunction Compile(InlineFunctionDefinition function, CancellationToken cancellation)
    {
        CheckSignature(function);
        CheckText(function);

        CancellationTokenSource deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
        deadline.CancelAfter(timeoutMs);

        try
        {
            slots.Wait(deadline.Token);
        }
        catch (OperationCanceledException)
        {
            deadline.Dispose();

            if (cancellation.IsCancellationRequested)
                throw;

            throw Busy(function);
        }

        // From here the slot and the deadline belong to the compile thread, which gives them back
        // when it ends, however late that is.
        CompileJob job = new(function, deadline);

        try
        {
            Thread thread = new(() => Run(job), CompileStackBytes) { IsBackground = true, Name = "camusdb-inline-compile" };
            thread.Start();
        }
        catch
        {
            deadline.Dispose();
            slots.Release();
            throw;
        }

        // The thread has its time limit, and then a little more to see the cancelled token.
        if (!job.Done.Wait(timeoutMs + stopGraceMs) && job.TryAbandon())

        {
            Interlocked.Increment(ref abandoned);

            logger?.LogError(
                "The compile of inline function {Function} (source {Hash}) did not stop at its limit of {Timeout} ms. Its thread still runs and " +
                "holds one compile slot until it ends; {Abandoned} compiles were left this way since the node started",
                function.Name, function.SourceSha256, timeoutMs, Volatile.Read(ref abandoned));

            throw new CamusDBException(
                CamusDBErrorCodes.InlineFunctionCompileFailed,
                $"Function '{function.Name}' did not compile within {timeoutMs} ms (inline_function_compile_timeout_ms) and the compiler " +
                "did not stop; make the body simpler");
        }

        job.Done.Wait();

        switch (job.Failure)
        {
            case null:
                return job.Result!;

            case OperationCanceledException when cancellation.IsCancellationRequested:
                throw new OperationCanceledException(cancellation);

            case OperationCanceledException:
                throw Busy(function);

            case InsufficientExecutionStackException:
                throw new CamusDBException(
                    CamusDBErrorCodes.InlineFunctionCompileFailed,
                    $"Function '{function.Name}' is too complex to compile: an expression is nested too deeply");

            default:
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(job.Failure).Throw();
                throw job.Failure;
        }
    }

    private InlineFunctionCompileBusyException Busy(InlineFunctionDefinition function) => new(
        $"Function '{function.Name}' did not compile within {timeoutMs} ms (inline_function_compile_timeout_ms); " +
        "make the body simpler, or try again when the node compiles fewer functions");

    /// <summary>The body of the compile thread. It never lets an exception leave: that would end the process.</summary>
    private void Run(CompileJob job)
    {
        int now = Interlocked.Increment(ref running);
        int peak;

        while (now > (peak = Volatile.Read(ref peakRunning)) && Interlocked.CompareExchange(ref peakRunning, now, peak) != peak)
        {
        }

        try
        {
            job.Result = CompileCore(job.Function, job.Deadline.Token);
        }
        catch (Exception failure)
        {
            job.Failure = failure;
        }
        finally
        {
            Interlocked.Decrement(ref running);
            Interlocked.Increment(ref compilations);
            job.Deadline.Dispose();

            try
            {
                slots.Release();
            }
            catch (ObjectDisposedException)
            {
                // The engine closed while this compile ran over its limit.
            }

            // Nobody waits for a compile that was left: its code has no owner, so unload it here.
            if (!job.TryComplete())
                job.Result?.Context.Unload();

            job.Done.Set();
        }
    }

    /// <summary>
    /// One compile on its own thread, and the hand-over between that thread and the caller that
    /// waits for it. Exactly one of the two wins: the thread completes the job, or the caller leaves
    /// it when the wait is over.
    /// </summary>
    private sealed class CompileJob(InlineFunctionDefinition function, CancellationTokenSource deadline)
    {
        private const int Running = 0, Completed = 1, Abandoned = 2;

        private int state;

        public InlineFunctionDefinition Function { get; } = function;

        public CancellationTokenSource Deadline { get; } = deadline;

        public ManualResetEventSlim Done { get; } = new(false);

        public CompiledInlineFunction? Result { get; set; }

        public Exception? Failure { get; set; }

        public bool TryComplete() => Interlocked.CompareExchange(ref state, Completed, Running) == Running;

        public bool TryAbandon() => Interlocked.CompareExchange(ref state, Abandoned, Running) == Running;
    }

    private CompiledInlineFunction CompileCore(InlineFunctionDefinition function, CancellationToken cancellation)
    {
        // ── 1. Syntax ────────────────────────────────────────────────────────────────────────────

        List<InlineFunctionFinding> errors = [];
        BlockSyntax? parsed = InlineFunctionSyntaxChecker.Parse(function.Source, ParseOptions, errors);

        if (parsed is null)
            throw CompileFailed(function, errors);

        if (InlineFunctionSyntaxChecker.MeasureDepth(parsed, InlineFunctionSyntaxChecker.MaxDepth) > InlineFunctionSyntaxChecker.MaxDepth)
            throw new CamusDBException(
                CamusDBErrorCodes.InlineFunctionRefused,
                $"Function '{function.Name}' is refused: the body is nested more than {InlineFunctionSyntaxChecker.MaxDepth} levels deep");

        List<InlineFunctionFinding> refused = InlineFunctionSyntaxChecker.Check(parsed);

        if (refused.Count > 0)
            throw Refused(function, refused);

        // The compiler cannot be stopped in the middle of these two kinds of work, so a body that
        // asks for too much of either does not reach it.
        if (InlineFunctionSyntaxChecker.CheckLambdaCost(parsed) is { } costly)
            throw Refused(function, [costly]);

        List<InlineFunctionFinding> grown = InlineFunctionTypeGrowthChecker.Check(parsed, function.Parameters.Select(parameter => parameter.Name));

        if (grown.Count > 0)
            throw Refused(function, grown);

        cancellation.ThrowIfCancellationRequested();

        // ── 2. First compile and the allowed list ────────────────────────────────────────────────

        string unit = UnitKey(function)[..16];
        (SyntaxTree tree, MethodDeclarationSyntax bodyMethod) = Graft(WrapperSource(function, unit, withRun: false), parsed);

        CSharpCompilation checkedCompilation = CSharpCompilation.Create(
            "camusdb-inline-check-" + unit, [tree], FrameworkReferences.Value, CompilationOptions);

        List<InlineFunctionFinding> compileErrors = ErrorsOf(checkedCompilation.GetDiagnostics(cancellation), bodyMethod);

        if (compileErrors.Count > 0)
            throw CompileFailed(function, compileErrors);

        InlineFunctionSymbolReport report = InlineFunctionSymbolChecker.Check(
            checkedCompilation.GetSemanticModel(tree), bodyMethod, arrayLimit, cancellation);

        if (report.Findings.Count > 0)
            throw Refused(function, report.Findings);

        // ── 3. Budget checks and the second compile ──────────────────────────────────────────────

        BlockSyntax budgeted = InlineFunctionBudgetRewriter.Rewrite(bodyMethod.Body!, report);
        (SyntaxTree finalTree, MethodDeclarationSyntax finalBody) = Graft(WrapperSource(function, unit, withRun: true), budgeted);

        CSharpCompilation compilation = CSharpCompilation.Create(
            "camusdb-inline-" + unit, [finalTree], [.. FrameworkReferences.Value, BudgetReference.Value], CompilationOptions);

        using MemoryStream stream = new();
        Microsoft.CodeAnalysis.Emit.EmitResult emitted = compilation.Emit(stream, cancellationToken: cancellation);

        if (!emitted.Success)
        {
            // The first compile had no error, so these come from the code the server added.
            List<InlineFunctionFinding> internalErrors = ErrorsOf(emitted.Diagnostics, finalBody);

            throw new CamusDBException(
                CamusDBErrorCodes.InlineFunctionCompileFailed,
                $"Function '{function.Name}' does not compile after the server added its budget checks. " +
                "This is an error of the server, not of the body: " + Join(internalErrors));
        }

        byte[] image = stream.ToArray();

        // ── 4. The image check and the load ──────────────────────────────────────────────────────

        IReadOnlyList<string> foreign = InlineFunctionImageVerifier.Verify(image);

        if (foreign.Count > 0)
            throw new CamusDBException(
                CamusDBErrorCodes.InlineFunctionRefused,
                $"Function '{function.Name}' is refused: its compiled code refers to {string.Join("; ", foreign)}. " +
                "The body passed the source check, so the compiler added these references for a language form the server does not accept yet");

        InlineFunctionLoadContext context = new(unit);

        try
        {
            Assembly assembly = context.LoadFromStream(new MemoryStream(image, writable: false));
            MethodInfo method = assembly.GetType(WrapperNamespace + ".Fn_" + unit, throwOnError: true)!
                .GetMethod(RunMethodName, BindingFlags.Public | BindingFlags.Static)!;

            return new CompiledInlineFunction(method, context, image.Length);
        }
        catch
        {
            context.Unload();
            throw;
        }
    }

    // ─── Checks before the compiler ──────────────────────────────────────────────────────────────

    private void CheckText(InlineFunctionDefinition function)
    {
        string source = function.Source;

        // Every character is at least one byte and at most three, so most bodies need no count.
        if (source.Length > sourceLimitBytes || (source.Length * 3L > sourceLimitBytes && Encoding.UTF8.GetByteCount(source) > sourceLimitBytes))
            throw new CamusDBException(
                CamusDBErrorCodes.InlineFunctionRefused,
                $"Function '{function.Name}' is refused: the body is longer than {sourceLimitBytes} bytes (inline_function_source_limit_bytes)");

        if (source.Contains('\0'))
            throw new CamusDBException(
                CamusDBErrorCodes.InlineFunctionRefused,
                $"Function '{function.Name}' is refused: the body holds a NUL character");

        if (function.Language != InlineFunctionDefinition.CSharp)
            throw new CamusDBException(
                CamusDBErrorCodes.InlineFunctionRefused,
                $"Function '{function.Name}' is refused: the language '{function.Language}' is not supported; the only language is csharp");

        if (function.LanguageVersion.Length > 0 && function.LanguageVersion != LanguageName)
            throw new CamusDBException(
                CamusDBErrorCodes.InlineFunctionRefused,
                $"Function '{function.Name}' was created for C# language version '{function.LanguageVersion}', and this node compiles '{LanguageName}'");
    }

    /// <summary>
    /// The parameter and result types are in the mapped set, and each parameter name is a plain C#
    /// identifier that the body can use: not a C# keyword, not reserved for the server, and not the
    /// same as another parameter in any letter case.
    /// </summary>
    private static void CheckSignature(InlineFunctionDefinition function)
    {
        if (!InlineFunctionSignature.IsAccepted(function.ReturnType, function.ReturnElementType))
            throw new CamusDBException(
                CamusDBErrorCodes.InlineFunctionRefused,
                $"Function '{function.Name}' is refused: the result type {InlineFunctionSignature.SqlTypeName(function.ReturnType, function.ReturnElementType)} " +
                $"is not accepted; the accepted types are {InlineFunctionSignature.AcceptedTypes}");

        HashSet<string> names = new(StringComparer.OrdinalIgnoreCase);

        foreach (InlineFunctionParameter parameter in function.Parameters)
        {
            if (!InlineFunctionSignature.IsAccepted(parameter.Type, parameter.ElementType))
                throw new CamusDBException(
                    CamusDBErrorCodes.InlineFunctionRefused,
                    $"Function '{function.Name}' is refused: parameter '{parameter.Name}' has the type " +
                    $"{InlineFunctionSignature.SqlTypeName(parameter.Type, parameter.ElementType)}, which is not accepted; " +
                    $"the accepted types are {InlineFunctionSignature.AcceptedTypes}");

            if (!SyntaxFacts.IsValidIdentifier(parameter.Name) || SyntaxFacts.GetKeywordKind(parameter.Name) != SyntaxKind.None ||
                parameter.Name.StartsWith(InlineFunctionSyntaxChecker.ReservedPrefix, StringComparison.Ordinal))
                throw new CamusDBException(
                    CamusDBErrorCodes.InlineFunctionRefused,
                    $"Function '{function.Name}' is refused: '{parameter.Name}' cannot be a parameter name, because the body could not name it in C#");

            if (!names.Add(parameter.Name))
                throw new CamusDBException(
                    CamusDBErrorCodes.InlineFunctionRefused,
                    $"Function '{function.Name}' is refused: two parameters are named '{parameter.Name}'");
        }
    }

    // ─── The method around the body ──────────────────────────────────────────────────────────────

    /// <summary>
    /// The source the server writes around a body: a static class with the method <c>Body</c>, whose
    /// block is empty here and is replaced with the parsed body, and, for the image, the public method
    /// <c>Run</c> that the server binds. <c>Run</c> has parameter names of its own, so no parameter
    /// name of the function can hide <c>Body</c> in it, and it marks a <c>DATETIME</c> result as UTC.
    /// </summary>
    private static string WrapperSource(InlineFunctionDefinition function, string unit, bool withRun)
    {
        StringBuilder text = new(512);

        text.Append("using System;\nusing System.Collections.Generic;\nusing System.Globalization;\nusing System.Linq;\n");
        text.Append("using System.Text;\nusing System.Text.RegularExpressions;\n\n");
        text.Append("namespace ").Append(WrapperNamespace).Append("\n{\n    public static class Fn_").Append(unit).Append("\n    {\n");

        if (withRun)
        {
            bool dates = function.ReturnType == ColumnType.DateTime || function.ReturnElementType == ColumnType.DateTime;

            text.Append("        public static ");
            AppendSignature(text, function, RunMethodName, (_, index) => RunParameter(index));
            text.Append(" => ").Append(dates ? "global::CamusDB.Extensions.Inline.InlineValues.Utc(" : "(").Append(BodyMethodName).Append('(');

            for (int i = 0; i < function.Parameters.Count; i++)
                text.Append(i > 0 ? ", " : "").Append(RunParameter(i));

            text.Append("));\n\n");
        }

        text.Append("        private static ");
        AppendSignature(text, function, BodyMethodName, (parameter, _) => parameter.Name);
        text.Append("\n        {\n        }\n    }\n}\n");

        return text.ToString();
    }

    private static string RunParameter(int index) =>
        InlineFunctionSyntaxChecker.ReservedPrefix + "_a" + index.ToString(CultureInfo.InvariantCulture);

    /// <summary>Appends <c>long? Name(string a, long b)</c>: the result is always nullable, the parameters only when the function is called on NULL.</summary>
    private static void AppendSignature(
        StringBuilder text, InlineFunctionDefinition function, string methodName, Func<InlineFunctionParameter, int, string> parameterName)
    {
        bool nullable = function.NullInput == InlineFunctionNullInput.Called;

        text.Append(InlineFunctionSignature.CSharpTypeName(function.ReturnType, function.ReturnElementType, nullable: true));
        text.Append(' ').Append(methodName).Append('(');

        for (int i = 0; i < function.Parameters.Count; i++)
        {
            InlineFunctionParameter parameter = function.Parameters[i];

            if (i > 0)
                text.Append(", ");

            text.Append(InlineFunctionSignature.CSharpTypeName(parameter.Type, parameter.ElementType, nullable));
            text.Append(' ').Append(parameterName(parameter, i));
        }

        text.Append(')');
    }

    /// <summary>
    /// Parses the server's source and puts <paramref name="body"/> in place of the empty block of the
    /// <c>Body</c> method, as a syntax node. The body's text is never joined to the source, so nothing
    /// in it can close the method and write beside it.
    /// </summary>
    private static (SyntaxTree Tree, MethodDeclarationSyntax BodyMethod) Graft(string wrapper, BlockSyntax body)
    {
        CompilationUnitSyntax root = (CompilationUnitSyntax)CSharpSyntaxTree.ParseText(wrapper, ParseOptions).GetRoot();
        MethodDeclarationSyntax empty = FindBodyMethod(root);

        // The opening brace on its own line, as in the parsed text: line 1 of the body is the line after it.
        BlockSyntax placed = body.WithLeadingTrivia(SyntaxFactory.ElasticCarriageReturnLineFeed).WithTrailingTrivia(SyntaxFactory.ElasticCarriageReturnLineFeed);
        CompilationUnitSyntax grafted = root.ReplaceNode(empty.Body!, placed);

        SyntaxTree tree = CSharpSyntaxTree.Create(grafted, ParseOptions, path: "", encoding: Encoding.UTF8);
        return (tree, FindBodyMethod((CompilationUnitSyntax)tree.GetRoot()));
    }

    private static MethodDeclarationSyntax FindBodyMethod(CompilationUnitSyntax root)
    {
        foreach (MemberDeclarationSyntax space in root.Members)
        {
            if (space is not BaseNamespaceDeclarationSyntax { Members: [ClassDeclarationSyntax type] })
                continue;

            foreach (MemberDeclarationSyntax member in type.Members)
            {
                if (member is MethodDeclarationSyntax method && method.Identifier.ValueText == BodyMethodName)
                    return method;
            }
        }

        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "The server-written source of an inline function has no body method");
    }

    // ─── Diagnostics ─────────────────────────────────────────────────────────────────────────────

    private static List<InlineFunctionFinding> ErrorsOf(IEnumerable<Diagnostic> diagnostics, MethodDeclarationSyntax bodyMethod)
    {
        List<InlineFunctionFinding> errors = [];
        SyntaxTree tree = bodyMethod.SyntaxTree;
        int bodyLine = tree.GetLineSpan(bodyMethod.Body!.OpenBraceToken.Span).StartLinePosition.Line;
        int total = 0;

        foreach (Diagnostic diagnostic in diagnostics)
        {
            if (diagnostic.Severity != DiagnosticSeverity.Error)
                continue;

            if (++total > MaxDiagnostics)
                continue;

            string message = diagnostic.Id + ": " + diagnostic.GetMessage(CultureInfo.InvariantCulture);

            if (diagnostic.Location.IsInSource)
            {
                LinePosition start = diagnostic.Location.GetLineSpan().StartLinePosition;
                errors.Add(new InlineFunctionFinding(Math.Max(1, start.Line - bodyLine), start.Character + 1, message));
            }
            else
                errors.Add(new InlineFunctionFinding(1, 1, message));
        }

        if (total > MaxDiagnostics)
            errors.Add(new InlineFunctionFinding(1, 1, $"and {total - MaxDiagnostics} more errors"));

        return errors;
    }

    private static CamusDBException CompileFailed(InlineFunctionDefinition function, List<InlineFunctionFinding> errors) =>
        new(CamusDBErrorCodes.InlineFunctionCompileFailed, $"Function '{function.Name}' does not compile: " + Join(errors));

    private static CamusDBException Refused(InlineFunctionDefinition function, List<InlineFunctionFinding> findings) =>
        new(CamusDBErrorCodes.InlineFunctionRefused, $"Function '{function.Name}' is refused: " + Join(findings));

    private static string Join(List<InlineFunctionFinding> findings) => string.Join("; ", findings);

    // ─── References ──────────────────────────────────────────────────────────────────────────────

    private static MetadataReference[] LoadFrameworkReferences() =>
    [
        ReferenceTo(typeof(object).Assembly),
        ReferenceTo(Assembly.Load("System.Runtime")),
        ReferenceTo(typeof(SortedSet<>).Assembly),
        ReferenceTo(typeof(Enumerable).Assembly),
        ReferenceTo(typeof(System.Text.RegularExpressions.Regex).Assembly),
    ];

    private static MetadataReference ReferenceTo(Assembly assembly)
    {
        // The server is published as files on disk. A single-file build would have no path here.
        if (string.IsNullOrEmpty(assembly.Location))
            throw new CamusDBException(
                CamusDBErrorCodes.InlineFunctionCompileFailed,
                $"Inline functions cannot be compiled in this deployment: the assembly {assembly.GetName().Name} is not a file on disk");

        return MetadataReference.CreateFromFile(assembly.Location);
    }

    public void Dispose() => slots.Dispose();
}

#endif
