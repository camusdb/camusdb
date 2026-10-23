/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections;
using System.Linq.Expressions;
using System.Reflection;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Extensions;

namespace CamusDB.Core.Extensions;

/// <summary>
/// Turns a <see cref="CamusTableFunctionAttribute"/> method into an <see cref="ExtensionTableFunction"/>:
/// a <see cref="TableFunction"/> that the leaf scanner streams like <c>generate_series</c>.
///
/// <para><b>One compiled call, one typed wrapper.</b> The arguments are converted and the user method
/// is called by a delegate compiled once at startup, with the same parameter rules and NULL guards as
/// a scalar function (<see cref="ExtensionAdapterParts"/>). Each yielded value is converted by a typed
/// <c>Func&lt;T, ColumnValue&gt;</c>, so the row path holds no reflection.</para>
///
/// <para><b>Strict.</b> A NULL argument for a parameter that does not accept NULL gives zero rows
/// without calling the method, as <c>generate_series</c> does. A <c>null</c> enumerable is zero rows
/// too.</para>
/// </summary>
internal static class ExtensionTableFunctionBinder
{
    private static readonly MethodInfo FailedFactory =
        typeof(ExtensionTableFunction).GetMethod(nameof(ExtensionTableFunction.Failed), BindingFlags.NonPublic | BindingFlags.Static)!;

    public static ExtensionTableFunction Bind(MethodInfo method, CamusTableFunctionAttribute attribute)
    {
        string where = method.DeclaringType?.FullName + "." + method.Name;

        if (!method.IsStatic || !method.IsPublic || method.ContainsGenericParameters)
            throw ExtensionFunctionBinder.InvalidSignature(where, "a [CamusTableFunction] method must be public, static and not generic");

        if (string.IsNullOrWhiteSpace(attribute.Name))
            throw ExtensionFunctionBinder.InvalidSignature(where, "the function name is empty");

        if (attribute.Column is { } column && !ExtensionLoader.IsExtensionName(column))
            throw ExtensionFunctionBinder.InvalidSignature(where, $"the column name '{column}' is not a plain identifier");

        Type? element = method.ReturnType.IsGenericType && method.ReturnType.GetGenericTypeDefinition() == typeof(IEnumerable<>)
            ? method.ReturnType.GetGenericArguments()[0]
            : null;

        if (element is null)
            throw ExtensionFunctionBinder.InvalidSignature(where, $"a table function returns IEnumerable<T>, not {method.ReturnType}");

        if (!ExtensionValueMapping.IsMappedType(element))
            throw ExtensionFunctionBinder.InvalidSignature(where, "the output element " + ExtensionValueMapping.DescribeUnmapped(element));

        ParameterInfo[] parameters = method.GetParameters();
        int minArity = ExtensionAdapterParts.CheckParameters(parameters, 0, where);

        MethodInfo? countRows = null;

        if (attribute.CountRowsMethod is { Length: > 0 } countName)
        {
            countRows = method.DeclaringType!.GetMethod(
                countName,
                BindingFlags.Public | BindingFlags.Static,
                null,
                parameters.Select(p => p.ParameterType).ToArray(),
                null);

            if (countRows is null || countRows.ReturnType != typeof(long?))
                throw ExtensionFunctionBinder.InvalidSignature(
                    where, $"CountRowsMethod '{countName}' must be a public static method of the same class with the same parameters, returning long?");
        }

        Type functionType = typeof(ExtensionTableFunction<>).MakeGenericType(element);

        Delegate call = Compile(method, parameters, minArity, typeof(IEnumerable<>).MakeGenericType(element));
        Delegate? count = countRows is null ? null : Compile(countRows, parameters, minArity, typeof(long?));

        string signature = ExtensionFunctionBinder.Signature(attribute.Name, parameters, 0, "SETOF " + ExtensionValueMapping.SqlTypeName(element));

        return (ExtensionTableFunction)Activator.CreateInstance(
            functionType,
            BindingFlags.NonPublic | BindingFlags.Instance,
            null,
            [attribute, minArity, parameters.Length, call, count, signature],
            null)!;
    }

    /// <summary>
    /// <code>
    /// (calledName, args) =>
    /// {
    ///     if (args[i] is NULL) return null;        // for each non-nullable parameter i: zero rows
    ///     Arg0 a0 = ToArg0(args[0], calledName, 0);
    ///     R result;
    ///     try { result = Method(a0); }
    ///     catch (OperationCanceledException) { throw; }
    ///     catch (Exception ex) { throw Failed(calledName, ex); }
    ///     return result;
    /// }
    /// </code>
    /// The same shape serves the count method, whose <c>null</c> means "unknown".
    /// </summary>
    private static Delegate Compile(MethodInfo method, ParameterInfo[] parameters, int minArity, Type resultType)
    {
        ParameterExpression calledName = Expression.Parameter(typeof(string), "calledName");
        ParameterExpression arguments = Expression.Parameter(typeof(IReadOnlyList<ColumnValue>), "arguments");
        LabelTarget returnLabel = Expression.Label(resultType, "return");

        List<ParameterExpression> locals = new(parameters.Length + 1);
        List<Expression> body = new(parameters.Length * 2 + 3);

        ExtensionAdapterParts.EmitArguments(
            parameters, 0, minArity, calledName, arguments,
            Expression.Return(returnLabel, Expression.Default(resultType)), locals, body);

        ParameterExpression result = Expression.Variable(resultType, "result");
        List<ParameterExpression> callArguments = [.. locals];
        locals.Add(result);

        body.Add(ExtensionAdapterParts.GuardUserCall(
            Expression.Assign(result, Expression.Call(method, callArguments)),
            caught => Expression.Call(FailedFactory, calledName, caught)));

        body.Add(Expression.Label(returnLabel, result));

        Type delegateType = typeof(Func<,,>).MakeGenericType(typeof(string), typeof(IReadOnlyList<ColumnValue>), resultType);

        return Expression.Lambda(delegateType, Expression.Block(resultType, locals, body), calledName, arguments).Compile();
    }
}

/// <summary>
/// A table function that a C# extension registered. Non-generic so the engine can hold and resolve it
/// beside the built-ins; <see cref="ExtensionTableFunction{T}"/> does the typed work.
/// </summary>
internal abstract class ExtensionTableFunction : TableFunction
{
    protected ExtensionTableFunction(CamusTableFunctionAttribute attribute, int minArguments, int maxArguments, string signature)
    {
        Name = attribute.Name;
        DefaultColumnName = attribute.Column;
        IsVolatile = attribute.IsVolatile;
        MinArguments = minArguments;
        MaxArguments = maxArguments;
        Signature = signature;
    }

    public override string Name { get; }

    public override int MinArguments { get; }

    public override int MaxArguments { get; }

    public override string? DefaultColumnName { get; }

    /// <summary>True when the output can differ between calls; the leaf scan then spools it per statement.</summary>
    public override bool IsVolatile { get; }

    /// <summary>The value a cancellation check happens after: every value, because a user value can take long.</summary>
    public override int CancellationCheckInterval => 1;

    /// <summary>The SQL signature for <c>SHOW EXTENSIONS</c>.</summary>
    public string Signature { get; }

    /// <summary>Called from compiled adapters and from the enumerator when user code throws.</summary>
    internal static CamusDBException Failed(string function, Exception inner) =>
        new(CamusDBErrorCodes.ExtensionFunctionFailed, $"Table function '{function}' failed: {inner.Message}", inner);
}

/// <summary>The typed half of <see cref="ExtensionTableFunction"/>.</summary>
internal sealed class ExtensionTableFunction<T> : ExtensionTableFunction
{
    private readonly Func<string, IReadOnlyList<ColumnValue>, IEnumerable<T>?> call;

    private readonly Func<string, IReadOnlyList<ColumnValue>, long?>? count;

    private readonly Func<T, ColumnValue> wrap;

    private ExtensionTableFunction(
        CamusTableFunctionAttribute attribute,
        int minArguments,
        int maxArguments,
        Func<string, IReadOnlyList<ColumnValue>, IEnumerable<T>?> call,
        Func<string, IReadOnlyList<ColumnValue>, long?>? count,
        string signature)
        : base(attribute, minArguments, maxArguments, signature)
    {
        this.call = call;
        this.count = count;
        wrap = ExtensionAdapterParts.ResultConverter<T>();
        OutputType = ExtensionValueMapping.ColumnTypeOf(typeof(T));
        OutputElementType = ExtensionValueMapping.ArrayElementColumnTypeOf(typeof(T));
    }

    public override ColumnType OutputType { get; }

    public override ColumnType? OutputElementType { get; }

    /// <summary>
    /// Calls the user method at once, so an argument error surfaces before the first row, and returns a
    /// sequence whose enumerator wraps the user's: a failure in <c>MoveNext</c> becomes
    /// <see cref="CamusDBErrorCodes.ExtensionFunctionFailed"/>, and disposing it disposes the user's
    /// enumerator, which runs a <c>finally</c> in an iterator method.
    /// </summary>
    public override IEnumerable<ColumnValue> Generate(IReadOnlyList<ColumnValue> arguments)
    {
        IEnumerable<T>? values = call(Name, arguments);
        return values is null ? [] : new Values(this, values);
    }

    public override long? CountRows(IReadOnlyList<ColumnValue> arguments) => count?.Invoke(Name, arguments);

    private sealed class Values : IEnumerable<ColumnValue>
    {
        private readonly ExtensionTableFunction<T> function;

        private readonly IEnumerable<T> values;

        public Values(ExtensionTableFunction<T> function, IEnumerable<T> values)
        {
            this.function = function;
            this.values = values;
        }

        public IEnumerator<ColumnValue> GetEnumerator()
        {
            try
            {
                return new Enumerator(function, values.GetEnumerator());
            }
            catch (Exception ex) when (ex is not OperationCanceledException and not CamusDBException)
            {
                throw Failed(function.Name, ex);
            }
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    private sealed class Enumerator : IEnumerator<ColumnValue>
    {
        private readonly ExtensionTableFunction<T> function;

        private readonly IEnumerator<T> inner;

        private ColumnValue current = ColumnValue.Null;

        public Enumerator(ExtensionTableFunction<T> function, IEnumerator<T> inner)
        {
            this.function = function;
            this.inner = inner;
        }

        public ColumnValue Current => current;

        object IEnumerator.Current => current;

        public bool MoveNext()
        {
            T value;

            try
            {
                if (!inner.MoveNext())
                    return false;

                value = inner.Current;
            }
            catch (Exception ex) when (ex is not OperationCanceledException and not CamusDBException)
            {
                throw Failed(function.Name, ex);
            }

            current = function.wrap(value);
            return true;
        }

        public void Reset() => throw new NotSupportedException();

        public void Dispose()
        {
            try
            {
                inner.Dispose();
            }
            catch (Exception ex) when (ex is not OperationCanceledException and not CamusDBException)
            {
                throw Failed(function.Name, ex);
            }
        }
    }
}
