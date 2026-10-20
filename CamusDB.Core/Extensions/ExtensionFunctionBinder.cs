/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Linq.Expressions;
using System.Reflection;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Extensions;

namespace CamusDB.Core.Extensions;

/// <summary>
/// Turns a <see cref="CamusFunctionAttribute"/> method into a <see cref="ScalarFunctionDescriptor"/>
/// whose evaluator is a delegate compiled once, at startup, from an expression tree. The call path
/// therefore holds no reflection: each call converts its arguments with direct calls into
/// <see cref="ExtensionValueMapping"/>, calls the user method, and wraps the result — the same cost
/// class as a built-in function.
///
/// <para><b>NULL rules.</b> A parameter that accepts NULL (<c>Nullable&lt;T&gt;</c>, or a reference
/// type annotated nullable) receives <c>null</c>. Any other parameter propagates NULL: the adapter
/// returns NULL without calling the method, the same as <see cref="ScalarFunctionArguments.PropagateNull"/>
/// for built-ins. A <c>null</c> return value is NULL.</para>
///
/// <para><b>Arity.</b> The evaluator checks the arity before it calls the adapter. Optional parameters
/// make a range; a missing optional argument takes the method's default value.</para>
///
/// <para><b>Errors.</b> A conversion error (a wrong argument type) is a <see cref="CamusDBException"/>
/// raised before the user method runs. An exception from the user method becomes
/// <see cref="CamusDBErrorCodes.ExtensionFunctionFailed"/> with the function name and the inner
/// message. <see cref="OperationCanceledException"/> passes through unchanged so cancellation keeps its
/// meaning. A stack overflow in user code cannot be caught and ends the process.</para>
/// </summary>
internal static class ExtensionFunctionBinder
{
    private static readonly MethodInfo CountGetter =
        typeof(IReadOnlyCollection<ColumnValue>).GetProperty(nameof(IReadOnlyCollection<ColumnValue>.Count))!.GetMethod!;

    private static readonly MethodInfo ItemGetter =
        typeof(IReadOnlyList<ColumnValue>).GetProperty("Item")!.GetMethod!;

    private static readonly MethodInfo FailedFactory =
        typeof(ExtensionFunctionBinder).GetMethod(nameof(Failed), BindingFlags.NonPublic | BindingFlags.Static)!;

    /// <summary>
    /// Builds the descriptor for one method. Throws <see cref="CamusDBErrorCodes.ExtensionInvalidSignature"/>
    /// when the method is not public static, is generic, or uses a type outside the value mapping.
    /// </summary>
    public static ScalarFunctionDescriptor Bind(MethodInfo method, CamusFunctionAttribute attribute)
    {
        string where = method.DeclaringType?.FullName + "." + method.Name;

        if (!method.IsStatic || !method.IsPublic || method.ContainsGenericParameters)
            throw InvalidSignature(where, "a [CamusFunction] method must be public, static and not generic");

        if (string.IsNullOrWhiteSpace(attribute.Name))
            throw InvalidSignature(where, "the function name is empty");

        if (method.ReturnType == typeof(void) || !ExtensionValueMapping.IsMappedType(method.ReturnType))
            throw InvalidSignature(where, $"the return type {method.ReturnType} is not in the extension value mapping");

        ParameterInfo[] parameters = method.GetParameters();
        int minArity = parameters.Length;

        for (int i = 0; i < parameters.Length; i++)
        {
            ParameterInfo parameter = parameters[i];

            if (parameter.ParameterType.IsByRef || parameter.IsOut)
                throw InvalidSignature(where, $"parameter '{parameter.Name}' is passed by reference");

            if (!ExtensionValueMapping.IsMappedType(parameter.ParameterType))
                throw InvalidSignature(where, $"parameter '{parameter.Name}' has type {parameter.ParameterType}, which is not in the extension value mapping");

            if (parameter.HasDefaultValue)
                minArity = Math.Min(minArity, i);
            else if (minArity < parameters.Length)
                throw InvalidSignature(where, $"parameter '{parameter.Name}' has no default value but follows an optional parameter");
        }

        ScalarFunctionEvaluatorDelegate evaluator = Compile(method, parameters, minArity);
        ColumnType returnType = ExtensionValueMapping.ColumnTypeOf(method.ReturnType);

        return new ScalarFunctionDescriptor
        {
            Name = attribute.Name,
            Aliases = attribute.Aliases ?? [],
            MinArity = minArity,
            MaxArity = parameters.Length,
            Evaluator = evaluator,
            IsVolatile = attribute.IsVolatile,
            InferReturnType = _ => returnType,
        };
    }

    /// <summary>
    /// <code>
    /// (calledName, args) =>
    /// {
    ///     if (args[i] is NULL) return NULL;        // for each non-nullable parameter i
    ///     Arg0 a0 = ToArg0(args[0], calledName, 0);   // conversions run before the user call
    ///     Arg1 a1 = args.Count > 1 ? ToArg1(args[1], calledName, 1) : default1;
    ///     R result;
    ///     try { result = Method(a0, a1); }
    ///     catch (OperationCanceledException) { throw; }
    ///     catch (Exception ex) { throw Failed(calledName, ex); }
    ///     return FromR(result);
    /// }
    /// </code>
    /// </summary>
    private static ScalarFunctionEvaluatorDelegate Compile(MethodInfo method, ParameterInfo[] parameters, int minArity)
    {
        ParameterExpression calledName = Expression.Parameter(typeof(string), "calledName");
        ParameterExpression arguments = Expression.Parameter(typeof(IReadOnlyList<ColumnValue>), "arguments");
        LabelTarget returnLabel = Expression.Label(typeof(ColumnValue), "return");

        List<ParameterExpression> locals = new(parameters.Length + 1);
        List<Expression> body = new(parameters.Length * 2 + 3);

        for (int i = 0; i < parameters.Length; i++)
        {
            ParameterInfo parameter = parameters[i];
            Expression argument = Expression.Call(arguments, ItemGetter, Expression.Constant(i));
            Expression present = Expression.GreaterThan(Expression.Call(arguments, CountGetter), Expression.Constant(i));

            bool acceptsNull = AcceptsNull(parameter);

            if (!acceptsNull)
            {
                Expression isNull = Expression.Equal(
                    Expression.Property(argument, nameof(ColumnValue.Type)),
                    Expression.Constant(ColumnType.Null));

                // An absent optional argument is not NULL: guard the read with the count.
                Expression test = i < minArity ? isNull : Expression.AndAlso(present, isNull);
                body.Add(Expression.IfThen(test, Expression.Return(returnLabel, Expression.Constant(ColumnValue.Null))));
            }

            ParameterExpression local = Expression.Variable(parameter.ParameterType, parameter.Name ?? "arg" + i);
            locals.Add(local);

            Expression converted = Expression.Call(
                ToClrConverter(parameter.ParameterType, acceptsNull),
                argument,
                calledName,
                Expression.Constant(i));

            if (converted.Type != parameter.ParameterType)
                converted = Expression.Convert(converted, parameter.ParameterType);

            if (i >= minArity)
            {
                object? defaultValue = parameter.DefaultValue is DBNull ? null : parameter.DefaultValue;
                Expression fallback = defaultValue is null
                    ? Expression.Default(parameter.ParameterType)
                    : Expression.Constant(defaultValue, parameter.ParameterType);
                converted = Expression.Condition(present, converted, fallback);
            }

            body.Add(Expression.Assign(local, converted));
        }

        ParameterExpression result = Expression.Variable(method.ReturnType, "result");
        locals.Add(result);

        ParameterExpression caught = Expression.Variable(typeof(Exception), "ex");

        body.Add(Expression.TryCatch(
            Expression.Block(typeof(void), Expression.Assign(result, Expression.Call(method, locals.GetRange(0, parameters.Length)))),
            Expression.Catch(typeof(OperationCanceledException), Expression.Rethrow(typeof(void))),
            Expression.Catch(caught, Expression.Throw(Expression.Call(FailedFactory, calledName, caught), typeof(void)))));

        body.Add(Expression.Label(returnLabel, Expression.Call(FromClrConverter(method.ReturnType), result)));

        Expression<ScalarFunctionEvaluatorDelegate> lambda = Expression.Lambda<ScalarFunctionEvaluatorDelegate>(
            Expression.Block(typeof(ColumnValue), locals, body),
            calledName,
            arguments);

        return lambda.Compile();
    }

    /// <summary>
    /// True when the parameter is declared to accept NULL: a <c>Nullable&lt;T&gt;</c>, or a reference
    /// type annotated nullable. A reference type in an assembly compiled without nullable annotations
    /// reads as "unknown" and propagates NULL, the safer default.
    /// </summary>
    private static bool AcceptsNull(ParameterInfo parameter)
    {
        if (Nullable.GetUnderlyingType(parameter.ParameterType) is not null)
            return true;

        if (parameter.ParameterType.IsValueType)
            return false;

        // NullabilityInfoContext is not thread-safe, and engines can start concurrently in one process.
        return new NullabilityInfoContext().Create(parameter).ReadState == NullabilityState.Nullable;
    }

    private static MethodInfo ToClrConverter(Type type, bool acceptsNull)
    {
        Type underlying = Nullable.GetUnderlyingType(type) ?? type;
        bool nullable = acceptsNull || Nullable.GetUnderlyingType(type) is not null;

        string name =
            underlying == typeof(long) ? "Int64" :
            underlying == typeof(double) ? "Double" :
            underlying == typeof(float) ? "Single" :
            underlying == typeof(bool) ? "Boolean" :
            underlying == typeof(string) ? (nullable ? "String" : "StringValue") :
            underlying == typeof(Guid) ? "Guid" :
            underlying == typeof(byte[]) ? "Bytes" :
            underlying == typeof(DateTime) ? "DateTime" :
            underlying == typeof(decimal) ? "Decimal" :
            throw new ArgumentException($"Unmapped type {type}", nameof(type));

        // A nullable reference parameter uses the nullable converter so NULL arrives as null; a
        // value-type parameter that is not Nullable<T> never reaches here with NULL (guarded above).
        bool useNullable = nullable && (Nullable.GetUnderlyingType(type) is not null || !type.IsValueType);
        string methodName = useNullable ? "ToNullable" + name : "To" + name;

        return typeof(ExtensionValueMapping).GetMethod(methodName, BindingFlags.Public | BindingFlags.Static)
               ?? throw new InvalidOperationException($"Missing converter {methodName}");
    }

    private static MethodInfo FromClrConverter(Type type)
    {
        Type underlying = Nullable.GetUnderlyingType(type) ?? type;
        bool nullableValue = Nullable.GetUnderlyingType(type) is not null;

        string name =
            underlying == typeof(long) ? "Int64" :
            underlying == typeof(double) ? "Double" :
            underlying == typeof(float) ? "Single" :
            underlying == typeof(bool) ? "Boolean" :
            underlying == typeof(string) ? "String" :
            underlying == typeof(Guid) ? "Guid" :
            underlying == typeof(byte[]) ? "Bytes" :
            underlying == typeof(DateTime) ? "DateTime" :
            underlying == typeof(decimal) ? "Decimal" :
            throw new ArgumentException($"Unmapped type {type}", nameof(type));

        string methodName = nullableValue ? "FromNullable" + name : "From" + name;

        return typeof(ExtensionValueMapping).GetMethod(methodName, BindingFlags.Public | BindingFlags.Static)
               ?? throw new InvalidOperationException($"Missing converter {methodName}");
    }

    /// <summary>Called from compiled adapters when the user method throws.</summary>
    private static CamusDBException Failed(string function, Exception inner) =>
        new(CamusDBErrorCodes.ExtensionFunctionFailed, $"Function '{function}' failed: {inner.Message}", inner);

    private static CamusDBException InvalidSignature(string where, string reason) =>
        new(CamusDBErrorCodes.ExtensionInvalidSignature, $"Extension function {where}: {reason}");
}
