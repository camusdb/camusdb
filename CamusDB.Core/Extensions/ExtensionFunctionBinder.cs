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
/// <para><b>Arrays.</b> An <c>IReadOnlyList&lt;T&gt;</c> parameter or return type maps to <c>array(E)</c>
/// (see <see cref="ExtensionValueMapping"/>). The adapter converts the list with a typed element
/// delegate created here, once, so an array call holds no reflection either. A function that returns
/// a list reports its element type on the descriptor
/// (<see cref="ScalarFunctionDescriptor.ArrayElementType"/>), which is what lets a CTAS or a derived
/// table declare the column's element type.</para>
///
/// <para><b>Errors.</b> A conversion error (a wrong argument type) is a <see cref="CamusDBException"/>
/// raised before the user method runs. An exception from the user method becomes
/// <see cref="CamusDBErrorCodes.ExtensionFunctionFailed"/> with the function name and the inner
/// message. <see cref="OperationCanceledException"/> passes through unchanged so cancellation keeps its
/// meaning. A stack overflow in user code cannot be caught and ends the process.</para>
/// </summary>
internal static class ExtensionFunctionBinder
{
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

        if (method.ReturnType == typeof(void))
            throw InvalidSignature(where, "the method returns void");

        if (!ExtensionValueMapping.IsMappedType(method.ReturnType))
            throw InvalidSignature(where, "the return value " + ExtensionValueMapping.DescribeUnmapped(method.ReturnType));

        ParameterInfo[] parameters = method.GetParameters();
        int minArity = ExtensionAdapterParts.CheckParameters(parameters, 0, where);

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
            ArrayElementType = ExtensionValueMapping.ArrayElementColumnTypeOf(method.ReturnType),
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

        ExtensionAdapterParts.EmitArguments(
            parameters, 0, minArity, calledName, arguments,
            Expression.Return(returnLabel, Expression.Constant(ColumnValue.Null)), locals, body);

        ParameterExpression result = Expression.Variable(method.ReturnType, "result");
        List<ParameterExpression> callArguments = [.. locals];
        locals.Add(result);

        body.Add(ExtensionAdapterParts.GuardUserCall(
            Expression.Assign(result, Expression.Call(method, callArguments)),
            caught => Expression.Call(FailedFactory, calledName, caught)));

        body.Add(Expression.Label(returnLabel, ExtensionAdapterParts.ConvertResult(method.ReturnType, result)));

        Expression<ScalarFunctionEvaluatorDelegate> lambda = Expression.Lambda<ScalarFunctionEvaluatorDelegate>(
            Expression.Block(typeof(ColumnValue), locals, body),
            calledName,
            arguments);

        return lambda.Compile();
    }

    /// <summary>
    /// The SQL signature of a member, for <c>SHOW EXTENSIONS</c>: <c>name(INT64, ARRAY(STRING)) -&gt; STRING</c>.
    /// An optional parameter is shown in brackets. <paramref name="skip"/> leading parameters are not
    /// SQL arguments (an aggregate's state).
    /// </summary>
    internal static string Signature(string name, ParameterInfo[] parameters, int skip, string result)
    {
        System.Text.StringBuilder text = new(name.Length + 16 * parameters.Length);
        text.Append(name).Append('(');

        for (int i = skip; i < parameters.Length; i++)
        {
            if (i > skip)
                text.Append(", ");

            string type = ExtensionValueMapping.SqlTypeName(parameters[i].ParameterType);
            text.Append(parameters[i].HasDefaultValue ? "[" + type + "]" : type);
        }

        return text.Append(") -> ").Append(result).ToString();
    }

    /// <summary>Called from compiled adapters when the user method throws.</summary>
    private static CamusDBException Failed(string function, Exception inner) =>
        new(CamusDBErrorCodes.ExtensionFunctionFailed, $"Function '{function}' failed: {inner.Message}", inner);

    internal static CamusDBException InvalidSignature(string where, string reason) =>
        new(CamusDBErrorCodes.ExtensionInvalidSignature, $"Extension function {where}: {reason}");
}

/// <summary>
/// The expression-tree pieces that every extension adapter shares: the parameter checks, the NULL
/// guards and argument conversions, the result conversion, and the exception guard around the user
/// call. Scalar functions (<see cref="ExtensionFunctionBinder"/>), table functions
/// (<see cref="ExtensionTableFunctionBinder"/>) and aggregates (<see cref="ExtensionAggregateBinder"/>)
/// all build from here, so one value mapping and one NULL rule apply to every member kind.
///
/// <para><b>Startup only.</b> These methods use reflection to build the trees. The compiled delegates
/// hold direct calls into <see cref="ExtensionValueMapping"/> and typed element delegates, so nothing
/// built here reflects on the call path.</para>
/// </summary>
internal static class ExtensionAdapterParts
{
    private static readonly MethodInfo CountGetter =
        typeof(IReadOnlyCollection<ColumnValue>).GetProperty(nameof(IReadOnlyCollection<ColumnValue>.Count))!.GetMethod!;

    private static readonly MethodInfo ItemGetter =
        typeof(IReadOnlyList<ColumnValue>).GetProperty("Item")!.GetMethod!;

    /// <summary>
    /// Checks the SQL parameters <c>parameters[skip..]</c> and returns the minimum arity: the count of
    /// leading parameters without a default value. Throws for a by-reference parameter, an unmapped
    /// type, or a required parameter after an optional one.
    /// </summary>
    public static int CheckParameters(ParameterInfo[] parameters, int skip, string where)
    {
        int count = parameters.Length - skip;
        int minArity = count;

        for (int i = skip; i < parameters.Length; i++)
        {
            ParameterInfo parameter = parameters[i];

            if (parameter.ParameterType.IsByRef || parameter.IsOut)
                throw ExtensionFunctionBinder.InvalidSignature(where, $"parameter '{parameter.Name}' is passed by reference");

            if (!ExtensionValueMapping.IsMappedType(parameter.ParameterType))
                throw ExtensionFunctionBinder.InvalidSignature(where, $"parameter '{parameter.Name}' {ExtensionValueMapping.DescribeUnmapped(parameter.ParameterType)}");

            if (parameter.HasDefaultValue)
                minArity = Math.Min(minArity, i - skip);
            else if (minArity < count)
                throw ExtensionFunctionBinder.InvalidSignature(where, $"parameter '{parameter.Name}' has no default value but follows an optional parameter");
        }

        return minArity;
    }

    /// <summary>
    /// Adds one local per SQL parameter (<c>parameters[skip..]</c>, read from <c>arguments[i - skip]</c>)
    /// and the statements that fill them: a NULL guard that runs <paramref name="onNull"/> for each
    /// parameter that does not accept NULL, then the conversion. An absent optional argument takes the
    /// method's default value. The locals are appended to <paramref name="locals"/> in parameter order.
    /// </summary>
    public static void EmitArguments(
        ParameterInfo[] parameters,
        int skip,
        int minArity,
        Expression calledName,
        Expression arguments,
        Expression onNull,
        List<ParameterExpression> locals,
        List<Expression> body)
    {
        for (int p = skip; p < parameters.Length; p++)
        {
            int i = p - skip;
            ParameterInfo parameter = parameters[p];
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
                body.Add(Expression.IfThen(test, onNull));
            }

            ParameterExpression local = Expression.Variable(parameter.ParameterType, parameter.Name ?? "arg" + i);
            locals.Add(local);

            Expression converted = ConvertArgument(parameter.ParameterType, acceptsNull, argument, calledName, Expression.Constant(i));

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
    }

    /// <summary>
    /// <c>try { call; } catch (OperationCanceledException) { throw; } catch (Exception ex) { throw wrap(ex); }</c>
    /// </summary>
    public static Expression GuardUserCall(Expression call, Func<ParameterExpression, Expression> wrap)
    {
        ParameterExpression caught = Expression.Variable(typeof(Exception), "ex");

        return Expression.TryCatch(
            Expression.Block(typeof(void), call),
            Expression.Catch(typeof(OperationCanceledException), Expression.Rethrow(typeof(void))),
            Expression.Catch(caught, Expression.Throw(wrap(caught), typeof(void))));
    }

    /// <summary>
    /// True when the parameter is declared to accept NULL: a <c>Nullable&lt;T&gt;</c>, or a reference
    /// type annotated nullable. A reference type in an assembly compiled without nullable annotations
    /// reads as "unknown" and propagates NULL, the safer default.
    /// </summary>
    public static bool AcceptsNull(ParameterInfo parameter)
    {
        if (Nullable.GetUnderlyingType(parameter.ParameterType) is not null)
            return true;

        if (parameter.ParameterType.IsValueType)
            return false;

        // NullabilityInfoContext is not thread-safe, and engines can start concurrently in one process.
        return new NullabilityInfoContext().Create(parameter).ReadState == NullabilityState.Nullable;
    }

    /// <summary>
    /// The conversion of one engine argument into a CLR value of <paramref name="type"/>:
    /// <c>ExtensionValueMapping.To{Type}(argument, calledName, index)</c> for a scalar, and
    /// <c>ToList&lt;T&gt;(argument, calledName, index, elementConverter, nullElements)</c> for an array.
    /// </summary>
    public static Expression ConvertArgument(Type type, bool acceptsNull, Expression argument, Expression calledName, Expression index)
    {
        if (ExtensionValueMapping.ArrayElementOf(type) is { } element)
        {
            // Reference elements always accept NULL; a value element only as Nullable<T>.
            bool nullElements = !element.IsValueType || Nullable.GetUnderlyingType(element) is not null;
            Delegate elementConverter = ScalarToClr(element, acceptsNull: nullElements)
                .CreateDelegate(typeof(Func<,,,>).MakeGenericType(typeof(ColumnValue), typeof(string), typeof(int), element));

            MethodInfo list = typeof(ExtensionValueMapping)
                .GetMethod(acceptsNull ? nameof(ExtensionValueMapping.ToNullableList) : nameof(ExtensionValueMapping.ToList))!
                .MakeGenericMethod(element);

            return Expression.Call(list, argument, calledName, index, Expression.Constant(elementConverter), Expression.Constant(nullElements));
        }

        Expression converted = Expression.Call(ScalarToClr(type, acceptsNull), argument, calledName, index);
        return converted.Type == type ? converted : Expression.Convert(converted, type);
    }

    /// <summary>
    /// The conversion of a CLR value of <paramref name="type"/> into an engine value:
    /// <c>From{Type}(value)</c> for a scalar, and <c>FromList&lt;T&gt;(value, elementType, elementConverter)</c>
    /// for an array.
    /// </summary>
    public static Expression ConvertResult(Type type, Expression value)
    {
        if (ExtensionValueMapping.ArrayElementOf(type) is { } element)
        {
            Delegate elementConverter = ScalarFromClr(element)
                .CreateDelegate(typeof(Func<,>).MakeGenericType(element, typeof(ColumnValue)));

            MethodInfo list = typeof(ExtensionValueMapping)
                .GetMethod(nameof(ExtensionValueMapping.FromList))!
                .MakeGenericMethod(element);

            return Expression.Call(
                list,
                value,
                Expression.Constant(ExtensionValueMapping.ColumnTypeOf(element)),
                Expression.Constant(elementConverter));
        }

        return Expression.Call(ScalarFromClr(type), value);
    }

    /// <summary>A typed <c>Func&lt;T, ColumnValue&gt;</c> for a mapped type, for wrappers that convert outside a tree.</summary>
    public static Func<T, ColumnValue> ResultConverter<T>()
    {
        ParameterExpression value = Expression.Parameter(typeof(T), "value");
        return Expression.Lambda<Func<T, ColumnValue>>(ConvertResult(typeof(T), value), value).Compile();
    }

    private static MethodInfo ScalarToClr(Type type, bool acceptsNull)
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

    private static MethodInfo ScalarFromClr(Type type)
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
}
