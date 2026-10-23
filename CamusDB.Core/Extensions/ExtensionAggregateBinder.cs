/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Linq.Expressions;
using System.Reflection;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Extensions;

namespace CamusDB.Core.Extensions;

/// <summary>
/// Turns a <see cref="CamusAggregateAttribute"/> class into an <see cref="ExtensionAggregate"/>: three
/// delegates compiled once at startup, from expression trees, that create a state, fold one row into
/// it, and finish it. The query aggregator calls them through one arm of its per-aggregate state, so a
/// row costs one delegate call and the argument conversions, the same cost class as a scalar UDF.
///
/// <para><b>Shape.</b> The class implements <see cref="ICamusAggregate{TState}"/> once. <c>Accumulate</c>
/// takes the state and then the SQL arguments, whose types follow the value mapping, and returns void;
/// it must be the only public method of that name. <c>Finish</c> takes the state and returns a mapped
/// type. <c>Merge(TState, TState)</c> is optional and recorded only: this version never decomposes an
/// extension aggregate into partials.</para>
///
/// <para><b>NULL.</b> The NULL guard of a scalar UDF, with a different outcome: a NULL argument for a
/// parameter that does not accept NULL skips the row instead of making the result NULL.</para>
/// </summary>
internal static class ExtensionAggregateBinder
{
    /// <summary>The bytes charged for one state when the attribute gives no hint.</summary>
    internal const int DefaultStateSizeBytes = 64;

    private static readonly MethodInfo FailedFactory =
        typeof(ExtensionAggregate).GetMethod(nameof(ExtensionAggregate.Failed), BindingFlags.NonPublic | BindingFlags.Static)!;

    public static ExtensionAggregate Bind(Type type, CamusAggregateAttribute attribute, Func<object> factory)
    {
        string where = type.FullName ?? type.Name;

        if (string.IsNullOrWhiteSpace(attribute.Name))
            throw ExtensionFunctionBinder.InvalidSignature(where, "the aggregate name is empty");

        if (attribute.StateSizeHint < 0)
            throw ExtensionFunctionBinder.InvalidSignature(where, "StateSizeHint must not be negative");

        Type[] implemented = type.GetInterfaces()
            .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(ICamusAggregate<>))
            .ToArray();

        if (implemented.Length != 1)
            throw ExtensionFunctionBinder.InvalidSignature(where, "a [CamusAggregate] class must implement ICamusAggregate<TState> exactly once");

        Type state = implemented[0].GetGenericArguments()[0];
        MethodInfo create = implemented[0].GetMethod(nameof(ICamusAggregate<object>.Create))!;

        MethodInfo accumulate = FindOne(type, "Accumulate", where);
        ParameterInfo[] parameters = accumulate.GetParameters();

        if (accumulate.ReturnType != typeof(void) || parameters.Length == 0 || parameters[0].ParameterType != state)
            throw ExtensionFunctionBinder.InvalidSignature(where, $"Accumulate must return void and take {state.Name} as its first parameter");

        int minArity = ExtensionAdapterParts.CheckParameters(parameters, 1, where);

        MethodInfo finish = FindOne(type, "Finish", where);

        if (finish.GetParameters() is not [{ } finishState] || finishState.ParameterType != state || finish.ReturnType == typeof(void))
            throw ExtensionFunctionBinder.InvalidSignature(where, $"Finish must take one {state.Name} parameter and return a value");

        if (!ExtensionValueMapping.IsMappedType(finish.ReturnType))
            throw ExtensionFunctionBinder.InvalidSignature(where, "the result of Finish " + ExtensionValueMapping.DescribeUnmapped(finish.ReturnType));

        MethodInfo? merge = type.GetMethod("Merge", BindingFlags.Public | BindingFlags.Instance, null, [state, state], null);

        if (merge is not null && merge.ReturnType != typeof(void))
            throw ExtensionFunctionBinder.InvalidSignature(where, $"Merge must return void");

        return new ExtensionAggregate(
            attribute.Name,
            minArity,
            parameters.Length - 1,
            ExtensionValueMapping.ColumnTypeOf(finish.ReturnType),
            ExtensionValueMapping.ArrayElementColumnTypeOf(finish.ReturnType),
            attribute.StateSizeHint > 0 ? attribute.StateSizeHint : DefaultStateSizeBytes,
            merge is not null,
            ExtensionFunctionBinder.Signature(attribute.Name, parameters, 1, ExtensionValueMapping.SqlTypeName(finish.ReturnType)),
            factory,
            CompileCreate(type, create),
            CompileAccumulate(type, state, accumulate, parameters, minArity),
            CompileFinish(type, state, finish));
    }

    private static MethodInfo FindOne(Type type, string name, string where)
    {
        MethodInfo[] methods = type.GetMethods(BindingFlags.Public | BindingFlags.Instance)
            .Where(m => m.Name == name && !m.IsGenericMethodDefinition)
            .ToArray();

        return methods.Length switch
        {
            1 => methods[0],
            0 => throw ExtensionFunctionBinder.InvalidSignature(where, $"a [CamusAggregate] class needs a public {name} method"),
            _ => throw ExtensionFunctionBinder.InvalidSignature(where, $"a [CamusAggregate] class must have exactly one public {name} method"),
        };
    }

    /// <summary><c>(instance, calledName) =&gt; { try { return ((T)instance).Create(); } catch ... }</c></summary>
    private static Func<object, string, object> CompileCreate(Type type, MethodInfo create)
    {
        ParameterExpression instance = Expression.Parameter(typeof(object), "instance");
        ParameterExpression calledName = Expression.Parameter(typeof(string), "calledName");
        ParameterExpression result = Expression.Variable(typeof(object), "result");

        Expression call = Expression.Convert(Expression.Call(Expression.Convert(instance, type), create), typeof(object));

        BlockExpression body = Expression.Block(
            typeof(object),
            [result],
            ExtensionAdapterParts.GuardUserCall(Expression.Assign(result, call), caught => Expression.Call(FailedFactory, calledName, caught)),
            result);

        return Expression.Lambda<Func<object, string, object>>(body, instance, calledName).Compile();
    }

    /// <summary>
    /// <code>
    /// (instance, state, calledName, args) =>
    /// {
    ///     if (args[i] is NULL) return;               // for each non-nullable parameter: skip the row
    ///     Arg0 a0 = ToArg0(args[0], calledName, 0);
    ///     try { ((T)instance).Accumulate((TState)state, a0); }
    ///     catch (OperationCanceledException) { throw; }
    ///     catch (Exception ex) { throw Failed(calledName, ex); }
    /// }
    /// </code>
    /// </summary>
    private static Action<object, object, string, IReadOnlyList<ColumnValue>> CompileAccumulate(
        Type type, Type state, MethodInfo accumulate, ParameterInfo[] parameters, int minArity)
    {
        ParameterExpression instance = Expression.Parameter(typeof(object), "instance");
        ParameterExpression stateObject = Expression.Parameter(typeof(object), "state");
        ParameterExpression calledName = Expression.Parameter(typeof(string), "calledName");
        ParameterExpression arguments = Expression.Parameter(typeof(IReadOnlyList<ColumnValue>), "arguments");
        LabelTarget skip = Expression.Label("skip");

        List<ParameterExpression> locals = new(parameters.Length);
        List<Expression> body = new(parameters.Length * 2 + 2);

        ExtensionAdapterParts.EmitArguments(parameters, 1, minArity, calledName, arguments, Expression.Return(skip), locals, body);

        List<Expression> callArguments = [Expression.Convert(stateObject, state), .. locals];

        body.Add(ExtensionAdapterParts.GuardUserCall(
            Expression.Call(Expression.Convert(instance, type), accumulate, callArguments),
            caught => Expression.Call(FailedFactory, calledName, caught)));

        body.Add(Expression.Label(skip));

        return Expression.Lambda<Action<object, object, string, IReadOnlyList<ColumnValue>>>(
            Expression.Block(typeof(void), locals, body),
            instance, stateObject, calledName, arguments).Compile();
    }

    /// <summary><c>(instance, state, calledName) =&gt; From(((T)instance).Finish((TState)state))</c>, guarded.</summary>
    private static Func<object, object, string, ColumnValue> CompileFinish(Type type, Type state, MethodInfo finish)
    {
        ParameterExpression instance = Expression.Parameter(typeof(object), "instance");
        ParameterExpression stateObject = Expression.Parameter(typeof(object), "state");
        ParameterExpression calledName = Expression.Parameter(typeof(string), "calledName");
        ParameterExpression result = Expression.Variable(finish.ReturnType, "result");

        BlockExpression body = Expression.Block(
            typeof(ColumnValue),
            [result],
            ExtensionAdapterParts.GuardUserCall(
                Expression.Assign(result, Expression.Call(Expression.Convert(instance, type), finish, Expression.Convert(stateObject, state))),
                caught => Expression.Call(FailedFactory, calledName, caught)),
            ExtensionAdapterParts.ConvertResult(finish.ReturnType, result));

        return Expression.Lambda<Func<object, object, string, ColumnValue>>(body, instance, stateObject, calledName).Compile();
    }
}

/// <summary>
/// An aggregate function that a C# extension registered, ready for the query aggregator: create a
/// state per group, fold rows into it, finish it. See <see cref="ExtensionAggregateBinder"/>.
///
/// <para><b>One instance per engine.</b> The class is created on first use and shared by every
/// statement of the engine; the contract tells the author to keep per-group data in the state only.
/// A failed constructor fails every use, because a failed <see cref="Lazy{T}"/> caches its exception.</para>
///
/// <para><b>Never decomposed.</b> The planner's partial-aggregation and decomposition allow-lists name
/// built-ins only, so a query with an extension aggregate always aggregates the gathered rows on the
/// node that runs the statement. <see cref="HasMerge"/> is recorded for a later version that ships
/// partial states.</para>
/// </summary>
internal sealed class ExtensionAggregate
{
    private readonly Lazy<object> instance;

    private readonly Func<object, string, object> create;

    private readonly Action<object, object, string, IReadOnlyList<ColumnValue>> accumulate;

    private readonly Func<object, object, string, ColumnValue> finish;

    internal ExtensionAggregate(
        string name,
        int minArity,
        int maxArity,
        ColumnType returnType,
        ColumnType? returnElementType,
        int stateSizeBytes,
        bool hasMerge,
        string signature,
        Func<object> factory,
        Func<object, string, object> create,
        Action<object, object, string, IReadOnlyList<ColumnValue>> accumulate,
        Func<object, object, string, ColumnValue> finish)
    {
        Name = name;
        MinArity = minArity;
        MaxArity = maxArity;
        ReturnType = returnType;
        ReturnElementType = returnElementType;
        StateSizeBytes = stateSizeBytes;
        HasMerge = hasMerge;
        Signature = signature;
        instance = new Lazy<object>(factory, LazyThreadSafetyMode.ExecutionAndPublication);
        this.create = create;
        this.accumulate = accumulate;
        this.finish = finish;
    }

    /// <summary>The SQL name.</summary>
    public string Name { get; }

    public int MinArity { get; }

    public int MaxArity { get; }

    /// <summary>The column type of the <c>Finish</c> result.</summary>
    public ColumnType ReturnType { get; }

    /// <summary>The element type when <see cref="ReturnType"/> is an array.</summary>
    public ColumnType? ReturnElementType { get; }

    /// <summary>The bytes a spilling GROUP BY charges for one state (the attribute's hint, or a default).</summary>
    public int StateSizeBytes { get; }

    /// <summary>True when the class has a <c>Merge</c> method; not used by this version.</summary>
    public bool HasMerge { get; }

    /// <summary>The SQL signature for <c>SHOW EXTENSIONS</c>.</summary>
    public string Signature { get; }

    /// <summary>A new state for a group.</summary>
    public object CreateState(string calledName) => create(instance.Value, calledName);

    /// <summary>Folds one row's evaluated arguments into <paramref name="state"/>; a NULL argument for a non-nullable parameter skips the row.</summary>
    public void Accumulate(object state, string calledName, IReadOnlyList<ColumnValue> arguments) =>
        accumulate(instance.Value, state, calledName, arguments);

    /// <summary>The result of a group.</summary>
    public ColumnValue Finish(object state, string calledName) => finish(instance.Value, state, calledName);

    /// <summary>Called from compiled adapters when user code throws.</summary>
    internal static CamusDBException Failed(string aggregate, Exception inner) =>
        new(CamusDBErrorCodes.ExtensionFunctionFailed, $"Aggregate '{aggregate}' failed: {inner.Message}", inner);
}
