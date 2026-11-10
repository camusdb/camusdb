/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;

using NUnit.Framework;

using CamusDB.Core.Extensions.Inline;

namespace CamusDB.Tests.InlineFunctions;

/// <summary>
/// Measures the framework that the server runs on against the facts that
/// <see cref="InlineFunctionTypeGrowthChecker"/> stands on.
///
/// <para>That check bounds the size of every type a body can make, from the syntax alone, before the
/// compiler runs. It is correct only when framework members build their result types in a limited
/// way: each type parameter at most once, plus a small number of names of their own. The body of an
/// inline function cannot declare a type or a generic function, so framework members are the only
/// makers of types. If a later framework adds a member that returns, say, a pair of its own type
/// parameter for a type that a body can name, a body could double a type with each call and the
/// bound would be wrong. These tests fail then, on the build machine, and say which member.</para>
///
/// <para>The compiler of inline functions sees every public member of these assemblies, also the
/// ones that the allowed list refuses after the first compile, so all of them are measured.</para>
/// </summary>
[TestFixture]
public sealed class TestInlineFunctionFrameworkShapes
{
    private const BindingFlags Declared = BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly;

    /// <summary>The type names a type holds apart from type parameters, and how often it names each type parameter.</summary>
    private sealed class Shape
    {
        public int Fixed;

        public Dictionary<Type, int> Uses { get; } = [];

        public int MostUses => Uses.Count == 0 ? 0 : Uses.Values.Max();
    }

    private static readonly Assembly[] References =
    [
        typeof(object).Assembly,
        Assembly.Load("System.Runtime"),
        typeof(SortedSet<>).Assembly,
        typeof(Enumerable).Assembly,
        typeof(Regex).Assembly,
    ];

    private static Shape Measure(Type type)
    {
        Shape shape = new();
        Walk(type, shape);
        return shape;
    }

    private static void Walk(Type type, Shape shape)
    {
        if (type.IsByRef)
        {
            Walk(type.GetElementType()!, shape);
            return;
        }

        if (type.IsGenericParameter)
        {
            shape.Uses[type] = shape.Uses.GetValueOrDefault(type) + 1;
            return;
        }

        shape.Fixed++;

        if (type.IsArray || type.IsPointer)
            Walk(type.GetElementType()!, shape);
        else if (type.IsGenericType)
        {
            foreach (Type argument in type.GetGenericArguments())
                Walk(argument, shape);
        }
    }

    private static Type? ResultOf(MemberInfo member) => member switch
    {
        MethodInfo method => method.ReturnType,
        PropertyInfo property => property.PropertyType,
        FieldInfo field => field.FieldType,
        EventInfo @event => @event.EventHandlerType,
        _ => null,
    };

    /// <summary>
    /// A type parameter that can only be a number: the generic math interfaces constrain their own
    /// parameter to themselves, and a body cannot make a type that implements them.
    /// </summary>
    private static bool IsNumericInterface(Type type) =>
        type.IsInterface && type.Namespace == "System.Numerics";

    private static IEnumerable<(Type Type, MemberInfo Member, Type Result)> Members()
    {
        foreach (Assembly assembly in References)
        {
            foreach (Type type in assembly.GetExportedTypes())
            {
                foreach (MemberInfo member in type.GetMembers(Declared))
                {
                    if (ResultOf(member) is { } result)
                        yield return (type, member, result);
                }
            }
        }
    }

    [Test]
    public void AMemberResult_AddsFewerNamesThanTheBoundAssumes()
    {
        List<string> wide = [];
        int measured = 0;

        foreach ((Type type, MemberInfo member, Type result) in Members())
        {
            measured++;

            // One name less than the constant: a call can also infer a type argument from an
            // interface of its receiver, which the next test bounds.
            if (Measure(result).Fixed > InlineFunctionTypeGrowthChecker.MemberOverhead - 3)
                wide.Add($"{type}.{member.Name} -> {result}");
        }

        Assert.Greater(measured, 20_000, "the members of the framework were found");
        CollectionAssert.IsEmpty(wide, "a member result holds more type names of its own than MemberOverhead allows");
    }

    [Test]
    public void AMemberResult_NamesEachTypeParameterOnce()
    {
        List<string> doubled = [];

        foreach ((Type type, MemberInfo member, Type result) in Members())
        {
            if (Measure(result).MostUses > 1 && !IsNumericInterface(type))
                doubled.Add($"{type}.{member.Name} -> {result}");
        }

        CollectionAssert.IsEmpty(doubled, "a member whose result names a type parameter twice doubles a type with each call");
    }

    [Test]
    public void AnInheritedMemberResult_NamesEachTypeParameterOfTheTypeOnce()
    {
        List<string> doubled = [];

        foreach (Assembly assembly in References)
        {
            foreach (Type type in assembly.GetExportedTypes())
            {
                if (!type.IsGenericTypeDefinition || IsNumericInterface(type))
                    continue;

                HashSet<Type> own = [.. type.GetGenericArguments()];
                List<Type> parents = [.. type.GetInterfaces()];

                for (Type? parent = type.BaseType; parent is not null; parent = parent.BaseType)
                    parents.Add(parent);

                foreach (Type parent in parents)
                {
                    if (!parent.IsGenericType)
                        continue;

                    foreach (MemberInfo member in parent.GetMembers(Declared))
                    {
                        if (ResultOf(member) is { } result && Measure(result).Uses.Any(use => own.Contains(use.Key) && use.Value > 1))
                            doubled.Add($"{type} through {parent}.{member.Name} -> {result}");
                    }
                }
            }
        }

        CollectionAssert.IsEmpty(doubled);
    }

    [Test]
    public void ATypeArgumentOfAnImplementedInterface_IsSmall_AndNamesEachTypeParameterOnce()
    {
        List<string> findings = [];

        foreach (Assembly assembly in References)
        {
            foreach (Type type in assembly.GetExportedTypes())
            {
                IEnumerable<Type> parents = type.BaseType is null ? type.GetInterfaces() : type.GetInterfaces().Append(type.BaseType);

                foreach (Type parent in parents)
                {
                    if (!parent.IsGenericType)
                        continue;

                    foreach (Type argument in parent.GetGenericArguments())
                    {
                        Shape shape = Measure(argument);

                        if (shape.Fixed > 3 || shape.MostUses > 1)
                            findings.Add($"{type} : {parent}");
                    }
                }
            }
        }

        CollectionAssert.IsEmpty(findings, "a call infers a type argument from an interface of its receiver; MemberOverhead allows three names for it");
    }

    [Test]
    public void AParameterOfADelegate_AndAnOutParameter_AreSmall_AndNameEachTypeParameterOnce()
    {
        List<string> findings = [];

        foreach ((Type type, MemberInfo member, Type _) in Members())
        {
            if (member is not MethodInfo method)
                continue;

            foreach (ParameterInfo parameter in method.GetParameters())
            {
                Type parameterType = parameter.ParameterType;

                if (parameterType.IsByRef && !parameter.IsIn)
                {
                    Shape passed = Measure(parameterType);

                    if (passed.Fixed > 2 || passed.MostUses > 1)
                        findings.Add($"{type}.{method.Name}: out or ref {parameterType}");
                }

                Type bare = parameterType.IsByRef ? parameterType.GetElementType()! : parameterType;

                if (!typeof(Delegate).IsAssignableFrom(bare) || bare == typeof(Delegate) || bare == typeof(MulticastDelegate) ||
                    bare.GetMethod("Invoke") is not { } invoke)
                    continue;

                foreach (ParameterInfo input in invoke.GetParameters())
                {
                    Shape given = Measure(input.ParameterType);

                    if (given.Fixed > 2 || given.MostUses > 1)
                        findings.Add($"{type}.{method.Name}: {bare} takes {input.ParameterType}");
                }
            }
        }

        CollectionAssert.IsEmpty(findings, "the type of a lambda parameter and of an 'out var' comes from these");
    }

    [Test]
    public void AnOperator_IsABool_OrNoLargerThanAnOperand()
    {
        List<string> findings = [];

        foreach ((Type type, MemberInfo member, Type result) in Members())
        {
            if (member is not MethodInfo { IsSpecialName: true } method || !method.Name.StartsWith("op_", StringComparison.Ordinal) ||
                method.Name is "op_Implicit" or "op_Explicit" || IsNumericInterface(type))
                continue;

            if (method.Name is "op_True" or "op_False")
            {
                if (type.IsGenericType)
                    findings.Add($"{type}.{method.Name}: a generic type with '&&' and '||' of its own");

                continue;
            }

            if (method.Name is "op_Equality" or "op_Inequality" or "op_LessThan" or "op_GreaterThan" or "op_LessThanOrEqual" or "op_GreaterThanOrEqual")
            {
                if (result != typeof(bool))
                    findings.Add($"{type}.{method.Name}: a comparison that is not a bool");

                continue;
            }

            Shape made = Measure(result);

            bool covered = (made.Fixed <= 1 && made.Uses.Count == 0) || method.GetParameters().Any(parameter =>
            {
                Shape operand = Measure(parameter.ParameterType);
                return operand.Fixed >= made.Fixed && made.Uses.All(use => operand.Uses.GetValueOrDefault(use.Key) >= use.Value);
            });

            if (!covered)
                findings.Add($"{type}.{method.Name} -> {result}");
        }

        CollectionAssert.IsEmpty(findings);
    }

    /// <summary>
    /// A call binds its lambdas in the order of the text. That is correct when no delegate takes a
    /// type that only a delegate after it gives.
    /// </summary>
    [Test]
    public void ADelegateParameter_TakesNoTypeThatOnlyALaterDelegateReturns()
    {
        List<string> findings = [];

        foreach ((Type type, MemberInfo member, Type _) in Members())
        {
            if (member is not MethodInfo { IsGenericMethodDefinition: true } method)
                continue;

            ParameterInfo[] parameters = method.GetParameters();
            HashSet<Type> known = [];

            // What any parameter that is not a delegate gives is known whatever its position.
            foreach (ParameterInfo parameter in parameters)
            {
                if (InvokeOf(parameter.ParameterType) is null)
                    known.UnionWith(Measure(parameter.ParameterType).Uses.Keys);
            }

            foreach (ParameterInfo parameter in parameters)
            {
                if (InvokeOf(parameter.ParameterType) is not { } invoke)
                    continue;

                foreach (ParameterInfo input in invoke.GetParameters())
                {
                    foreach (Type used in Measure(input.ParameterType).Uses.Keys)
                    {
                        if (used.DeclaringMethod is not null && !known.Contains(used))
                            findings.Add($"{type}.{method.Name}: {parameter.Name} takes {used.Name} before a parameter gives it");
                    }
                }

                known.UnionWith(Measure(invoke.ReturnType).Uses.Keys);
            }
        }

        CollectionAssert.IsEmpty(findings);

        static MethodInfo? InvokeOf(Type parameterType)
        {
            Type bare = parameterType.IsByRef ? parameterType.GetElementType()! : parameterType;

            return typeof(Delegate).IsAssignableFrom(bare) && bare != typeof(Delegate) && bare != typeof(MulticastDelegate)
                ? bare.GetMethod("Invoke")
                : null;
        }
    }
}
