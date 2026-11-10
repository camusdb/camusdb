/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Extensions.Inline;

namespace CamusDB.Tests.InlineFunctions;

/// <summary>
/// Runs the body of an inline function through the real pipeline of one
/// <see cref="InlineFunctionRuntime"/>, with no database: compile, checks, load, bind, call. The
/// tests of what the compiler accepts, refuses and stops need nothing else, and they run in
/// milliseconds this way.
/// </summary>
internal sealed class InlineFunctionProbe : IDisposable
{
    private readonly InlineFunctionRuntime runtime;

    public InlineFunctionProbe(CamusDBOptions? options = null)
    {
        runtime = new InlineFunctionRuntime(options ?? CamusDBOptions.Default, logger: null);
    }

    public InlineFunctionRuntime Runtime => runtime;

    /// <summary>A function <c>f(parameters) RETURNS returnType</c> with <paramref name="body"/>.</summary>
    public static InlineFunctionDefinition Define(
        string body, ColumnType returnType, params (string Name, ColumnType Type)[] parameters) => new()
    {
        Name = "f",
        Source = body,
        SourceSha256 = InlineFunctionRuntime.SourceHash(body),
        ReturnType = returnType,
        Parameters = parameters.Select(p => new InlineFunctionParameter { Name = p.Name, Type = p.Type }).ToList(),
        LanguageVersion = InlineFunctionRuntime.LanguageVersion,
        RuleRevision = InlineFunctionAllowList.Revision,
    };

    /// <summary>A function <c>f(s STRING, n INT64) RETURNS STRING</c>, the shape most cases use.</summary>
    public static InlineFunctionDefinition Text(string body) =>
        Define(body, ColumnType.String, ("s", ColumnType.String), ("n", ColumnType.Integer64));

    /// <summary>A function <c>f(s STRING, n INT64) RETURNS INT64</c>.</summary>
    public static InlineFunctionDefinition Number(string body) =>
        Define(body, ColumnType.Integer64, ("s", ColumnType.String), ("n", ColumnType.Integer64));

    /// <summary>Compiles and checks <paramref name="function"/> as <c>CREATE FUNCTION</c> does. Throws what the statement throws.</summary>
    public void Validate(InlineFunctionDefinition function) => runtime.Validate(function, default);

    /// <summary>The error <c>CREATE FUNCTION</c> gives for <paramref name="function"/>.</summary>
    public CamusDBException Refusal(InlineFunctionDefinition function) =>
        NUnit.Framework.Assert.Catch<CamusDBException>(() => Validate(function))!;

    /// <summary>Compiles <paramref name="function"/> as a first call does, and calls it with the budgets of this probe.</summary>
    public ColumnValue Call(InlineFunctionDefinition function, params ColumnValue[] arguments)
    {
        InlineFunctionUnit unit = runtime.Acquire(function, "probe.f");

        try
        {
            if (unit.Evaluator is null)
                throw new CamusDBException(unit.Failure!.Code, unit.Failure.Message);

            return runtime.Guard(unit.Evaluator)(function.Name, arguments);
        }
        finally
        {
            runtime.Release(unit, "probe.f");
        }
    }

    /// <summary>Calls a function of the <see cref="Text"/> or <see cref="Number"/> shape.</summary>
    public ColumnValue Call(InlineFunctionDefinition function, string s, long n) =>
        Call(function, new ColumnValue(ColumnType.String, s), new ColumnValue(ColumnType.Integer64, n));

    public void Dispose() => runtime.Dispose();
}
