/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using CamusDB.Extensions;

namespace CamusDB.Extensions.Inline;

/// <summary>
/// The SQL session of an inline trigger body: <c>Sql</c> in the body. It wraps the session the
/// server gives a trigger (<see cref="ISqlSession"/>) with two changes.
///
/// <para><b>Typed parameters.</b> The contract's session takes its parameters as
/// <c>IReadOnlyDictionary&lt;string, object?&gt;</c>. A body cannot use <c>object</c> as a type
/// argument (a value inside a value, to any depth, overflows the stack when the framework prints or
/// compares it), so this session takes each parameter through <see cref="Set(string, long?)"/> and
/// its overloads, one for each SQL type, and converts them itself. The parameters collected by the
/// <c>Set</c> calls belong to the next <see cref="ExecuteAsync"/> or <see cref="QueryAsync"/>, which
/// clears them.</para>
///
/// <para><b>The budget around a statement.</b> The body's allocation count is per thread and the
/// statement may continue on another thread, so the session suspends the body's
/// <see cref="TriggerBudget"/> before the statement and resumes it just before it returns (see that
/// class). A session method is the only thing a body can await.</para>
/// </summary>
public sealed class InlineSqlSession
{
    private readonly ISqlSession inner;

    private readonly TriggerBudget budget;

    private Dictionary<string, object?>? parameters;

    /// <summary>Wraps <paramref name="inner"/>, the session the server built with the rights the trigger runs with.</summary>
    public InlineSqlSession(ISqlSession inner, TriggerBudget budget)
    {
        this.inner = inner ?? throw new ArgumentNullException(nameof(inner));
        this.budget = budget ?? throw new ArgumentNullException(nameof(budget));
    }

    /// <summary>Adds the parameter <c>@name</c> for the next statement. Returns this session, so calls chain.</summary>
    public InlineSqlSession Set(string name, long? value) => Add(name, value);

    /// <inheritdoc cref="Set(string, long?)"/>
    public InlineSqlSession Set(string name, string? value) => Add(name, value);

    /// <inheritdoc cref="Set(string, long?)"/>
    public InlineSqlSession Set(string name, double? value) => Add(name, value);

    /// <inheritdoc cref="Set(string, long?)"/>
    public InlineSqlSession Set(string name, float? value) => Add(name, value);

    /// <inheritdoc cref="Set(string, long?)"/>
    public InlineSqlSession Set(string name, bool? value) => Add(name, value);

    /// <inheritdoc cref="Set(string, long?)"/>
    public InlineSqlSession Set(string name, Guid? value) => Add(name, value);

    /// <inheritdoc cref="Set(string, long?)"/>
    public InlineSqlSession Set(string name, byte[]? value) => Add(name, value);

    /// <inheritdoc cref="Set(string, long?)"/>
    public InlineSqlSession Set(string name, DateTime? value) => Add(name, value);

    /// <inheritdoc cref="Set(string, long?)"/>
    public InlineSqlSession Set(string name, decimal? value) => Add(name, value);

    /// <inheritdoc cref="Set(string, long?)"/>
    public InlineSqlSession Set(string name, IReadOnlyList<long?>? value) => Add(name, value);

    /// <inheritdoc cref="Set(string, long?)"/>
    public InlineSqlSession Set(string name, IReadOnlyList<string?>? value) => Add(name, value);

    /// <inheritdoc cref="Set(string, long?)"/>
    public InlineSqlSession Set(string name, IReadOnlyList<double?>? value) => Add(name, value);

    /// <inheritdoc cref="Set(string, long?)"/>
    public InlineSqlSession Set(string name, IReadOnlyList<bool?>? value) => Add(name, value);

    /// <summary>Runs <c>INSERT</c>, <c>UPDATE</c> or <c>DELETE</c> with the parameters set so far; returns the changed rows.</summary>
    public async Task<long> ExecuteAsync(string sql)
    {
        Dictionary<string, object?>? taken = Take();
        budget.Suspend();

        try
        {
            return await inner.ExecuteAsync(sql, taken).ConfigureAwait(false);
        }
        finally
        {
            budget.Resume();
        }
    }

    /// <summary>Runs <c>SELECT</c> with the parameters set so far; the rows are copies that stay valid.</summary>
    public async Task<IReadOnlyList<IRow>> QueryAsync(string sql)
    {
        Dictionary<string, object?>? taken = Take();
        budget.Suspend();

        try
        {
            return await inner.QueryAsync(sql, taken).ConfigureAwait(false);
        }
        finally
        {
            budget.Resume();
        }
    }

    private InlineSqlSession Add(string name, object? value)
    {
        if (string.IsNullOrEmpty(name))
            throw new ArgumentException("A parameter needs a name.", nameof(name));

        (parameters ??= new Dictionary<string, object?>(StringComparer.Ordinal))[name] = value;
        return this;
    }

    private Dictionary<string, object?>? Take()
    {
        Dictionary<string, object?>? taken = parameters;
        parameters = null;
        return taken;
    }
}
