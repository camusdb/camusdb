/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;

namespace CamusDB.Extensions.Inline;

/// <summary>The budget of an inline function call that a check found exhausted.</summary>
public enum CallBudgetKind
{
    /// <summary>The call ran longer than its wall-clock limit.</summary>
    Time = 0,

    /// <summary>The call allocated more managed memory than its limit.</summary>
    Allocation = 1,

    /// <summary>The call asked for an array with more elements than the limit.</summary>
    ArrayLength = 2,

    /// <summary>The call nested its own calls until the thread's stack was almost full.</summary>
    Stack = 3,

    /// <summary>The runtime could not give the call the memory it asked for.</summary>
    OutOfMemory = 4,
}

/// <summary>
/// Thrown by <see cref="CallBudget"/> when an inline function call goes over one of its budgets. The
/// server's adapter turns it into the statement error for an exhausted budget.
///
/// <para>The body of an inline function cannot catch it: the server rewrites every <c>catch</c> clause
/// of the body with a filter that lets this exception through
/// (<see cref="CallBudget.IsBudgetException"/>). A body that could catch it would run on after its
/// deadline.</para>
/// </summary>
public sealed class CallBudgetExceededException : Exception
{
    /// <summary>Creates the exception for one exhausted budget.</summary>
    public CallBudgetExceededException(CallBudgetKind kind, string message)
        : base(message)
    {
        Kind = kind;
    }

    /// <summary>The budget that was exhausted.</summary>
    public CallBudgetKind Kind { get; }
}
