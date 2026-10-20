/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;

namespace CamusDB.Extensions;

/// <summary>
/// Marks a public static method as a SQL scalar function. The server discovers the method when it
/// loads the extension at startup and registers it next to the built-in functions.
///
/// <para><b>Signature.</b> Parameters and the return value use the plain CLR types of the value
/// mapping: <c>long</c>, <c>double</c>, <c>float</c>, <c>bool</c>, <c>string</c>, <c>Guid</c>,
/// <c>byte[]</c>, <c>DateTime</c> and <c>decimal</c>. Any other type (an array, <c>object</c>, a
/// custom class) fails the server's boot with the method name.</para>
///
/// <para><b>NULL.</b> A parameter of type <c>Nullable&lt;T&gt;</c>, or a reference parameter
/// annotated nullable (<c>string?</c>, <c>byte[]?</c>), receives SQL NULL as <c>null</c>. For any
/// other parameter, a NULL argument makes the result NULL and the method is not called. A
/// <c>null</c> return value is SQL NULL.</para>
///
/// <para><b>Volatility.</b> Set <see cref="IsVolatile"/> when the result can differ for the same
/// arguments (random values, clocks, external state). A non-volatile function is assumed
/// deterministic: its result can be cached, it is allowed in a CHECK constraint, and in a column
/// DEFAULT it is evaluated once when the table is created and stored as a constant. A volatile
/// function in a DEFAULT is evaluated for each inserted row.</para>
///
/// <para><b>Errors.</b> An exception thrown by the method ends the statement with a CamusDB
/// error that carries the function name and the exception message.</para>
///
/// <para>The method is compiled into a typed delegate once at startup, so each call costs one
/// delegate invocation, the same as a built-in function. The method must be thread-safe:
/// concurrent statements call it at the same time.</para>
/// </summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = false)]
public sealed class CamusFunctionAttribute : Attribute
{
    /// <summary>Creates the attribute with the SQL name the function is called by.</summary>
    /// <param name="name">The SQL name. Names are case-insensitive and must not collide with a
    /// built-in function or a function of another loaded extension.</param>
    public CamusFunctionAttribute(string name)
    {
        Name = name;
    }

    /// <summary>The SQL name the function is called by.</summary>
    public string Name { get; }

    /// <summary>Additional SQL names for the same function. Each alias obeys the same collision
    /// rules as <see cref="Name"/>.</summary>
    public string[] Aliases { get; set; } = Array.Empty<string>();

    /// <summary>True when the result can differ between calls with the same arguments. See the
    /// class summary for what volatility changes.</summary>
    public bool IsVolatile { get; set; }
}
