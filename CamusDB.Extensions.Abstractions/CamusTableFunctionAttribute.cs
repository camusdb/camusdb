/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;

namespace CamusDB.Extensions;

/// <summary>
/// Marks a public static method that returns <c>IEnumerable&lt;T&gt;</c> as a set-returning function
/// that stands as a relation in <c>FROM</c>, the same as the built-in <c>generate_series</c>:
///
/// <code>
/// [CamusTableFunction("split_words", Column = "word")]
/// public static IEnumerable&lt;string&gt; SplitWords(string text) { ... }
///
/// -- SELECT word FROM split_words('a b c')
/// -- SELECT w.word FROM split_words('a b') AS w JOIN dictionary d ON d.term = w.word
/// </code>
///
/// <para><b>Signature.</b> The parameters follow the scalar-function value mapping (see
/// <see cref="CamusFunctionAttribute"/>), arrays included. <c>T</c> is one mapped type and makes the
/// one output column. A NULL argument for a parameter that does not accept NULL gives zero rows, and
/// so does a <c>null</c> return value. A <c>null</c> element is a NULL row value.</para>
///
/// <para><b>Laziness.</b> The server pulls one value at a time and can stop at any point (a
/// <c>LIMIT</c>, a cancelled statement, an error elsewhere in the query). It then disposes the
/// enumerator, so a <c>finally</c> block or a <c>using</c> in an iterator method runs. Do not build the
/// whole output first. The server checks the statement's cancellation between two values, so a value
/// that takes long to compute delays a cancel.</para>
///
/// <para><b>Re-reads.</b> A join can read a relation more than once. A function that is not
/// <see cref="IsVolatile"/> is called again for each read, so it must return the same values for the
/// same arguments. The output of a volatile function is kept for the statement after its first read,
/// so every read sees the same rows.</para>
///
/// <para><b>Errors.</b> An exception from the method or from the enumerator ends the statement with a
/// CamusDB error that carries the function name.</para>
/// </summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = false)]
public sealed class CamusTableFunctionAttribute : Attribute
{
    /// <summary>Creates the attribute with the SQL name the function is called by in <c>FROM</c>.</summary>
    /// <param name="name">The SQL name. It must not collide with a built-in function, an aggregate, or
    /// another function of the same extension.</param>
    public CamusTableFunctionAttribute(string name)
    {
        Name = name;
    }

    /// <summary>The SQL name.</summary>
    public string Name { get; }

    /// <summary>The name of the output column. Without it, the function name (or the alias in
    /// <c>FROM</c>) names the column, the same rule as <c>generate_series</c>.</summary>
    public string? Column { get; set; }

    /// <summary>True when the output can differ between calls with the same arguments. The server then
    /// keeps the output for the statement after the first read, and never caches a result that uses
    /// it.</summary>
    public bool IsVolatile { get; set; }

    /// <summary>
    /// The name of a public static method of the same class that returns the exact row count for the
    /// same arguments as <c>long?</c> (null when it cannot tell). The planner uses it as the row estimate
    /// when the arguments are constants; without it the function gets the default estimate. The method
    /// takes the same parameters as the function and must be cheap and free of effects.
    /// </summary>
    public string? CountRowsMethod { get; set; }
}
