/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;

namespace CamusDB.Extensions;

/// <summary>
/// Marks a public class that implements <see cref="ICamusProcedure"/> as a stored procedure that
/// <c>CALL name(args)</c> runs. The class needs a public parameterless constructor. The server
/// creates one instance when it loads the extension and shares it between statements, so the
/// class must be thread-safe and must not keep per-call state in fields.
///
/// <para>The argument count is checked before the procedure runs: a call with fewer than
/// <see cref="MinArguments"/> or more than <see cref="MaxArguments"/> arguments fails.</para>
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class CamusProcedureAttribute : Attribute
{
    /// <summary>Creates the attribute with the SQL name <c>CALL</c> uses.</summary>
    /// <param name="name">The SQL name. Names are case-insensitive and unique across the loaded
    /// extensions.</param>
    public CamusProcedureAttribute(string name)
    {
        Name = name;
    }

    /// <summary>The SQL name <c>CALL</c> uses.</summary>
    public string Name { get; }

    /// <summary>The smallest accepted argument count. The default is 0.</summary>
    public int MinArguments { get; set; }

    /// <summary>The largest accepted argument count, or -1 for no upper limit. The default is
    /// -1.</summary>
    public int MaxArguments { get; set; } = -1;
}
