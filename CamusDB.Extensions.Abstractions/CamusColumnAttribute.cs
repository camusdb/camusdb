/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;

namespace CamusDB.Extensions;

/// <summary>
/// Maps a property of a typed trigger's row class (<see cref="ICamusTrigger{TRow}"/>) to a column
/// whose name differs from the property name: <c>[CamusColumn("total_cents")] public long? Total</c>.
/// Without it a property maps to the column of its own name, compared case-insensitively.
/// </summary>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = false)]
public sealed class CamusColumnAttribute : Attribute
{
    /// <summary>Creates the attribute with the column name.</summary>
    /// <param name="name">The column name, compared case-insensitively.</param>
    public CamusColumnAttribute(string name)
    {
        Name = name;
    }

    /// <summary>The column name.</summary>
    public string Name { get; }
}
