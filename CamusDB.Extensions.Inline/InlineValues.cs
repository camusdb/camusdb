/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;

namespace CamusDB.Extensions.Inline;

/// <summary>
/// Makes the result of an inline SQL function independent of the node that computed it. The method
/// the server writes around the body of a function passes a <c>DATETIME</c> result through
/// <see cref="Utc(DateTime)"/>.
///
/// <para><b>Why.</b> A <c>DateTime</c> that the body builds (<c>new DateTime(2020, 1, 1)</c>) has no
/// kind. A conversion of such a value to UTC reads the time zone of the machine, so two nodes in two
/// zones would store two different values. Here a value with no kind is taken as UTC, digit for
/// digit, on every node. The arguments of a function arrive as UTC, and the body has no way to make
/// a local time.</para>
/// </summary>
public static class InlineValues
{
    /// <summary>Returns <paramref name="value"/> marked as UTC, with the same clock digits.</summary>
    public static DateTime Utc(DateTime value) =>
        value.Kind == DateTimeKind.Utc ? value : DateTime.SpecifyKind(value, DateTimeKind.Utc);

    /// <summary>The same as <see cref="Utc(DateTime)"/> for a value that can be NULL.</summary>
    public static DateTime? Utc(DateTime? value) => value is { } present ? Utc(present) : null;

    /// <summary>Returns a list with every element marked as UTC, or null for a null list.</summary>
    public static IReadOnlyList<DateTime?>? Utc(IReadOnlyList<DateTime?>? values)
    {
        if (values is null)
            return null;

        DateTime?[] result = new DateTime?[values.Count];

        for (int i = 0; i < result.Length; i++)
            result[i] = Utc(values[i]);

        return result;
    }
}
