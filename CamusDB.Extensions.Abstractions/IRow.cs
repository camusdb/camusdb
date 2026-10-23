/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;

namespace CamusDB.Extensions;

/// <summary>
/// Read access to one row. The accessor reads the server's row directly, without a copy, so it
/// is valid only during the call that received it. Do not keep a reference to it.
///
/// <para>Each typed getter returns <c>null</c> for SQL NULL and for a column the row does not
/// have. A getter of the wrong type for the column's value throws
/// <see cref="InvalidCastException"/>. The value mapping is: Integer64 → <c>long</c>,
/// Float64 → <c>double</c>, Float32 → <c>float</c>, Bool → <c>bool</c>, String and Id →
/// <c>string</c>, Uuid → <c>Guid</c>, Bytes → <c>byte[]</c>, Date and DateTime →
/// <c>DateTime</c> (UTC), Numeric → <c>decimal</c>, and <c>array(E)</c> → <c>IReadOnlyList&lt;T&gt;</c>
/// of the element mapping. <see cref="GetValue"/> returns an array as a list with nullable elements
/// (<c>IReadOnlyList&lt;long?&gt;</c>, <c>IReadOnlyList&lt;string?&gt;</c>); <see cref="GetArray{T}"/>
/// reads it with the element type you choose.</para>
/// </summary>
public interface IRow
{
    /// <summary>The names of the columns this row carries.</summary>
    IEnumerable<string> ColumnNames { get; }

    /// <summary>True when the row carries the column (with a value or NULL).</summary>
    bool Contains(string column);

    /// <summary>True when the column is SQL NULL or the row does not carry it.</summary>
    bool IsNull(string column);

    /// <summary>The value as a plain CLR object of the value mapping, or <c>null</c>.</summary>
    object? GetValue(string column);

    /// <summary>An Integer64 value.</summary>
    long? GetInt64(string column);

    /// <summary>A Float64 value. An Integer64 value converts.</summary>
    double? GetDouble(string column);

    /// <summary>A Float32 value.</summary>
    float? GetSingle(string column);

    /// <summary>A Bool value.</summary>
    bool? GetBoolean(string column);

    /// <summary>A String or Id value.</summary>
    string? GetString(string column);

    /// <summary>A Uuid value.</summary>
    Guid? GetGuid(string column);

    /// <summary>A Bytes value.</summary>
    byte[]? GetBytes(string column);

    /// <summary>A Date or DateTime value, in UTC.</summary>
    DateTime? GetDateTime(string column);

    /// <summary>A Numeric value. A value that does not fit <c>decimal</c> throws
    /// <see cref="OverflowException"/>.</summary>
    decimal? GetDecimal(string column);

    /// <summary>
    /// An array value as a new list of <typeparamref name="T"/>, a mapped element type: for example
    /// <c>GetArray&lt;long&gt;("ids")</c> or <c>GetArray&lt;string?&gt;("tags")</c>. A NULL element needs a
    /// nullable <typeparamref name="T"/> (<c>long?</c>) or a reference type; in an element type that
    /// cannot hold it, and for an element of another type, the call throws
    /// <see cref="InvalidCastException"/>. The list is a copy.
    /// </summary>
    IReadOnlyList<T>? GetArray<T>(string column);
}

/// <summary>
/// A row a BEFORE INSERT or BEFORE UPDATE trigger can change. In any other trigger
/// <see cref="IsWritable"/> is false and every setter throws
/// <see cref="InvalidOperationException"/>.
///
/// <para>A value set here is converted to the column's type when the row is validated, exactly
/// as a value in an <c>INSERT</c> or <c>UPDATE</c> statement is. A value the column cannot hold
/// fails the statement.</para>
/// </summary>
public interface IWritableRow : IRow
{
    /// <summary>True in a BEFORE INSERT or BEFORE UPDATE trigger.</summary>
    bool IsWritable { get; }

    /// <summary>Sets the column to SQL NULL.</summary>
    void SetNull(string column);

    /// <summary>Sets an integer value.</summary>
    void Set(string column, long value);

    /// <summary>Sets a floating-point value.</summary>
    void Set(string column, double value);

    /// <summary>Sets a 32-bit floating-point value.</summary>
    void Set(string column, float value);

    /// <summary>Sets a boolean value.</summary>
    void Set(string column, bool value);

    /// <summary>Sets a string value. <c>null</c> sets SQL NULL.</summary>
    void Set(string column, string? value);

    /// <summary>Sets a UUID value.</summary>
    void Set(string column, Guid value);

    /// <summary>Sets a bytes value. <c>null</c> sets SQL NULL.</summary>
    void Set(string column, byte[]? value);

    /// <summary>Sets a date-time value. A value that is not UTC is converted to UTC.</summary>
    void Set(string column, DateTime value);

    /// <summary>Sets a numeric value.</summary>
    void Set(string column, decimal value);

    /// <summary>
    /// Sets an array value from a list of a mapped element type (<c>long</c>, <c>long?</c>,
    /// <c>string</c>, ...). The element type of the value comes from <typeparamref name="T"/>.
    /// <c>null</c> sets SQL NULL. The list is copied, so a later change to it has no effect.
    /// </summary>
    void SetArray<T>(string column, IReadOnlyList<T>? values);
}
