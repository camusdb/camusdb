/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Threading.Tasks;

namespace CamusDB.Extensions;

/// <summary>
/// A row trigger that receives OLD and NEW as instances of its own row class
/// <typeparamref name="TRow"/>. It binds, fires and fails exactly as an <see cref="ICamusTrigger"/>
/// does (see that interface for the firing rules); only the row form differs.
///
/// <code>
/// public sealed class OrderRow
/// {
///     public long Id { get; set; }
///     public string Name { get; set; } = "";
///     [CamusColumn("total_cents")] public long? Total { get; set; }
/// }
///
/// public sealed class TrimOrderName : ICamusTrigger&lt;OrderRow&gt;
/// {
///     public Task FireAsync(ITriggerContext&lt;OrderRow&gt; context)
///     {
///         if (context.New is { } row)
///             row.Name = row.Name.Trim();   // BEFORE INSERT or UPDATE: written back
///         return Task.CompletedTask;
///     }
/// }
/// </code>
///
/// <para><b>Why a generic interface and not a generic method.</b> A generic method lets the caller
/// choose the type argument, and the server has no row type to choose. The trigger class fixes it.</para>
///
/// <para><b>Mapping.</b> Each public property with a public getter and setter maps to the column of
/// the same name (case-insensitive), or to the name in its <see cref="CamusColumnAttribute"/>. Its
/// type follows the value mapping of <see cref="IRow"/>, arrays included. Columns the class does not
/// map are ignored, so a class can model part of a row. <c>CREATE TRIGGER</c> refuses a property with
/// no column and a property whose type does not fit its column. A column a typed trigger maps cannot
/// be dropped or renamed while the trigger exists.</para>
///
/// <para><b>NULL.</b> A <c>Nullable&lt;T&gt;</c> property, or a reference property annotated nullable
/// (<c>string?</c>), receives NULL as <c>null</c>. A NULL in any other property fails the statement
/// with an error that names the column. A column that has no value when the trigger fires (a column
/// an INSERT omits, or the generated id in BEFORE INSERT) is NULL, so map such a column to a nullable
/// property.</para>
///
/// <para><b>BEFORE triggers write back changed properties only.</b> After the call the server
/// compares each mapped property of NEW with its value before the call and writes back only the
/// changed ones, through the same path as <see cref="IWritableRow.Set(string, long)"/>: the values are
/// converted to the column types and the row is validated as usual. An unchanged property costs no
/// write. An AFTER trigger's changes to its row objects are ignored.</para>
///
/// <para><b>Cost.</b> Each fired row creates one object for each image (OLD, NEW) and converts each
/// mapped column. Prefer the untyped <see cref="ICamusTrigger"/> on a hot table: it reads the row
/// without a copy.</para>
/// </summary>
/// <typeparam name="TRow">The row class: a public class with a public parameterless constructor.</typeparam>
public interface ICamusTrigger<TRow> where TRow : class, new()
{
    /// <summary>Handles one row event.</summary>
    /// <param name="context">The event, the typed OLD and NEW rows, the untyped rows, the SQL session
    /// and the cancellation token.</param>
    Task FireAsync(ITriggerContext<TRow> context);
}

/// <summary>
/// What a typed trigger receives for one row event. <see cref="Old"/> and <see cref="New"/> hide the
/// untyped rows of <see cref="ITriggerContext"/>; cast the context to <see cref="ITriggerContext"/> to
/// read those.
/// </summary>
public interface ITriggerContext<TRow> : ITriggerContext where TRow : class
{
    /// <summary>The row before the change, for UPDATE and DELETE. <c>null</c> for INSERT.</summary>
    new TRow? Old { get; }

    /// <summary>
    /// The row after the change, for INSERT and UPDATE. <c>null</c> for DELETE. A BEFORE trigger can
    /// change its properties; see <see cref="ICamusTrigger{TRow}"/> for the write-back rule.
    /// </summary>
    new TRow? New { get; }
}
