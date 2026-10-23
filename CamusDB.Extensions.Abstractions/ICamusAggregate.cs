/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;

namespace CamusDB.Extensions;

/// <summary>
/// An aggregate function for <c>GROUP BY</c> and for global aggregates (<c>SELECT geomean(x) FROM t</c>).
/// Mark the implementing class with <see cref="CamusAggregateAttribute"/>:
///
/// <code>
/// [CamusAggregate("geomean")]
/// public sealed class GeoMean : ICamusAggregate&lt;GeoMean.State&gt;
/// {
///     public sealed class State { public double LogSum; public long Count; }
///
///     public State Create() =&gt; new State();
///
///     public void Accumulate(State state, double x)
///     {
///         if (x &gt; 0) { state.LogSum += Math.Log(x); state.Count++; }
///     }
///
///     public double? Finish(State state) =&gt; state.Count == 0 ? null : Math.Exp(state.LogSum / state.Count);
/// }
/// </code>
///
/// <para><b>Three methods besides <see cref="Create"/>, found by name.</b> Their types are the
/// aggregate's SQL signature, so the interface cannot declare them:</para>
/// <list type="bullet">
/// <item><c>void Accumulate(TState state, ...)</c> takes the SQL arguments after the state. Their types
/// follow the scalar-function value mapping (see <see cref="CamusFunctionAttribute"/>), arrays
/// included. It must be the only public method of that name.</item>
/// <item><c>R Finish(TState state)</c> returns the result, of a mapped type <c>R</c>; <c>null</c> is SQL
/// NULL.</item>
/// <item><c>void Merge(TState into, TState other)</c> is optional. It folds one partial state into
/// another. This server version does not call it yet: an extension aggregate always runs on the node
/// that runs the statement, above the step that gathers rows from other nodes.</item>
/// </list>
///
/// <para><b>NULL.</b> A row whose argument is NULL for a parameter that does not accept NULL is
/// skipped, the SQL rule for built-in aggregates. A <c>Nullable&lt;T&gt;</c> or nullable reference
/// parameter receives the NULL.</para>
///
/// <para><b>Groups.</b> The server calls <see cref="Create"/> once for each group, then
/// <c>Accumulate</c> once for each row of the group, then <c>Finish</c> once. A global aggregate over
/// zero rows calls <c>Create</c> and then <c>Finish</c>. A group of <c>GROUP BY</c> exists only when
/// a row has its key.</para>
///
/// <para><b>Threading and state.</b> The server creates one instance of the class for each engine and
/// shares it between statements, so keep all per-group data in <typeparamref name="TState"/> and none
/// in fields. One state object is used by one thread at a time.</para>
///
/// <para><b>Errors.</b> An exception from any of the methods ends the statement with a CamusDB error
/// that carries the aggregate name.</para>
///
/// <para><b>Where it is allowed.</b> In the select list and in <c>HAVING</c>, like a built-in
/// aggregate. A CHECK constraint and a column DEFAULT refuse it.</para>
/// </summary>
/// <typeparam name="TState">The per-group state: a class the aggregate creates and changes in place.</typeparam>
public interface ICamusAggregate<TState> where TState : class
{
    /// <summary>Creates the state of a new group.</summary>
    TState Create();
}

/// <summary>
/// Marks a public class that implements <see cref="ICamusAggregate{TState}"/> as a SQL aggregate. The
/// class needs a public parameterless constructor.
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class CamusAggregateAttribute : Attribute
{
    /// <summary>Creates the attribute with the SQL name.</summary>
    /// <param name="name">The SQL name. It must not collide with a built-in function or aggregate, or
    /// with another function of the same extension.</param>
    public CamusAggregateAttribute(string name)
    {
        Name = name;
    }

    /// <summary>The SQL name.</summary>
    public string Name { get; }

    /// <summary>
    /// The estimated size in bytes of one state object, for the memory budget of a <c>GROUP BY</c>:
    /// the server cannot measure a state, so it charges this for each group. Zero (the default)
    /// charges 64 bytes. Give a larger value for a state that holds a collection, so that a
    /// <c>GROUP BY</c> with many groups spills to disk before it runs out of memory.
    /// </summary>
    public int StateSizeHint { get; set; }
}
