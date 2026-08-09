
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// How a view's body is written when the user creates it, and how it must be enforced on writes
/// that pass through the view. Numeric values are persisted and must remain stable.
/// </summary>
public enum CheckOptionKind
{
    /// <summary>No <c>WITH CHECK OPTION</c>: a write through the view may produce a row the view
    /// itself cannot see.</summary>
    None = 0,

    /// <summary>Rows written through the view must satisfy <b>this</b> view's predicate only.</summary>
    Local = 1,

    /// <summary>Rows written through the view must satisfy this view's predicate <b>and</b> every
    /// underlying view's predicate, regardless of those views' own check options. This is the
    /// meaning of a bare <c>WITH CHECK OPTION</c>, matching PostgreSQL.</summary>
    Cascaded = 2
}

/// <summary>
/// One output column of a view, as frozen at creation time.
/// </summary>
/// <remarks>
/// A view's column set is deliberately <b>not</b> re-derived at reference time. If it were, adding a
/// column to a base table would silently change the shape of every <c>SELECT *</c> view over it, and
/// a dependent view or a client's column mapping would change meaning without any statement having
/// been issued. PostgreSQL avoids this by storing a parsed tree with <c>*</c> already expanded;
/// CamusDB stores this list plus the normalized body for the same reason.
/// </remarks>
public sealed class ViewColumnSchema
{
    /// <summary>The column's name as the view exposes it — the body's output name, or the
    /// corresponding entry of an explicit column-alias list when one was given.</summary>
    public string Name { get; set; } = "";

    /// <summary>The type the body's expression produces, derived by the same code that reports a
    /// query's client-facing column metadata, so a view column always has the type a plain
    /// <c>SELECT</c> of that expression would report.</summary>
    public ColumnType Type { get; set; }

    /// <summary>
    /// The base-table column this output column is a direct, unmodified reference to, or null when
    /// the column is computed (an expression, a function call, an aggregate). Only non-null columns
    /// are writable through an auto-updatable view — an <c>UPDATE</c> that targets a computed column
    /// has no base column to write to and is refused.
    /// </summary>
    public string? SourceColumnName { get; set; }
}

/// <summary>
/// The stored form of a view or materialized-view body.
///
/// <para>Both the SQL text and the resolved dependency ids are kept, and they must be updated
/// together. The text is what <c>SHOW CREATE VIEW</c> prints and what re-parsing rebuilds the AST
/// from; the ids are what makes a base-table <c>RENAME</c> non-breaking and what
/// <c>DROP … RESTRICT</c> consults. Letting them drift apart silently corrupts one or the other:
/// stale ids make a dependency check pass when it should fail, stale text makes the view
/// unresolvable.</para>
/// </summary>
public sealed class ViewDefinition
{
    /// <summary>
    /// The view body as SQL, <b>normalized at creation time</b>: <c>*</c> expanded to an explicit
    /// column list and identifiers rendered canonically.
    ///
    /// <para>Storing the normalized form rather than what the user typed is what freezes the view's
    /// shape (see <see cref="ViewColumnSchema"/>) and what makes a rename a targeted AST edit rather
    /// than a string substitution — the latter would happily rewrite a matching word inside a string
    /// literal or an unrelated table's column list.</para>
    /// </summary>
    public string Sql { get; set; } = "";

    /// <summary>Output columns, in order, frozen at creation. <c>CREATE OR REPLACE VIEW</c> may only
    /// append to this list; see <c>ViewShapeComparer</c>.</summary>
    public List<ViewColumnSchema>? Columns { get; set; }

    /// <summary>
    /// Ids of the tables this view reads directly. Stored by id, never by name, because ids are
    /// immutable: that is what lets a base table be renamed without breaking the view, and it makes
    /// "which views depend on table T" an O(views) scan rather than a re-parse of every body.
    /// </summary>
    public List<string>? DependsOnTableIds { get; set; }

    /// <summary>Ids of the views this view reads directly. Separate from
    /// <see cref="DependsOnTableIds"/> because the two live in different name maps and different
    /// meta key families.</summary>
    public List<string>? DependsOnViewIds { get; set; }

    /// <summary>Whether writes through this view must satisfy its predicate. Meaningful only on an
    /// auto-updatable view.</summary>
    public CheckOptionKind CheckOption { get; set; }

    /// <summary>
    /// The user that owns the view. Base-relation privileges for a query that goes through this view
    /// are checked against <b>this</b> principal, not the caller's — the owner's-rights model that
    /// makes a view usable for row- and column-level security. Null when the view was created with
    /// authentication disabled, in which case no base-relation check applies anyway.
    /// </summary>
    public string? Owner { get; set; }
}
