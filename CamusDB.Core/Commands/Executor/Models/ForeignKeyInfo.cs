/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// One foreign key as a DDL statement declared it, carried through a ticket to the code that validates
/// it against the schema and persists it as a <see cref="ForeignKeySchema"/>. Built from SQL by the
/// CREATE TABLE and ALTER TABLE ticket creators, and from JSON by the HTTP API.
///
/// <para>This is a separate carrier from <see cref="ConstraintInfo"/> on purpose: that type feeds index
/// construction, and the index builder refuses any constraint it cannot turn into an index. CHECK
/// constraints use their own carrier for the same reason.</para>
///
/// <para>Everything here is a name. Names become ids only when the schema is at hand, in the DDL path,
/// because the referenced table and its columns are resolved there under the schema lock.</para>
/// </summary>
public sealed class ForeignKeyInfo
{
    /// <summary>The constraint name: the one the statement gave, or a PostgreSQL-style default.</summary>
    public string Name { get; }

    /// <summary>Referencing (child) column names, in constraint order.</summary>
    public string[] Columns { get; }

    /// <summary>Referenced (parent) table name, without a database qualifier.</summary>
    public string ReferencedTable { get; }

    /// <summary>
    /// Referenced (parent) column names, paired by position with <see cref="Columns"/>. Empty when the
    /// statement named none, which means the parent's primary key, as in PostgreSQL.
    /// </summary>
    public string[] ReferencedColumns { get; }

    /// <summary>The <c>ON DELETE</c> action. <see cref="ForeignKeyAction.NoAction"/> when not given.</summary>
    public ForeignKeyAction OnDelete { get; }

    /// <summary>The <c>ON UPDATE</c> action. <see cref="ForeignKeyAction.NoAction"/> when not given.</summary>
    public ForeignKeyAction OnUpdate { get; }

    /// <summary>The <c>MATCH</c> type. <see cref="ForeignKeyMatch.Simple"/> when not given.</summary>
    public ForeignKeyMatch Match { get; }

    /// <summary>True when the statement said <c>DEFERRABLE</c>.</summary>
    public bool Deferrable { get; }

    /// <summary>True when the statement said <c>INITIALLY DEFERRED</c>.</summary>
    public bool InitiallyDeferred { get; }

    public ForeignKeyInfo(
        string name,
        string[] columns,
        string referencedTable,
        string[] referencedColumns,
        ForeignKeyAction onDelete = ForeignKeyAction.NoAction,
        ForeignKeyAction onUpdate = ForeignKeyAction.NoAction,
        ForeignKeyMatch match = ForeignKeyMatch.Simple,
        bool deferrable = false,
        bool initiallyDeferred = false)
    {
        Name = name;
        Columns = columns;
        ReferencedTable = referencedTable;
        ReferencedColumns = referencedColumns;
        OnDelete = onDelete;
        OnUpdate = onUpdate;
        Match = match;
        Deferrable = deferrable;
        InitiallyDeferred = initiallyDeferred;
    }

    /// <summary>
    /// Refuses a clause that CamusDB parses but does not enforce. Only <c>NO ACTION</c> and
    /// <c>RESTRICT</c>, <c>MATCH SIMPLE</c>, and immediate checking are supported. The clause is refused
    /// rather than ignored: a constraint that silently dropped its <c>ON DELETE CASCADE</c> would keep
    /// rows the user asked to have removed.
    /// </summary>
    public void RequireSupported()
    {
        RequireSupportedAction(OnDelete, "ON DELETE");
        RequireSupportedAction(OnUpdate, "ON UPDATE");

        if (Match != ForeignKeyMatch.Simple)
            throw new CamusDBException(
                CamusDBErrorCodes.FeatureNotSupported,
                $"Foreign key '{Name}': MATCH {Match.ToString().ToUpperInvariant()} is not supported. " +
                "Only MATCH SIMPLE is supported: a NULL in any referencing column satisfies the constraint.");

        if (Deferrable || InitiallyDeferred)
            throw new CamusDBException(
                CamusDBErrorCodes.FeatureNotSupported,
                $"Foreign key '{Name}': deferred checking is not supported. " +
                "Constraints are checked at the end of each statement.");
    }

    private void RequireSupportedAction(ForeignKeyAction action, string clause)
    {
        if (action is ForeignKeyAction.NoAction or ForeignKeyAction.Restrict)
            return;

        string words = action switch
        {
            ForeignKeyAction.Cascade => "CASCADE",
            ForeignKeyAction.SetNull => "SET NULL",
            ForeignKeyAction.SetDefault => "SET DEFAULT",
            _ => action.ToString()
        };

        throw new CamusDBException(
            CamusDBErrorCodes.FeatureNotSupported,
            $"Foreign key '{Name}': {clause} {words} is not supported. " +
            "Only NO ACTION and RESTRICT are supported; both refuse the change while child rows exist.");
    }
}
