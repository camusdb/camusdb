
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct CreateTableTicket
{
    public string DatabaseName { get; }

    public string TableName { get; }

    public ColumnInfo[] Columns { get; }

    public ConstraintInfo[] Constraints { get; }

    /// <summary>
    /// CHECK constraints collected from both column-level inline declarations (desugared to
    /// named constraints) and explicit table-level <c>CONSTRAINT name CHECK (cond)</c> clauses.
    /// Empty when no CHECK constraints were declared.
    /// </summary>
    public CheckConstraintInfo[] CheckConstraints { get; }

    public bool IfNotExists { get; }

    /// <summary>
    /// Table-level comment from a trailing <c>) COMMENT '…'</c>. Null when none was declared. There
    /// is no removal form here — clearing a table comment is <c>COMMENT ON TABLE … IS NULL</c>.
    /// </summary>
    public string? Comment { get; }

    /// <summary>
    /// Whether the relation being created is an ordinary table or a materialized view. A materialized
    /// view is created through this same ticket because it <i>is</i> a relation — columns, rows,
    /// indexes and statistics all behave identically — so reusing the path is what gives it deferred
    /// drop, relink, backup and ANALYZE without any new integration surface.
    /// </summary>
    public Catalogs.Models.RelationKind Kind { get; }

    /// <summary>
    /// The query that populates a materialized view. Null for an ordinary table, and required when
    /// <see cref="Kind"/> is <c>MaterializedView</c> — a materialized view whose definition was lost
    /// could never be refreshed again.
    /// </summary>
    public Catalogs.Models.ViewDefinition? ViewDefinition { get; }

    public CreateTableTicket(
        string databaseName,
        string tableName,
        ColumnInfo[] columns,
        ConstraintInfo[] constraints,
        bool ifNotExists,
        CheckConstraintInfo[]? checkConstraints = null,
        string? comment = null,
        IReadOnlyDictionary<string, string>? settings = null,
        Catalogs.Models.RelationKind kind = Catalogs.Models.RelationKind.Table,
        Catalogs.Models.ViewDefinition? viewDefinition = null
    )
    {
        Comment = comment;
        Settings = settings;
        DatabaseName = databaseName;
        TableName = tableName;
        Columns = columns;
        Constraints = constraints;
        CheckConstraints = checkConstraints ?? [];
        IfNotExists = ifNotExists;
        Kind = kind;
        ViewDefinition = viewDefinition;
    }

    /// <summary>
    /// Table storage parameters supplied inline as <c>WITH (key = value, ...)</c>, or null when none
    /// were given. Present so <c>SHOW CREATE TABLE</c> renders a statement that re-creates the same
    /// table — a rendering that silently dropped its settings would look faithful and reproduce a
    /// differently-configured table.
    /// </summary>
    public IReadOnlyDictionary<string, string>? Settings { get; }
}

