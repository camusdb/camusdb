/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// Turns the foreign-key nodes of a CREATE TABLE or ALTER TABLE statement into
/// <see cref="ForeignKeyInfo"/> values. Shared by both ticket creators, so a clause means the same
/// thing in every position it can be written.
///
/// <para>The parser keeps the column-level form flat: <c>REFERENCES</c> and each of its clauses are
/// separate column constraints, because a clause tail after <c>REFERENCES</c> would be ambiguous with
/// the column's other constraints. <see cref="CollectColumnLevel"/> puts them back together: a clause
/// belongs to the nearest <c>REFERENCES</c> before it on the same column.</para>
///
/// <para>This reader checks what the statement text alone decides: the clause words, repeated clauses,
/// column counts and the database qualifier. Everything that needs the schema — that the referenced
/// table and columns exist, that they carry a unique index, that the types match — happens where the
/// schema is locked. Whether a clause is supported is decided by the validator
/// (<see cref="ForeignKeyInfo.RequireSupported"/>), so the HTTP API gets the same answer.</para>
/// </summary>
internal static class ForeignKeyAstReader
{
    /// <summary>A foreign key under construction: the pieces read so far and the clauses already seen.</summary>
    private sealed class Draft
    {
        public string? Name;
        public required string[] Columns;
        public required string ReferencedTable;
        public required string[] ReferencedColumns;
        public ForeignKeyAction OnDelete = ForeignKeyAction.NoAction;
        public ForeignKeyAction OnUpdate = ForeignKeyAction.NoAction;
        public ForeignKeyMatch Match = ForeignKeyMatch.Simple;
        public bool Deferrable;
        public bool InitiallyDeferred;
        public readonly HashSet<string> SeenClauses = new(StringComparer.Ordinal);

        public ForeignKeyInfo Finish(string name) =>
            new(name, Columns, ReferencedTable, ReferencedColumns, OnDelete, OnUpdate, Match, Deferrable, InitiallyDeferred);
    }

    /// <summary>
    /// Reads every foreign key of a CREATE TABLE item list, in source order, and gives each a name.
    /// </summary>
    /// <param name="itemList">The statement's column and constraint list.</param>
    /// <param name="tableName">The table being created, for default names.</param>
    /// <param name="databaseName">The current database; a reference qualified with another is refused.</param>
    /// <param name="knownColumns">The table's columns; a referencing column must be one of them.</param>
    /// <param name="takenNames">
    /// Names that other constraints of the table already use (CHECK and named NOT NULL). A default name
    /// avoids them; an explicit name that repeats one is refused.
    /// </param>
    public static ForeignKeyInfo[] ReadCreateTable(
        NodeAst itemList,
        string tableName,
        string databaseName,
        IReadOnlyCollection<string> knownColumns,
        IEnumerable<string> takenNames)
    {
        List<Draft> drafts = [];
        CollectItems(itemList, databaseName, drafts);

        if (drafts.Count == 0)
            return [];

        HashSet<string> columns = new(knownColumns, StringComparer.OrdinalIgnoreCase);

        foreach (Draft draft in drafts)
        {
            foreach (string column in draft.Columns)
            {
                if (!columns.Contains(column))
                    throw new CamusDBException(
                        CamusDBErrorCodes.UnknownColumn,
                        $"Column '{column}' named in a foreign key does not exist in table '{tableName}'");
            }
        }

        return Name(drafts, tableName, takenNames);
    }

    /// <summary>
    /// Reads the constraint of <c>ALTER TABLE … ADD [CONSTRAINT name] FOREIGN KEY …</c> and gives it a
    /// name that the table's existing constraints do not use.
    /// </summary>
    public static ForeignKeyInfo ReadAlterTable(
        NodeAst constraint,
        string tableName,
        string databaseName,
        IReadOnlyCollection<string> knownColumns,
        IEnumerable<string> takenNames)
    {
        Draft draft = ReadTableLevel(constraint, databaseName);

        HashSet<string> columns = new(knownColumns, StringComparer.OrdinalIgnoreCase);
        foreach (string column in draft.Columns)
        {
            if (!columns.Contains(column))
                throw new CamusDBException(
                    CamusDBErrorCodes.UnknownColumn,
                    $"Column '{column}' named in a foreign key does not exist in table '{tableName}'");
        }

        return Name([draft], tableName, takenNames)[0];
    }

    private static void CollectItems(NodeAst node, string databaseName, List<Draft> drafts)
    {
        switch (node.nodeType)
        {
            case NodeType.CreateTableItemList:
                if (node.leftAst is not null)
                    CollectItems(node.leftAst, databaseName, drafts);
                if (node.rightAst is not null)
                    CollectItems(node.rightAst, databaseName, drafts);
                return;

            case NodeType.CreateTableItem:
                if (node.extendedOne is not null)
                    CollectColumnLevel(node.extendedOne, node.leftAst?.yytext ?? "", databaseName, drafts);
                return;

            case NodeType.CreateTableConstraintForeignKey:
                drafts.Add(ReadTableLevel(node, databaseName));
                return;
        }
    }

    /// <summary>
    /// Walks one column's constraints in source order. The list is left-recursive, so visiting the left
    /// branch before the right one yields the order the user wrote.
    /// </summary>
    private static void CollectColumnLevel(NodeAst node, string columnName, string databaseName, List<Draft> drafts)
    {
        Draft? current = null;
        CollectColumnLevel(node, columnName, databaseName, drafts, ref current);
    }

    private static void CollectColumnLevel(NodeAst node, string columnName, string databaseName, List<Draft> drafts, ref Draft? current)
    {
        switch (node.nodeType)
        {
            case NodeType.CreateTableFieldConstraintList:
                if (node.leftAst is not null)
                    CollectColumnLevel(node.leftAst, columnName, databaseName, drafts, ref current);
                if (node.rightAst is not null)
                    CollectColumnLevel(node.rightAst, columnName, databaseName, drafts, ref current);
                return;

            case NodeType.ConstraintForeignKey:
                string[] referencedColumns = ReadColumnList(node.rightAst);
                if (referencedColumns.Length > 1)
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidForeignKeyDefinition,
                        $"Column '{columnName}' REFERENCES {referencedColumns.Length} columns; a column-level " +
                        "reference names at most one. Use a table-level FOREIGN KEY for a composite key.");

                current = new()
                {
                    Name = node.yytext,
                    Columns = [columnName],
                    ReferencedTable = ReadTableName(node.leftAst!, databaseName),
                    ReferencedColumns = referencedColumns
                };
                drafts.Add(current);
                return;

            case NodeType.ForeignKeyOption:
                if (current is null)
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInput,
                        $"'{DescribeClause(node.yytext!)}' on column '{columnName}' must follow a REFERENCES clause");

                ApplyClause(current, node.yytext!);
                return;
        }
    }

    private static Draft ReadTableLevel(NodeAst node, string databaseName)
    {
        string[] columns = ReadColumnList(node.leftAst);
        string[] referencedColumns = ReadColumnList(node.extendedOne);

        HashSet<string> distinct = new(StringComparer.OrdinalIgnoreCase);
        foreach (string column in columns)
        {
            if (!distinct.Add(column))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidForeignKeyDefinition,
                    $"Column '{column}' appears more than once in a FOREIGN KEY");
        }

        if (referencedColumns.Length > 0 && referencedColumns.Length != columns.Length)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidForeignKeyDefinition,
                $"FOREIGN KEY names {columns.Length} referencing column(s) but {referencedColumns.Length} referenced column(s)");

        Draft draft = new()
        {
            Name = node.yytext,
            Columns = columns,
            ReferencedTable = ReadTableName(node.rightAst!, databaseName),
            ReferencedColumns = referencedColumns
        };

        ApplyClauses(draft, node.extendedTwo);
        return draft;
    }

    private static void ApplyClauses(Draft draft, NodeAst? node)
    {
        if (node is null)
            return;

        if (node.nodeType == NodeType.ForeignKeyOptionList)
        {
            ApplyClauses(draft, node.leftAst);
            ApplyClauses(draft, node.rightAst);
            return;
        }

        ApplyClause(draft, node.yytext!);
    }

    /// <summary>Applies one <c>kind:value</c> clause, refusing a clause kind given twice.</summary>
    private static void ApplyClause(Draft draft, string clause)
    {
        int separator = clause.IndexOf(':');
        string kind = clause[..separator];
        string value = clause[(separator + 1)..];

        // DEFERRABLE and NOT DEFERRABLE are one clause kind, as are the two INITIALLY forms.
        if (!draft.SeenClauses.Add(kind))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Foreign key clause '{DescribeKind(kind)}' is given more than once");

        switch (kind)
        {
            case "on_delete":
                draft.OnDelete = ParseAction(value);
                break;

            case "on_update":
                draft.OnUpdate = ParseAction(value);
                break;

            case "match":
                draft.Match = value switch
                {
                    "full" => ForeignKeyMatch.Full,
                    "partial" => ForeignKeyMatch.Partial,
                    _ => ForeignKeyMatch.Simple
                };
                break;

            case "deferrable":
                draft.Deferrable = value == "true";
                break;

            case "initially":
                draft.InitiallyDeferred = value == "deferred";
                break;

            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, $"Unknown foreign key clause '{clause}'");
        }
    }

    private static ForeignKeyAction ParseAction(string value) => value switch
    {
        "no_action" => ForeignKeyAction.NoAction,
        "restrict" => ForeignKeyAction.Restrict,
        "cascade" => ForeignKeyAction.Cascade,
        "set_null" => ForeignKeyAction.SetNull,
        "set_default" => ForeignKeyAction.SetDefault,
        _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, $"Unknown referential action '{value}'")
    };

    private static string DescribeKind(string kind) => kind switch
    {
        "on_delete" => "ON DELETE",
        "on_update" => "ON UPDATE",
        "match" => "MATCH",
        "deferrable" => "[NOT] DEFERRABLE",
        "initially" => "INITIALLY",
        _ => kind
    };

    private static string DescribeClause(string clause)
    {
        int separator = clause.IndexOf(':');
        string kind = clause[..separator];
        string value = clause[(separator + 1)..].Replace('_', ' ').ToUpperInvariant();

        return kind switch
        {
            "deferrable" => value == "TRUE" ? "DEFERRABLE" : "NOT DEFERRABLE",
            _ => DescribeKind(kind) + " " + value
        };
    }

    /// <summary>
    /// The referenced table's name. A reference always stays inside the current database, so a name
    /// qualified with another database is refused; one qualified with the current database is accepted
    /// and the qualifier dropped.
    /// </summary>
    private static string ReadTableName(NodeAst node, string databaseName)
    {
        string name = node.yytext!;
        int dot = name.LastIndexOf('.');

        if (dot < 0)
            return name;

        string qualifier = name[..dot];
        if (!string.Equals(qualifier, databaseName, StringComparison.OrdinalIgnoreCase))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidForeignKeyDefinition,
                $"A foreign key cannot reference '{name}': the referenced table must be in the current database '{databaseName}'");

        return name[(dot + 1)..];
    }

    private static string[] ReadColumnList(NodeAst? node)
    {
        if (node is null)
            return [];

        List<string> names = [];
        ReadColumnList(node, names);
        return [.. names];
    }

    private static void ReadColumnList(NodeAst node, List<string> names)
    {
        if (node.nodeType == NodeType.IndexIdentifierList)
        {
            ReadColumnList(node.leftAst!, names);
            ReadColumnList(node.rightAst!, names);
            return;
        }

        names.Add(node.yytext!);
    }

    /// <summary>
    /// Keeps each explicit name, refusing one that repeats another constraint of the table, and gives
    /// every unnamed constraint PostgreSQL's default, <c>{table}_{column}[_{column}…]_fkey</c>, with a
    /// numeric suffix when that is taken.
    /// </summary>
    private static ForeignKeyInfo[] Name(List<Draft> drafts, string tableName, IEnumerable<string> takenNames)
    {
        HashSet<string> taken = new(takenNames, StringComparer.OrdinalIgnoreCase);

        // Explicit names first, so a default can never claim a name the user wrote later in the statement.
        foreach (Draft draft in drafts)
        {
            if (draft.Name is { Length: > 0 } name && !taken.Add(name))
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Duplicate constraint name '{name}'");
        }

        ForeignKeyInfo[] result = new ForeignKeyInfo[drafts.Count];

        for (int i = 0; i < drafts.Count; i++)
        {
            Draft draft = drafts[i];
            string name = draft.Name is { Length: > 0 } explicitName ? explicitName : DefaultName(tableName, draft.Columns, taken);
            result[i] = draft.Finish(name);
        }

        return result;
    }

    private static string DefaultName(string tableName, string[] columns, HashSet<string> taken)
    {
        string stem = $"{tableName}_{string.Join('_', columns)}_fkey";

        if (taken.Add(stem))
            return stem;

        for (int suffix = 1; ; suffix++)
        {
            string candidate = stem + suffix;
            if (taken.Add(candidate))
                return candidate;
        }
    }
}
