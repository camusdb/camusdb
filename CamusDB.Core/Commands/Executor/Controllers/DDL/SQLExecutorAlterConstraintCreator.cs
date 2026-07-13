
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// Translates <c>AlterTableAddConstraintCheck</c>, <c>AlterTableDropConstraint</c>,
/// <c>AlterTableSetNotNull</c>, and <c>AlterTableDropNotNull</c> AST nodes into
/// <see cref="AlterConstraintTicket"/> instances. Validates CHECK expressions at parse time
/// (no subqueries, aggregates, or volatile functions; all referenced columns must exist).
/// </summary>
internal sealed class SQLExecutorAlterConstraintCreator : SQLExecutorBaseCreator
{
    /// <summary>
    /// Builds an <see cref="AlterConstraintTicket"/> from an ALTER TABLE constraint AST node.
    /// Handles ADD CONSTRAINT CHECK, DROP CONSTRAINT, SET NOT NULL, and DROP NOT NULL.
    /// </summary>
    internal AlterConstraintTicket CreateAlterConstraintTicket(
        ExecuteSQLTicket sqlTicket,
        NodeAst ast,
        TableSchema tableSchema)
    {
        string tableName = ast.leftAst!.yytext!;

        if (ast.nodeType == NodeType.AlterTableSetNotNull)
        {
            string columnName = ast.rightAst!.yytext!;
            return new AlterConstraintTicket(
                databaseName: sqlTicket.DatabaseName,
                tableName: tableName,
                constraintName: "",
                expression: null,
                referencedColumns: null,
                operation: AlterConstraintOperation.SetNotNull,
                columnName: columnName
            );
        }

        if (ast.nodeType == NodeType.AlterTableDropNotNull)
        {
            string columnName = ast.rightAst!.yytext!;
            return new AlterConstraintTicket(
                databaseName: sqlTicket.DatabaseName,
                tableName: tableName,
                constraintName: "",
                expression: null,
                referencedColumns: null,
                operation: AlterConstraintOperation.DropNotNull,
                columnName: columnName
            );
        }

        string constraintName = ast.yytext!;

        if (ast.nodeType == NodeType.AlterTableDropConstraint)
        {
            return new AlterConstraintTicket(
                databaseName: sqlTicket.DatabaseName,
                tableName: tableName,
                constraintName: constraintName,
                expression: null,
                referencedColumns: null,
                operation: AlterConstraintOperation.DropConstraint
            );
        }

        // AlterTableAddConstraintCheck: rightAst = condition AST
        NodeAst condition = ast.rightAst!;

        HashSet<string> columnNames = [];
        if (tableSchema.Columns is not null)
            foreach (TableColumnSchema col in tableSchema.Columns)
                columnNames.Add(col.Name);

        SQLExecutorCreateTableCreator.ValidateCheckConditionForAlter(condition, columnNames);

        string expression = CheckConditionRenderer.Render(condition);
        HashSet<string> referenced = [];
        SQLExecutorCreateTableCreator.ExtractReferencedColumnsPublic(condition, referenced);

        return new AlterConstraintTicket(
            databaseName: sqlTicket.DatabaseName,
            tableName: tableName,
            constraintName: constraintName,
            expression: expression,
            referencedColumns: [.. referenced],
            operation: AlterConstraintOperation.AddCheck
        );
    }
}
