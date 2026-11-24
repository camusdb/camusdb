/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>A schema as text, for tests that compare the schemas of two databases or of two moments.</summary>
internal static class SchemaShape
{
    /// <summary>
    /// The shared schema of <paramref name="database"/> as text, by name, so that two databases and two moments
    /// of one database compare: every relation, column, index, constraint, setting, comment and state.
    /// </summary>
    public static string Of(DatabaseDescriptor database)
    {
        Schema schema = database.SharedSchema;
        StringBuilder text = new();

        string TableName(string? id) => schema.Tables.Values.FirstOrDefault(t => t.Id == id)?.Name ?? "?";

        foreach (TableSchema table in schema.Tables.Values.OrderBy(t => t.Name, StringComparer.Ordinal))
        {
            string ColumnNames(IEnumerable<string>? ids) =>
                string.Join(",", (ids ?? []).Select(id => table.Columns!.Find(c => c.Id == id)?.Name ?? id));

            text.AppendLine($"table {table.Name} {table.Kind} '{table.Comment}'");

            foreach (TableColumnSchema column in table.Columns!)
            {
                string? drawsFrom = schema.Sequences.Values.FirstOrDefault(s => s.Id == column.DefaultSequenceId)?.Name ?? column.DefaultSequenceId;
                text.AppendLine($"  column {column.Name} {column.Type} notnull={column.NotNull}/{column.NotNullConstraintName} default={column.DefaultFunction}/{drawsFrom} {column.State} '{column.Comment}' {column.Storage}");
            }

            foreach (TableIndexSchema index in (table.Indexes ?? []).OrderBy(i => i.Name, StringComparer.Ordinal))
                text.AppendLine($"  index {index.Name} {index.Type} ({ColumnNames(index.ColumnIds)}) include ({ColumnNames(index.IncludeColumnIds)}) {index.State} owned={index.OwnerConstraintId is not null} '{index.Comment}'");

            foreach (CheckConstraintSchema check in (table.CheckConstraints ?? []).OrderBy(c => c.Name, StringComparer.Ordinal))
                text.AppendLine($"  check {check.Name} {check.Expression}");

            foreach (ForeignKeySchema foreignKey in (table.ForeignKeys ?? []).OrderBy(f => f.Name, StringComparer.Ordinal))
                text.AppendLine($"  fk {foreignKey.Name} ({ColumnNames(foreignKey.ColumnIds)}) -> {TableName(foreignKey.ReferencedTableId)} {foreignKey.State}");

            foreach ((string key, string value) in (table.Settings ?? []).OrderBy(s => s.Key, StringComparer.Ordinal))
                text.AppendLine($"  setting {key}={value}");
        }

        foreach (ViewSchema view in schema.Views.Values.OrderBy(v => v.Name, StringComparer.Ordinal))
            text.AppendLine($"view {view.Name} '{view.Comment}'");

        foreach (SequenceSchema sequence in schema.Sequences.Values.OrderBy(s => s.Name, StringComparer.Ordinal))
            text.AppendLine($"sequence {sequence.Name} {sequence.Increment} owner={(sequence.OwnedByTableId is null ? "" : TableName(sequence.OwnedByTableId))}");

        foreach (InlineFunctionDefinition function in schema.Functions.Values.OrderBy(f => f.Name, StringComparer.Ordinal))
            text.AppendLine($"function {function.Name} {function.Source}");

        return text.ToString();
    }
}
