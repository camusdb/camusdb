/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// The size limits of one stored view or materialized-view definition, checked at <c>CREATE</c> and
/// <c>CREATE OR REPLACE</c> — where the author can still fix the definition — rather than at some
/// later reader's first <c>SELECT</c>.
/// </summary>
/// <remarks>
/// <para>What one definition costs is bounded here; what a statement's expansion of many definitions
/// costs is bounded by the expansion budget in <see cref="Queries.ViewExpander"/>. Both are needed:
/// a definition within every limit here can still be referenced twice by each of ten views stacked on
/// top of it.</para>
///
/// <para>Measured on the <b>stored, normalized</b> body, because that is what every later expansion
/// parses and copies — not the text the user typed, which can be shorter (<c>*</c> expands to a
/// column list) or longer (whitespace and comments are not stored).</para>
/// </remarks>
internal static class ViewDefinitionLimits
{
    internal static void Require(CamusDBOptions options, string viewName, ViewDefinition definition)
    {
        long maxBytes = options.MaxViewDefinitionBytes;
        if (maxBytes > 0)
        {
            int bytes = Encoding.UTF8.GetByteCount(definition.Sql);
            if (bytes > maxBytes)
                throw Refuse(viewName, $"its body is {bytes} bytes, over the {maxBytes} that max_view_definition_bytes allows");
        }

        int maxColumns = options.MaxViewOutputColumns;
        int columns = definition.Columns?.Count ?? 0;
        if (maxColumns > 0 && columns > maxColumns)
            throw Refuse(viewName, $"it has {columns} output columns, over the {maxColumns} that max_view_output_columns allows");

        int maxDependencies = options.MaxViewDirectDependencies;
        int dependencies = (definition.DependsOnTableIds?.Count ?? 0) + (definition.DependsOnViewIds?.Count ?? 0);
        if (maxDependencies > 0 && dependencies > maxDependencies)
            throw Refuse(viewName,
                $"it reads {dependencies} relations directly, over the {maxDependencies} that max_view_direct_dependencies allows");

        long maxNodes = options.MaxViewDefinitionNodes;
        if (maxNodes > 0)
        {
            long nodes = NodeAstWalk.Count(SQLParserProcessor.Parse(definition.Sql));
            if (nodes > maxNodes)
                throw Refuse(viewName, $"its body has {nodes} syntax-tree nodes, over the {maxNodes} that max_view_definition_nodes allows");
        }
    }

    private static CamusDBException Refuse(string viewName, string why)
        => new(
            CamusDBErrorCodes.ViewComplexityLimitExceeded,
            $"The definition of '{viewName}' is too large: {why}. Simplify it, or raise the setting.");
}
