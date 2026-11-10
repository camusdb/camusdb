/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Extensions.Inline;

/// <summary>One column of the table a trigger is on, as the generated row classes expose it.</summary>
/// <param name="Name">The column name, which becomes the property name when it is a C# identifier.</param>
/// <param name="Type">The SQL type.</param>
/// <param name="ElementType">The element type of an array column, else null.</param>
internal sealed record InlineTriggerColumn(string Name, ColumnType Type, ColumnType? ElementType);

/// <summary>
/// Everything the compiler needs to build one inline trigger unit. A unit is keyed by all of it, so
/// the same body on a table with other columns, or in a database with other functions, is another
/// unit: the generated row classes and function stubs are part of the code.
/// </summary>
internal sealed class InlineTriggerSource
{
    /// <summary>The trigger name, for messages.</summary>
    public required string Name { get; init; }

    /// <summary>The table the trigger is on, for messages.</summary>
    public required string TableName { get; init; }

    /// <summary>The C# body.</summary>
    public required string Source { get; init; }

    /// <summary>The columns of the table, in schema order.</summary>
    public required IReadOnlyList<InlineTriggerColumn> Columns { get; init; }

    /// <summary>The inline functions of the database at compile time; each becomes a method the body can call.</summary>
    public required IReadOnlyList<InlineFunctionDefinition> Functions { get; init; }
}
