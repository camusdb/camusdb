/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

/// <summary>
/// One row with one column that nothing reads: the source of a FROM-less operand of a set operation.
///
/// <para>A FROM-less <c>SELECT 1</c> runs as a statement on a path of its own, which evaluates the
/// projections once against an empty row. An operand of <c>SELECT 1 UNION SELECT 2</c> must instead
/// run through the bound-query path, as every other operand does, so the query creator gives it this
/// function as its FROM. Projection, filtering, grouping and the subquery rewrites then work on it
/// without a second code path.</para>
///
/// <para><b>Not a user function.</b> Its name holds asterisks, which no unquoted identifier can, so a
/// statement cannot name it by accident. It reads no storage and needs no privilege.</para>
/// </summary>
internal sealed class SingleRowTableFunction : TableFunction
{
    public static readonly SingleRowTableFunction Instance = new();

    /// <summary>The name, which is also the name of the relation and of its one column.</summary>
    public const string InternalName = "*single_row*";

    public override string Name => InternalName;

    public override int MinArguments => 0;

    public override int MaxArguments => 0;

    public override ColumnType OutputType => ColumnType.Bool;

    public override IEnumerable<ColumnValue> Generate(IReadOnlyList<ColumnValue> arguments) => [ColumnValue.True];

    public override long? CountRows(IReadOnlyList<ColumnValue> arguments) => 1;
}
