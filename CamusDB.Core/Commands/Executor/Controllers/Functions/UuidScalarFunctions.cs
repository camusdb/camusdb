
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
/// Generators for <see cref="ColumnType.Uuid"/> values. Both are marked volatile so a query that
/// calls them is never served from the query result cache (each call must produce a fresh value).
///
/// <para>
/// <c>gen_uuid_v7()</c> is time-ordered (48-bit Unix-millisecond prefix), so using it for a primary
/// key keeps sequential inserts local in the key space — friendlier to range routing than the fully
/// random <c>gen_uuid_v4()</c>, which scatters writes across the whole index.
/// </para>
/// </summary>
internal static class UuidScalarFunctions
{
    public static void Register(ScalarFunctionRegistry registry)
    {
        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "gen_uuid_v4",
            MinArity = 0,
            MaxArity = 0,
            IsVolatile = true,
            Evaluator = static (_, _) => ColumnValue.FromUuid(Guid.NewGuid()),
            InferReturnType = _ => ColumnType.Uuid,
        });

        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "gen_uuid_v7",
            MinArity = 0,
            MaxArity = 0,
            IsVolatile = true,
            Evaluator = static (_, _) => ColumnValue.FromUuid(Guid.CreateVersion7()),
            InferReturnType = _ => ColumnType.Uuid,
        });
    }
}
