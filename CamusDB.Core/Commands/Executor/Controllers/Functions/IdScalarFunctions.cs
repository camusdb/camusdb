
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

internal static class IdScalarFunctions
{
    public static void Register(ScalarFunctionRegistry registry)
    {
        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "gen_id",
            MinArity = 0,
            MaxArity = 0,
            IsVolatile = true,
            Evaluator = EvaluateGenId,
            InferReturnType = _ => ColumnType.Id,
        });
    }

    private static ColumnValue EvaluateGenId(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        string generatedId = ObjectIdGenerator.Generate().ToString();
        return new ColumnValue(ColumnType.Id, generatedId);
    }
}
