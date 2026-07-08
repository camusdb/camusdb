
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

internal sealed class ScalarFunctionRegistry
{
    private readonly Dictionary<string, ScalarFunctionDescriptor> functions = new(StringComparer.OrdinalIgnoreCase);

    public void Register(ScalarFunctionDescriptor descriptor)
    {
        foreach (string name in descriptor.AllNames)
        {
            if (functions.ContainsKey(name))
                throw new InvalidOperationException($"Scalar function '{name}' is already registered");

            functions[name] = descriptor;
        }
    }

    public bool TryGet(string functionName, out ScalarFunctionDescriptor descriptor)
    {
        return functions.TryGetValue(functionName, out descriptor!);
    }

    public bool IsRegisteredScalarFunction(string functionName)
    {
        return functions.ContainsKey(functionName);
    }

    public ColumnType InferReturnType(string functionName, IReadOnlyList<ColumnType> argumentTypes)
    {
        if (!TryGet(functionName, out ScalarFunctionDescriptor? descriptor))
            return ColumnType.String;

        return descriptor.InferReturnType(argumentTypes);
    }

    public static ScalarFunctionRegistry CreateDefault()
    {
        ScalarFunctionRegistry registry = new();
        IdScalarFunctions.Register(registry);
        DateTimeScalarFunctions.Register(registry);
        MathScalarFunctions.Register(registry);
        StringScalarFunctions.Register(registry);
        JsonScalarFunctions.Register(registry);
        CastScalarFunctions.Register(registry);
        NullScalarFunctions.Register(registry);
        return registry;
    }
}
