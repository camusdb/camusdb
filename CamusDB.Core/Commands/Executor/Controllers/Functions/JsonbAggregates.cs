/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Jsonb;
using CamusDB.Core.Extensions;

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

/// <summary>
/// The PostgreSQL aggregates <c>jsonb_agg(value)</c> and <c>jsonb_object_agg(key, value)</c>, with
/// their <c>json_*</c> spellings.
///
/// <para><b>On the state-object path.</b> They are built as <see cref="ExtensionAggregate"/>
/// instances, so the aggregator runs them through the general workspace with one state per group,
/// the path an extension aggregate takes: grouped, global and spilled plans, and never split below a
/// gather (a state has no wire form). <see cref="Queries.AggregateFunctionCatalog"/> knows them as
/// built-ins, so no extension can claim the names and no extension needs to be loaded.</para>
///
/// <para>PostgreSQL rules: a group of zero rows gives SQL NULL; a NULL value becomes JSON null; a
/// NULL key of <c>jsonb_object_agg</c> is an error; a repeated key keeps its last value. The order of
/// <c>jsonb_agg</c> is the order the rows reach the aggregate; <c>ORDER BY</c> inside the call is not
/// supported.</para>
/// </summary>
internal static class JsonbAggregates
{
    private const int StateSizeBytes = 256;

    private sealed class ArrayState
    {
        public readonly List<byte[]> Items = [];
    }

    private sealed class ObjectState
    {
        public readonly List<KeyValuePair<string, byte[]>> Pairs = [];
    }

    public static IEnumerable<ExtensionAggregate> All()
    {
        foreach (string prefix in new[] { "jsonb", "json" })
        {
            yield return new ExtensionAggregate(
                prefix + "_agg", 1, 1, ColumnType.Jsonb, null, StateSizeBytes, hasMerge: false,
                signature: prefix + "_agg(any) RETURNS jsonb",
                factory: static () => new object(),
                create: static (_, _) => new ArrayState(),
                accumulate: static (_, state, _, arguments) =>
                    ((ArrayState)state).Items.Add(JsonbScalarFunctions.ToItem(arguments[0])),
                finish: static (_, state, _) =>
                    ((ArrayState)state).Items is { Count: > 0 } items
                        ? ColumnValue.FromJsonbCell(JsonbBuilder.Array(items))
                        : ColumnValue.Null);

            yield return new ExtensionAggregate(
                prefix + "_object_agg", 2, 2, ColumnType.Jsonb, null, StateSizeBytes, hasMerge: false,
                signature: prefix + "_object_agg(any, any) RETURNS jsonb",
                factory: static () => new object(),
                create: static (_, _) => new ObjectState(),
                accumulate: static (_, state, calledName, arguments) =>
                    ((ObjectState)state).Pairs.Add(new(Key(calledName, arguments[0]), JsonbScalarFunctions.ToItem(arguments[1]))),
                finish: static (_, state, _) =>
                    ((ObjectState)state).Pairs is { Count: > 0 } pairs
                        ? ColumnValue.FromJsonbCell(JsonbBuilder.Object(pairs))
                        : ColumnValue.Null);
        }
    }

    private static string Key(string calledName, ColumnValue key)
    {
        if (key.Type == ColumnType.Null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"{calledName}: field name must not be null");

        if (key.Type == ColumnType.Jsonb || !ScalarTextForm.TryFormat(key, out string text))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                $"{calledName}: key value must be scalar, not array, composite, or json");

        return text;
    }
}
