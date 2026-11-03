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
///
/// <para><b>Memory.</b> A state grows with its group, and one group's state cannot spill. Each state
/// reports its size (<see cref="IMeasuredAggregateState"/>), so a spilling GROUP BY counts the growth
/// of many groups, and refuses a group whose elements pass <see cref="JsonbLimits.MaxDocumentBytes"/>
/// with <see cref="CamusDBErrorCodes.JsonbValueTooLarge"/>.</para>
/// </summary>
internal static class JsonbAggregates
{
    private const int StateSizeBytes = 256;

    /// <summary>Estimated heap overhead of one retained item: its array object and its list slot.</summary>
    private const int ItemOverheadBytes = 40;

    /// <summary>Estimated heap overhead of one dictionary entry and its key string object.</summary>
    private const int EntryOverheadBytes = 64;

    /// <summary>
    /// The items of one <c>jsonb_agg</c> group, in arrival order. They must all be kept until the group
    /// finishes, so the state counts their bytes and refuses a group whose elements pass
    /// <see cref="JsonbLimits.MaxDocumentBytes"/>.
    /// </summary>
    private sealed class ArrayState : IMeasuredAggregateState
    {
        public readonly List<byte[]> Items = [];

        private long elementBytes;

        public long RetainedBytes => elementBytes + (long)Items.Count * ItemOverheadBytes;

        public void Add(string calledName, byte[] item)
        {
            elementBytes += item.Length;
            CheckSize(calledName, elementBytes);
            Items.Add(item);
        }
    }

    /// <summary>
    /// The pairs of one <c>jsonb_object_agg</c> group. A repeated key replaces its value as the row
    /// arrives, because only the last value is in the result: the state is bounded by the distinct
    /// keys, not by the rows. Its size is counted and checked as <see cref="ArrayState"/> does.
    /// </summary>
    private sealed class ObjectState : IMeasuredAggregateState
    {
        public readonly Dictionary<string, byte[]> Pairs = new(StringComparer.Ordinal);

        private long elementBytes;

        private long keyChars;

        public long RetainedBytes => elementBytes + keyChars * sizeof(char) + (long)Pairs.Count * EntryOverheadBytes;

        public void Add(string calledName, string key, byte[] item)
        {
            if (Pairs.TryGetValue(key, out byte[]? previous))
            {
                elementBytes += item.Length - previous.Length;
            }
            else
            {
                elementBytes += key.Length + item.Length;
                keyChars += key.Length;
            }

            CheckSize(calledName, elementBytes);
            Pairs[key] = item;
        }

        public ColumnValue Finish()
        {
            if (Pairs.Count == 0)
                return ColumnValue.Null;

            List<KeyValuePair<string, byte[]>> pairs = new(Pairs.Count);
            foreach (KeyValuePair<string, byte[]> pair in Pairs)
                pairs.Add(pair);

            return ColumnValue.FromJsonbCell(JsonbBuilder.Object(pairs));
        }
    }

    private static void CheckSize(string calledName, long elementBytes)
    {
        if (elementBytes > JsonbLimits.MaxDocumentBytes)
            throw new CamusDBException(CamusDBErrorCodes.JsonbValueTooLarge,
                $"{calledName}: total size of jsonb elements exceeds the maximum of {JsonbLimits.MaxDocumentBytes} bytes");
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
                accumulate: static (_, state, calledName, arguments) =>
                    ((ArrayState)state).Add(calledName, JsonbScalarFunctions.ToItem(arguments[0])),
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
                    ((ObjectState)state).Add(calledName, Key(calledName, arguments[0]), JsonbScalarFunctions.ToItem(arguments[1])),
                finish: static (_, state, _) => ((ObjectState)state).Finish());
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
