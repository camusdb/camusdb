/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Jsonb;

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

/// <summary>
/// The PostgreSQL set-returning jsonb functions, as relations in <c>FROM</c>:
/// <c>jsonb_each</c> and <c>jsonb_each_text</c> (columns <c>key</c>, <c>value</c>),
/// <c>jsonb_object_keys</c> (one column, named after the relation), and
/// <c>jsonb_array_elements</c> / <c>jsonb_array_elements_text</c> (one column, <c>value</c>). Each
/// <c>json_*</c> spelling is the same function under its own name.
///
/// <para>The argument is evaluated once before the first row, as for every function in <c>FROM</c>:
/// a literal, a parameter or an expression without columns. A column of another relation of the same
/// <c>FROM</c> would be <c>LATERAL</c>, which CamusDB does not support. Strict: a NULL document gives no
/// rows. A document of the wrong kind is an error with the PostgreSQL wording, raised before the first
/// row. Object keys come out in the stored order, which is the PostgreSQL order.</para>
/// </summary>
internal abstract class JsonbTableFunction : TableFunction
{
    protected JsonbTableFunction(string name) => Name = name;

    public override string Name { get; }

    public override int MinArguments => 1;

    public override int MaxArguments => 1;

    public static IEnumerable<TableFunction> All()
    {
        foreach (string prefix in new[] { "jsonb", "json" })
        {
            yield return new EachFunction(prefix + "_each", text: false);
            yield return new EachFunction(prefix + "_each_text", text: true);
            yield return new ObjectKeysFunction(prefix + "_object_keys");
            yield return new ArrayElementsFunction(prefix + "_array_elements", text: false);
            yield return new ArrayElementsFunction(prefix + "_array_elements_text", text: true);
        }
    }

    /// <summary>The CBOR item of the document argument, or null for a NULL argument.</summary>
    protected byte[]? Document(IReadOnlyList<ColumnValue> arguments) => arguments[0].Type switch
    {
        ColumnType.Null => null,
        ColumnType.Jsonb => JsonbDocument.Item(arguments[0].BytesValue).ToArray(),
        ColumnType.String => JsonbDocument.Item(JsonbEncoder.FromText(arguments[0].StrValue!)).ToArray(),
        _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
            $"Function '{Name}' argument 1 must be jsonb, got {arguments[0].Type}"),
    };

    /// <summary>The (key, value) ranges of an object item, or of the elements of an array item.</summary>
    protected static List<(Range Key, Range Value)> Children(byte[] item)
    {
        int pos = 0;
        int count = JsonbCbor.ReadContainerHeader(item, ref pos, out bool isObject);
        List<(Range, Range)> children = new(count);
        for (int i = 0; i < count; i++)
        {
            int keyStart = pos;
            if (isObject)
                JsonbCbor.ReadText(item, ref pos);
            Range key = keyStart..pos;
            int end = JsonbCbor.SkipItem(item, pos);
            children.Add((key, pos..end));
            pos = end;
        }

        return children;
    }

    protected static ColumnValue KeyText(byte[] item, Range key)
    {
        int pos = key.Start.Value;
        return new ColumnValue(ColumnType.String, Encoding.UTF8.GetString(JsonbCbor.ReadText(item, ref pos)));
    }

    protected static ColumnValue Value(byte[] item, Range value, bool text)
    {
        ReadOnlySpan<byte> slice = item.AsSpan()[value];
        if (!text)
            return ColumnValue.FromJsonbCell(JsonbEncoder.ToCell(slice));

        string? sqlText = JsonbDocument.ToSqlText(slice);
        return sqlText is null ? ColumnValue.Null : new ColumnValue(ColumnType.String, sqlText);
    }

    protected static CamusDBException Error(string message) => new(CamusDBErrorCodes.InvalidInput, message);

    /// <summary>Checks the kind of the document now, so a wrong argument fails before any row.</summary>
    protected abstract void CheckKind(byte[] item);

    protected abstract IEnumerable<ColumnValue[]> Rows(byte[] item);

    public override IEnumerable<ColumnValue[]> GenerateRows(IReadOnlyList<ColumnValue> arguments)
    {
        byte[]? item = Document(arguments);
        if (item is null)
            return [];

        CheckKind(item);
        return Rows(item);
    }

    public override IEnumerable<ColumnValue> Generate(IReadOnlyList<ColumnValue> arguments)
    {
        foreach (ColumnValue[] row in GenerateRows(arguments))
            yield return row[0];
    }

    public override long? CountRows(IReadOnlyList<ColumnValue> arguments)
    {
        try
        {
            byte[]? item = Document(arguments);
            if (item is null)
                return 0;

            CheckKind(item);
            int pos = 0;
            return JsonbCbor.ReadContainerHeader(item, ref pos, out _);
        }
        catch (CamusDBException)
        {
            // The error is raised again, to the caller, when the rows are generated.
            return null;
        }
    }

    private sealed class EachFunction(string name, bool text) : JsonbTableFunction(name)
    {
        private readonly (string, ColumnType)[] columns = [("key", ColumnType.String), ("value", text ? ColumnType.String : ColumnType.Jsonb)];

        public override ColumnType OutputType => ColumnType.String;

        public override IReadOnlyList<(string Name, ColumnType Type)>? OutputColumns => columns;

        protected override void CheckKind(byte[] item)
        {
            if (JsonbDocument.Kind(item) != JsonbKind.Object)
                throw Error($"cannot call {Name} on a non-object");
        }

        protected override IEnumerable<ColumnValue[]> Rows(byte[] item)
        {
            foreach ((Range key, Range value) in Children(item))
                yield return [KeyText(item, key), Value(item, value, text)];
        }
    }

    private sealed class ObjectKeysFunction(string name) : JsonbTableFunction(name)
    {
        public override ColumnType OutputType => ColumnType.String;

        protected override void CheckKind(byte[] item)
        {
            JsonbKind kind = JsonbDocument.Kind(item);
            if (kind == JsonbKind.Array)
                throw Error($"cannot call {Name} on an array");
            if (kind != JsonbKind.Object)
                throw Error($"cannot call {Name} on a scalar");
        }

        protected override IEnumerable<ColumnValue[]> Rows(byte[] item)
        {
            foreach ((Range key, _) in Children(item))
                yield return [KeyText(item, key)];
        }
    }

    private sealed class ArrayElementsFunction(string name, bool text) : JsonbTableFunction(name)
    {
        public override ColumnType OutputType => text ? ColumnType.String : ColumnType.Jsonb;

        public override string? DefaultColumnName => "value";

        protected override void CheckKind(byte[] item)
        {
            JsonbKind kind = JsonbDocument.Kind(item);
            if (kind == JsonbKind.Object)
                throw Error("cannot extract elements from an object");
            if (kind != JsonbKind.Array)
                throw Error("cannot extract elements from a scalar");
        }

        protected override IEnumerable<ColumnValue[]> Rows(byte[] item)
        {
            foreach ((_, Range value) in Children(item))
                yield return [Value(item, value, text)];
        }
    }
}
