/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Linq.Expressions;
using System.Reflection;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Extensions;

namespace CamusDB.Core.Extensions;

/// <summary>
/// The row mapping of a typed trigger (<see cref="ICamusTrigger{TRow}"/>): it builds the row objects
/// for OLD and NEW, calls the trigger, and, for a BEFORE trigger, writes the changed properties of NEW
/// back into the statement's row.
///
/// <para><b>Compiled once per row class.</b> A property maps to a column by name, so the mapping does
/// not depend on the table it fires on: <see cref="Create"/> compiles it with expression trees the
/// first time a node binds a trigger of that class, the same way <see cref="ExtensionFunctionBinder"/>
/// compiles a function adapter. The row path then holds one typed delegate call per property and no
/// reflection. Whether the mapping fits a table (each property has a column, of a type the property
/// can hold) is a separate check, <see cref="Validate"/>, that <c>CREATE TRIGGER</c> and the binding on
/// each node run against the table's current columns.</para>
///
/// <para><b>Write-back compares, it does not overwrite.</b> Before the call the adapter keeps the
/// engine values of the mapped columns. After a BEFORE call it compares each property of NEW with the
/// CLR value of its kept column (byte arrays and lists by content) and writes only the properties that
/// differ, through <see cref="TriggerRow"/>, the same path as <see cref="IWritableRow.Set(string, long)"/>.
/// So the statement coerces and validates exactly the changed columns, and an UPDATE keeps copying the
/// stored cells of the unchanged ones. An AFTER trigger's changes are never read.</para>
/// </summary>
internal abstract class TypedTriggerAdapter
{
    /// <summary>One mapped property: its CLR type and the column it reads, as the class names it.</summary>
    internal sealed record MappedProperty(PropertyInfo Property, string ColumnName, bool AcceptsNull);

    protected TypedTriggerAdapter(Type rowType, IReadOnlyList<MappedProperty> properties)
    {
        RowType = rowType;
        Properties = properties;
    }

    /// <summary>The row class.</summary>
    public Type RowType { get; }

    /// <summary>The mapped properties, in declaration order.</summary>
    public IReadOnlyList<MappedProperty> Properties { get; }

    /// <summary>
    /// Builds the adapter for <paramref name="rowType"/>. Throws
    /// <see cref="CamusDBErrorCodes.TriggerRowMappingInvalid"/> when the class cannot be a row class:
    /// no public parameterless constructor, a property of a type outside the value mapping, or two
    /// properties that map to one column.
    /// </summary>
    public static TypedTriggerAdapter Create(Type rowType)
    {
        if (!rowType.IsClass || rowType.IsAbstract || rowType.GetConstructor(Type.EmptyTypes) is null)
            throw Invalid(rowType, "the row class must be a concrete class with a public parameterless constructor");

        List<MappedProperty> properties = [];
        HashSet<string> columns = new(StringComparer.OrdinalIgnoreCase);
        List<string> problems = [];

        foreach (PropertyInfo property in rowType.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            // Public get and set only. An init-only setter carries the IsExternalInit modifier; such a
            // property cannot be written back, so it is out of the mapping, the same as a read-only one.
            if (property.GetIndexParameters().Length > 0 || property.GetMethod is not { IsPublic: true } || property.SetMethod is not { IsPublic: true } setter)
                continue;

            if (setter.ReturnParameter.GetRequiredCustomModifiers().Any(m => m.FullName == "System.Runtime.CompilerServices.IsExternalInit"))
                continue;

            string column = property.GetCustomAttribute<CamusColumnAttribute>()?.Name ?? property.Name;

            if (!ExtensionValueMapping.IsMappedType(property.PropertyType))
            {
                problems.Add($"property {property.Name} {ExtensionValueMapping.DescribeUnmapped(property.PropertyType)}");
                continue;
            }

            if (!columns.Add(column))
            {
                problems.Add($"two properties map to column '{column}'");
                continue;
            }

            properties.Add(new MappedProperty(property, column, AcceptsNull(property)));
        }

        if (problems.Count > 0)
            throw Invalid(rowType, string.Join("; ", problems));

        Type adapterType = typeof(TypedTriggerAdapter<>).MakeGenericType(rowType);
        return (TypedTriggerAdapter)Activator.CreateInstance(adapterType, BindingFlags.NonPublic | BindingFlags.Instance, null, [properties], null)!;
    }

    /// <summary>
    /// The problems of the mapping against <paramref name="columns"/>, empty when it fits: a property
    /// whose column the table lacks (or has only in a non-readable online state), and a property whose
    /// type cannot hold its column's values. Unmapped columns are not problems.
    /// </summary>
    public List<string> Validate(IReadOnlyList<TableColumnSchema> columns)
    {
        List<string> problems = [];

        foreach (MappedProperty mapped in Properties)
        {
            TableColumnSchema? column = FindColumn(columns, mapped.ColumnName);

            if (column is null)
            {
                problems.Add($"property {mapped.Property.Name} maps to column '{mapped.ColumnName}', which the table does not have");
                continue;
            }

            Type expected = ExpectedClrType(column);
            Type actual = Nullable.GetUnderlyingType(mapped.Property.PropertyType) ?? mapped.Property.PropertyType;

            if (ExtensionValueMapping.ArrayElementOf(actual) is { } element)
                actual = typeof(IReadOnlyList<>).MakeGenericType(Nullable.GetUnderlyingType(element) ?? element);

            if (actual != expected)
                problems.Add($"property {mapped.Property.Name} has type {mapped.Property.PropertyType.Name}, but column '{column.Name}' is {Describe(column)} (use {Spell(expected)})");
        }

        return problems;
    }

    /// <summary>The schema spelling of each mapped column, in property order; the columns must exist (call <see cref="Validate"/> first).</summary>
    public List<string> MappedColumnNames(IReadOnlyList<TableColumnSchema> columns) =>
        Properties.Select(p => FindColumn(columns, p.ColumnName)!.Name).ToList();

    /// <summary>
    /// Fires <paramref name="trigger"/> with typed rows built from <paramref name="oldValues"/> and
    /// <paramref name="newValues"/>. When <paramref name="writable"/> is set (BEFORE INSERT and BEFORE
    /// UPDATE), changed properties of NEW are written back through it.
    /// </summary>
    public abstract Task FireAsync(
        object trigger,
        ITriggerContext untyped,
        IReadOnlyDictionary<string, ColumnValue>? oldValues,
        IReadOnlyDictionary<string, ColumnValue>? newValues,
        TriggerRow? writable,
        string triggerName);

    /// <summary>The column of <paramref name="name"/> among the readable columns, case-insensitively.</summary>
    internal static TableColumnSchema? FindColumn(IReadOnlyList<TableColumnSchema> columns, string name)
    {
        foreach (TableColumnSchema column in columns)
        {
            if (string.Equals(column.Name, name, StringComparison.OrdinalIgnoreCase) && SchemaElementStateRules.IsReadable(column))
                return column;
        }

        return null;
    }

    /// <summary>The one CLR property type (without <c>Nullable</c>) that a column maps to, or <c>void</c> for none.</summary>
    private static Type ExpectedClrType(TableColumnSchema column) =>
        column.Type == ColumnType.Array
            ? column.ArrayElementType is { } element and not ColumnType.Bytes and not ColumnType.Array && ExpectedClrType(element) is var clr && clr != typeof(void)
                ? typeof(IReadOnlyList<>).MakeGenericType(clr)
                : typeof(void)
            : ExpectedClrType(column.Type);

    private static Type ExpectedClrType(ColumnType type) => type switch
    {
        ColumnType.Integer64 => typeof(long),
        ColumnType.Float64 => typeof(double),
        ColumnType.Float32 => typeof(float),
        ColumnType.Bool => typeof(bool),
        ColumnType.String or ColumnType.Id => typeof(string),
        ColumnType.Uuid => typeof(Guid),
        ColumnType.Bytes => typeof(byte[]),
        ColumnType.Date or ColumnType.DateTime => typeof(DateTime),
        ColumnType.Numeric => typeof(decimal),
        _ => typeof(void),
    };

    private static string Describe(TableColumnSchema column) =>
        column.Type == ColumnType.Array ? $"ARRAY({column.ArrayElementType})" : column.Type.ToString();

    private static string Spell(Type type) =>
        type == typeof(void) ? "no property type: the column has no mapping" :
        ExtensionValueMapping.ArrayElementOf(type) is { } element ? $"IReadOnlyList<{element.Name}>" : type.Name;

    private static bool AcceptsNull(PropertyInfo property)
    {
        if (Nullable.GetUnderlyingType(property.PropertyType) is not null)
            return true;

        if (property.PropertyType.IsValueType)
            return false;

        return new NullabilityInfoContext().Create(property).WriteState == NullabilityState.Nullable;
    }

    protected static CamusDBException Invalid(Type rowType, string reason) =>
        new(CamusDBErrorCodes.TriggerRowMappingInvalid, $"Typed trigger row class {rowType.FullName} cannot be mapped: {reason}");

    /// <summary>The engine value of <paramref name="column"/> in a row dictionary, or NULL when the row lacks it.</summary>
    protected static ColumnValue Lookup(IReadOnlyDictionary<string, ColumnValue> row, string column)
    {
        if (row.TryGetValue(column, out ColumnValue? value))
            return value;

        foreach (KeyValuePair<string, ColumnValue> entry in row)
        {
            if (string.Equals(entry.Key, column, StringComparison.OrdinalIgnoreCase))
                return entry.Value;
        }

        return ColumnValue.Null;
    }
}

/// <summary>The compiled mapping for one row class; see <see cref="TypedTriggerAdapter"/>.</summary>
internal sealed class TypedTriggerAdapter<TRow> : TypedTriggerAdapter where TRow : class, new()
{
    /// <summary>
    /// <c>(values, trigger) =&gt; new TRow { P0 = To(values[0]), ... }</c>, with a NULL check that
    /// throws for a property that cannot hold NULL and assigns <c>null</c> for one that can.
    /// </summary>
    private readonly Func<ColumnValue[], string, TRow> read;

    /// <summary>Per property: <c>(row, kept) =&gt; Same(row.P, To(kept))</c>.</summary>
    private readonly Func<TRow, ColumnValue, bool>[] unchanged;

    /// <summary>Per property: <c>row =&gt; From(row.P)</c>.</summary>
    private readonly Func<TRow, ColumnValue>[] get;

    private readonly string[] columnNames;

    private static readonly MethodInfo NullIntoFactory =
        typeof(TypedTriggerAdapter<TRow>).GetMethod(nameof(NullInto), BindingFlags.NonPublic | BindingFlags.Static)!;

    private static readonly MethodInfo SameMethod =
        typeof(TypedRowValues).GetMethod(nameof(TypedRowValues.Same), BindingFlags.NonPublic | BindingFlags.Static)!;

    private TypedTriggerAdapter(List<MappedProperty> properties) : base(typeof(TRow), properties)
    {
        columnNames = properties.Select(p => p.ColumnName).ToArray();
        read = CompileRead(properties);
        unchanged = new Func<TRow, ColumnValue, bool>[properties.Count];
        get = new Func<TRow, ColumnValue>[properties.Count];

        for (int i = 0; i < properties.Count; i++)
        {
            PropertyInfo property = properties[i].Property;

            ParameterExpression row = Expression.Parameter(typeof(TRow), "row");
            ParameterExpression kept = Expression.Parameter(typeof(ColumnValue), "kept");

            Expression before = Expression.Condition(
                Expression.Equal(Expression.Property(kept, nameof(ColumnValue.Type)), Expression.Constant(ColumnType.Null)),
                Expression.Default(property.PropertyType),
                ToClr(property.PropertyType, kept, property.Name, i));

            unchanged[i] = Expression.Lambda<Func<TRow, ColumnValue, bool>>(
                Expression.Call(SameMethod.MakeGenericMethod(property.PropertyType), Expression.Property(row, property), before),
                row, kept).Compile();

            get[i] = Expression.Lambda<Func<TRow, ColumnValue>>(
                ExtensionAdapterParts.ConvertResult(property.PropertyType, Expression.Property(row, property)),
                row).Compile();
        }
    }

    private static Func<ColumnValue[], string, TRow> CompileRead(List<MappedProperty> properties)
    {
        ParameterExpression values = Expression.Parameter(typeof(ColumnValue[]), "values");
        ParameterExpression trigger = Expression.Parameter(typeof(string), "trigger");
        ParameterExpression row = Expression.Variable(typeof(TRow), "row");

        List<Expression> body = [Expression.Assign(row, Expression.New(typeof(TRow)))];

        for (int i = 0; i < properties.Count; i++)
        {
            MappedProperty mapped = properties[i];
            Expression value = Expression.ArrayIndex(values, Expression.Constant(i));
            Expression isNull = Expression.Equal(Expression.Property(value, nameof(ColumnValue.Type)), Expression.Constant(ColumnType.Null));
            Expression target = Expression.Property(row, mapped.Property);

            Expression onNull = mapped.AcceptsNull
                ? Expression.Assign(target, Expression.Default(mapped.Property.PropertyType))
                : Expression.Throw(Expression.Call(NullIntoFactory, trigger, Expression.Constant(mapped.ColumnName), Expression.Constant(mapped.Property.Name)));

            body.Add(Expression.IfThenElse(
                isNull,
                Expression.Block(typeof(void), onNull),
                Expression.Assign(target, ToClr(mapped.Property.PropertyType, value, mapped.Property.Name, i))));
        }

        body.Add(row);

        return Expression.Lambda<Func<ColumnValue[], string, TRow>>(Expression.Block(typeof(TRow), [row], body), values, trigger).Compile();
    }

    /// <summary>The conversion of a non-NULL engine value to the property type, reported against the property name.</summary>
    private static Expression ToClr(Type type, Expression value, string propertyName, int index) =>
        ExtensionAdapterParts.ConvertArgument(type, acceptsNull: true, value, Expression.Constant("row property " + propertyName), Expression.Constant(index));

    public override async Task FireAsync(
        object trigger,
        ITriggerContext untyped,
        IReadOnlyDictionary<string, ColumnValue>? oldValues,
        IReadOnlyDictionary<string, ColumnValue>? newValues,
        TriggerRow? writable,
        string triggerName)
    {
        TRow? oldRow = oldValues is null ? null : read(Keep(oldValues), triggerName);

        ColumnValue[]? keptNew = newValues is null ? null : Keep(newValues);
        TRow? newRow = keptNew is null ? null : read(keptNew, triggerName);

        await ((ICamusTrigger<TRow>)trigger).FireAsync(new Context(untyped, oldRow, newRow)).ConfigureAwait(false);

        if (writable is null || newRow is null)
            return;

        for (int i = 0; i < columnNames.Length; i++)
        {
            if (!unchanged[i](newRow, keptNew![i]))
                writable.WriteValue(columnNames[i], get[i](newRow));
        }
    }

    /// <summary>The engine values of the mapped columns, in property order. The values are immutable, so
    /// a shallow copy is the snapshot the write-back compares against.</summary>
    private ColumnValue[] Keep(IReadOnlyDictionary<string, ColumnValue> row)
    {
        ColumnValue[] kept = new ColumnValue[columnNames.Length];

        for (int i = 0; i < kept.Length; i++)
            kept[i] = Lookup(row, columnNames[i]);

        return kept;
    }

    private static CamusDBException NullInto(string trigger, string column, string property) =>
        new(CamusDBErrorCodes.TriggerFailed,
            $"Trigger '{trigger}': column '{column}' is NULL, and property {typeof(TRow).Name}.{property} cannot hold NULL; " +
            "declare it Nullable<T> or as a nullable reference");

    private sealed class Context : ITriggerContext<TRow>
    {
        private readonly ITriggerContext untyped;

        public Context(ITriggerContext untyped, TRow? old, TRow? @new)
        {
            this.untyped = untyped;
            Old = old;
            New = @new;
        }

        public TRow? Old { get; }

        public TRow? New { get; }

        public string TriggerName => untyped.TriggerName;

        public string TableName => untyped.TableName;

        public TriggerTiming Timing => untyped.Timing;

        public TriggerOperation Operation => untyped.Operation;

        IRow? ITriggerContext.Old => untyped.Old;

        IWritableRow? ITriggerContext.New => untyped.New;

        public ISqlSession Sql => untyped.Sql;

        public CancellationToken CancellationToken => untyped.CancellationToken;
    }
}

/// <summary>The value comparison of the typed write-back: content for byte arrays and lists, equality otherwise.</summary>
internal static class TypedRowValues
{
    internal static bool Same<T>(T after, T before)
    {
        if (after is byte[] a && before is byte[] b)
            return a.AsSpan().SequenceEqual(b);

        if (after is System.Collections.IEnumerable list && after is not string && before is System.Collections.IEnumerable other)
            return SameItems(list, other);

        return EqualityComparer<T>.Default.Equals(after, before);
    }

    private static bool SameItems(System.Collections.IEnumerable a, System.Collections.IEnumerable b)
    {
        System.Collections.IEnumerator left = a.GetEnumerator();
        System.Collections.IEnumerator right = b.GetEnumerator();

        while (true)
        {
            bool hasLeft = left.MoveNext();

            if (hasLeft != right.MoveNext())
                return false;

            if (!hasLeft)
                return true;

            if (!Equals(left.Current, right.Current))
                return false;
        }
    }
}
