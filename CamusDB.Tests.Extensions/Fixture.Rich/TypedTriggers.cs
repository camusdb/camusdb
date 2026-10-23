/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CamusDB.Extensions;

namespace CamusDB.Fixtures.Rich;

/// <summary>Test-only row class over <c>orders (id, name, total_cents, tags, note)</c>.</summary>
public sealed class OrderRow
{
    public string? Id { get; set; }

    public string Name { get; set; } = "";

    [CamusColumn("total_cents")]
    public long? Total { get; set; }

    public IReadOnlyList<string>? Tags { get; set; }

    public string? Note { get; set; }
}

/// <summary>Test-only BEFORE trigger: trims the name, and adds 1 to a total of 99.</summary>
public sealed class TrimOrderName : ICamusTrigger<OrderRow>
{
    public Task FireAsync(ITriggerContext<OrderRow> context)
    {
        if (context.New is { } row)
        {
            row.Name = row.Name.Trim();

            if (row.Total == 99)
                row.Total = 100;
        }

        return Task.CompletedTask;
    }
}

/// <summary>Test-only BEFORE UPDATE trigger: writes OLD's name into NEW's note.</summary>
public sealed class NoteOldName : ICamusTrigger<OrderRow>
{
    public Task FireAsync(ITriggerContext<OrderRow> context)
    {
        if (context.New is { } row && context.Old is { } old)
            row.Note = "was " + old.Name;

        return Task.CompletedTask;
    }
}

/// <summary>Test-only BEFORE trigger: changes nothing.</summary>
public sealed class LeaveOrder : ICamusTrigger<OrderRow>
{
    public Task FireAsync(ITriggerContext<OrderRow> context) => Task.CompletedTask;
}

/// <summary>Test-only BEFORE trigger: sets a total the CHECK constraint refuses.</summary>
public sealed class NegativeTotal : ICamusTrigger<OrderRow>
{
    public Task FireAsync(ITriggerContext<OrderRow> context)
    {
        if (context.New is { } row)
            row.Total = -1;

        return Task.CompletedTask;
    }
}

/// <summary>Test-only AFTER trigger: changes its row object, which must have no effect, and logs the event.</summary>
public sealed class AfterOrderLog : ICamusTrigger<OrderRow>
{
    public async Task FireAsync(ITriggerContext<OrderRow> context)
    {
        OrderRow row = (context.New ?? context.Old)!;
        string tags = row.Tags is null ? "<null>" : string.Join("+", row.Tags);

        await context.Sql.ExecuteAsync(
            "INSERT INTO order_log (detail) VALUES (@detail)",
            new Dictionary<string, object?> { ["detail"] = $"{context.Operation}:{row.Name}:{row.Total?.ToString(CultureInfo.InvariantCulture) ?? "<null>"}:{tags}" });

        row.Name = "changed by AFTER";
    }
}

/// <summary>Test-only row class with a property whose column the table lacks.</summary>
public sealed class MissingColumnRow
{
    public string Name { get; set; } = "";

    public long Nope { get; set; }
}

public sealed class MissingColumnTrigger : ICamusTrigger<MissingColumnRow>
{
    public Task FireAsync(ITriggerContext<MissingColumnRow> context) => Task.CompletedTask;
}

/// <summary>Test-only row class with a property of the wrong type for its column.</summary>
public sealed class WrongTypeRow
{
    [CamusColumn("total_cents")]
    public string? Total { get; set; }
}

public sealed class WrongTypeTrigger : ICamusTrigger<WrongTypeRow>
{
    public Task FireAsync(ITriggerContext<WrongTypeRow> context) => Task.CompletedTask;
}

/// <summary>Test-only row class whose total cannot hold NULL.</summary>
public sealed class StrictTotalRow
{
    [CamusColumn("total_cents")]
    public long Total { get; set; }
}

public sealed class StrictTotalTrigger : ICamusTrigger<StrictTotalRow>
{
    public Task FireAsync(ITriggerContext<StrictTotalRow> context) => Task.CompletedTask;
}

/// <summary>Test-only row class with one property for each mapped type, over <c>typed_all</c>.</summary>
public sealed class AllTypesRow
{
    public long? I64 { get; set; }
    public double? F64 { get; set; }
    public float? F32 { get; set; }
    public bool? Flag { get; set; }
    public string? Txt { get; set; }
    public Guid? Uid { get; set; }
    public byte[]? Data { get; set; }
    public DateTime? Stamp { get; set; }
    public decimal? Amount { get; set; }
    public IReadOnlyList<long>? Ids { get; set; }
    public string? Summary { get; set; }
}

/// <summary>Test-only BEFORE INSERT trigger: writes what it received into <c>summary</c>.</summary>
public sealed class SummarizeAllTypes : ICamusTrigger<AllTypesRow>
{
    public Task FireAsync(ITriggerContext<AllTypesRow> context)
    {
        AllTypesRow row = context.New!;

        row.Summary = string.Join("|",
            Show(row.I64), Show(row.F64), Show(row.F32), Show(row.Flag), row.Txt ?? "<null>", Show(row.Uid),
            row.Data is null ? "<null>" : BitConverter.ToString(row.Data),
            row.Stamp is { } stamp ? stamp.Kind + ":" + stamp.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) : "<null>",
            Show(row.Amount),
            row.Ids is null ? "<null>" : string.Join("+", row.Ids));

        // A change to a byte array in place must count as a change.
        if (row.Data is { Length: > 0 } data)
            data[0] = 0xFF;

        return Task.CompletedTask;
    }

    private static string Show<T>(T? value) where T : struct =>
        value is null ? "<null>" : Convert.ToString(value.Value, CultureInfo.InvariantCulture)!;
}
