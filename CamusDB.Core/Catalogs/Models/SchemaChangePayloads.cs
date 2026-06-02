
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Catalogs.Models;

public sealed class SchemaCreateTablePayload
{
    public string TableName { get; set; } = "";

    public SchemaColumnPayload[] Columns { get; set; } = [];
}

public sealed class SchemaAlterColumnPayload
{
    public string TableName { get; set; } = "";

    public SchemaColumnPayload Column { get; set; } = new();
}

public sealed class SchemaColumnPayload
{
    public string Name { get; set; } = "";

    public ColumnType Type { get; set; }

    public bool NotNull { get; set; }

    public ColumnValue? DefaultValue { get; set; }

    public static SchemaColumnPayload FromColumnInfo(ColumnInfo column)
    {
        return new()
        {
            Name = column.Name,
            Type = column.Type,
            NotNull = column.NotNull,
            DefaultValue = column.Default
        };
    }
}

public sealed class SchemaDropTablePayload
{
    public string TableName { get; set; } = "";
}

public sealed class SchemaIndexPayload
{
    public string TableName { get; set; } = "";

    public TableIndexSchema? Index { get; set; }

    public string IndexName { get; set; } = "";
}

public sealed class SchemaElementStatePayload
{
    public string TableName { get; set; } = "";

    public string ElementName { get; set; } = "";

    public string State { get; set; } = "";
}
