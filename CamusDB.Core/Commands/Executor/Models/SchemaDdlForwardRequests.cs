
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Models;

// Serialization DTOs for the /internal/schema-ddl/* HTTP forwarding endpoints.
// Separate from the core ticket structs to avoid coupling System.Text.Json
// constructor-matching constraints to the internal command model.

public sealed class ColumnInfoRequest
{
    public string Name { get; set; } = "";
    public ColumnType Type { get; set; }
    public bool NotNull { get; set; }
    public ColumnValue? Default { get; set; }
    public int? MaxLength { get; set; }
    public ColumnType? ArrayElementType { get; set; }

    /// <summary>Name of a nullary volatile function default (e.g. <c>gen_uuid_v7</c>); null when absent.</summary>
    public string? DefaultFunction { get; set; }

    /// <summary>Name of a <c>CONSTRAINT name NOT NULL</c> declared on the column; null for bare NOT NULL.</summary>
    public string? NotNullConstraintName { get; set; }
}

/// <summary>
/// Wire form of a table-level (or desugared column-level) CHECK constraint carried on a
/// forwarded CREATE TABLE. Without this the leader would rebuild the table with no checks.
/// </summary>
public sealed class CheckConstraintInfoRequest
{
    public string Name { get; set; } = "";
    public string Expression { get; set; } = "";
    public string[] ReferencedColumns { get; set; } = [];
}

public sealed class ColumnIndexInfoRequest
{
    public string Name { get; set; } = "";
    public OrderType Order { get; set; }
}

public sealed class ConstraintInfoRequest
{
    public ConstraintType Type { get; set; }
    public string Name { get; set; } = "";
    public ColumnIndexInfoRequest[] Columns { get; set; } = [];
}

public sealed class ForwardCreateTableRequest
{
    public string OperationId { get; set; } = "";
    public string DatabaseName { get; set; } = "";
    public string TableName { get; set; } = "";
    public ColumnInfoRequest[] Columns { get; set; } = [];
    public ConstraintInfoRequest[] Constraints { get; set; } = [];
    public CheckConstraintInfoRequest[] CheckConstraints { get; set; } = [];
    public bool IfNotExists { get; set; }
}

public sealed class ForwardAlterTableRequest
{
    public string OperationId { get; set; } = "";
    public string DatabaseName { get; set; } = "";
    public string TableName { get; set; } = "";
    public AlterTableOperation Operation { get; set; }
    public ColumnInfoRequest Column { get; set; } = new();
    public string? NewName { get; set; }
}

public sealed class ForwardAlterIndexRequest
{
    public string OperationId { get; set; } = "";
    public string DatabaseName { get; set; } = "";
    public string TableName { get; set; } = "";
    public string IndexName { get; set; } = "";
    public ColumnIndexInfoRequest[] Columns { get; set; } = [];
    public AlterIndexOperation Operation { get; set; }
    public bool IfNotExists { get; set; }
    public string? NewName { get; set; }
}

public sealed class ForwardDropTableRequest
{
    public string OperationId { get; set; } = "";
    public string DatabaseName { get; set; } = "";
    public string TableName { get; set; } = "";
    public bool IfExists { get; set; }
}

public sealed class ForwardAlterConstraintRequest
{
    public string OperationId { get; set; } = "";
    public string DatabaseName { get; set; } = "";
    public string TableName { get; set; } = "";
    public string ConstraintName { get; set; } = "";
    public string? Expression { get; set; }
    public string[]? ReferencedColumns { get; set; }
    public AlterConstraintOperation Operation { get; set; }

    /// <summary>Target column for SET/DROP NOT NULL; null for CHECK add/drop. Without it the
    /// leader cannot resolve the column and the NOT NULL alter fails.</summary>
    public string? ColumnName { get; set; }
}

public sealed class ForwardRenameTableRequest
{
    public string OperationId { get; set; } = "";
    public string DatabaseName { get; set; } = "";
    public string TableName { get; set; } = "";
    public string NewName { get; set; } = "";
}

public sealed class SchemaDdlForwardResponse
{
    // "ok" | "not-leader" | "failed"
    public string Status { get; set; } = "";
    public bool Applied { get; set; }
    public string? Code { get; set; }
    public string? Message { get; set; }
}

/// <summary>
/// Payload for the follower→leader schema-apply acknowledgement. A follower posts this
/// to the leader's <c>/internal/schema-ddl/schema-ack</c> endpoint after applying each
/// schema version, so the leader's <c>SchemaAckTracker</c> observes real follower progress.
/// </summary>
public sealed class SchemaAckRequest
{
    public string Database { get; set; } = "";
    public string NodeEndpoint { get; set; } = "";
    public long AppliedSchemaVersion { get; set; }
}
