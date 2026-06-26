
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Util.ObjectIds;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core;

internal static partial class Log
{
    // Catalog / schema

    [LoggerMessage(Level = LogLevel.Information, Message = "Added table {TableName} to schema")]
    public static partial void LogTableAddedToSchema(ILogger logger, string tableName);

    [LoggerMessage(Level = LogLevel.Information, Message = "Modifed table {TableName} schema")]
    public static partial void LogTableSchemaModified(ILogger logger, string tableName);

    [LoggerMessage(Level = LogLevel.Critical, Message = "Schema checkpoint persist exhausted all {MaxAttempts} attempts for database {DbName} version {Version}; marking node degraded and scheduling schema partition step-down")]
    public static partial void LogSchemaCheckpointExhausted(ILogger logger, Exception ex, int maxAttempts, string dbName, long version);

    [LoggerMessage(Level = LogLevel.Information, Message = "Schema loaded: {Tables} table(s), {Indexes} index object(s)")]
    public static partial void LogSchemaLoaded(ILogger logger, int tables, int indexes);

    [LoggerMessage(Level = LogLevel.Information, Message = "Resuming coordinator job for {TableName}.{ElementName} → {TargetState} on database {DbName} (attempt {Attempt})")]
    public static partial void LogResumingCoordinatorJob(ILogger logger, string? tableName, string? elementName, SchemaElementState targetState, string dbName, int attempt);

    [LoggerMessage(Level = LogLevel.Information, Message = "Applied schema change {SchemaOp} for database {DbName}: {FromVersion}->{ToVersion}")]
    public static partial void LogSchemaChangeApplied(ILogger logger, SchemaOp schemaOp, string dbName, long fromVersion, long toVersion);

    [LoggerMessage(Level = LogLevel.Information, Message = "F1b: schema checkpoint re-persisted at version {Version} for database {DbName} after log restore")]
    public static partial void LogSchemaCheckpointRepersisted(ILogger logger, long version, string dbName);

    [LoggerMessage(Level = LogLevel.Information, Message = "Restored schema change {SchemaOp} for database {DbName}: {FromVersion}->{ToVersion}")]
    public static partial void LogSchemaChangeRestored(ILogger logger, SchemaOp schemaOp, string dbName, long fromVersion, long toVersion);

    // CommandExecutor

    [LoggerMessage(Level = LogLevel.Information, Message = "Backfilled {Rows} rows into index {IndexName}")]
    public static partial void LogIndexBackfillComplete(ILogger logger, int rows, string indexName);

    // Database lifecycle

    [LoggerMessage(Level = LogLevel.Information, Message = "Database {Name} closed")]
    public static partial void LogDatabaseClosed(ILogger logger, string name);

    [LoggerMessage(Level = LogLevel.Information, Message = "Database {Name} successfully created at {DbPath}")]
    public static partial void LogDatabaseCreated(ILogger logger, string name, string dbPath);

    [LoggerMessage(Level = LogLevel.Information, Message = "Database {Name} dropped")]
    public static partial void LogDatabaseDropped(ILogger logger, string name);

    [LoggerMessage(Level = LogLevel.Information, Message = "Database {DbName} opened")]
    public static partial void LogDatabaseOpened(ILogger logger, string dbName);

    // Table lifecycle

    [LoggerMessage(Level = LogLevel.Information, Message = "Registered table {TableName} in system space")]
    public static partial void LogTableRegisteredInSystemSpace(ILogger logger, string tableName);

    [LoggerMessage(Level = LogLevel.Information, Message = "Removed table {TableName} from database schema")]
    public static partial void LogTableRemovedFromDatabaseSchema(ILogger logger, string tableName);

    [LoggerMessage(Level = LogLevel.Information, Message = "Removed table {TableName} from system schema")]
    public static partial void LogTableRemovedFromSystemSchema(ILogger logger, string tableName);

    [LoggerMessage(Level = LogLevel.Information, Message = "Dropped table {TableName}")]
    public static partial void LogTableDropped(ILogger logger, string tableName);

    [LoggerMessage(Level = LogLevel.Information, Message = "Table {TableName} opened")]
    public static partial void LogTableOpened(ILogger logger, string tableName);

    // Query execution

    [LoggerMessage(Level = LogLevel.Information, Message = "Executing step {Type}")]
    public static partial void LogExecutingQueryStep(ILogger logger, QueryPlanStepType type);

    // DML

    [LoggerMessage(Level = LogLevel.Information, Message = "Row with rowid {RowId} deleted")]
    public static partial void LogRowDeleted(ILogger logger, ObjectIdValue rowId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Deleted {Rows} rows, Time taken: {Time}")]
    public static partial void LogRowsDeleted(ILogger logger, int rows, TimeSpan time);

    [LoggerMessage(Level = LogLevel.Information, Message = "Inserted {Rows} rows, Time taken: {Time}")]
    public static partial void LogRowsInserted(ILogger logger, int rows, TimeSpan time);

    [LoggerMessage(Level = LogLevel.Information, Message = "Row with rowid {RowId} updated")]
    public static partial void LogRowUpdated(ILogger logger, ObjectIdValue rowId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Updated {Rows} rows, Time taken: {Time}")]
    public static partial void LogRowsUpdated(ILogger logger, int rows, TimeSpan time);

    // DDL

    [LoggerMessage(Level = LogLevel.Information, Message = "Backfilled {ModifiedRows} rows for column '{ColumnName}'")]
    public static partial void LogColumnBackfillComplete(ILogger logger, int modifiedRows, string columnName);

    [LoggerMessage(Level = LogLevel.Information, Message = "Column added, modified {ModifiedRows} rows, Time taken: {Time}")]
    public static partial void LogColumnAdded(ILogger logger, int modifiedRows, TimeSpan time);

    [LoggerMessage(Level = LogLevel.Information, Message = "Column dropped, modified {Rows} rows, Time taken: {Time}")]
    public static partial void LogColumnDropped(ILogger logger, int rows, TimeSpan time);

    [LoggerMessage(Level = LogLevel.Information, Message = "Added {Rows} rows to index {IndexName}")]
    public static partial void LogIndexRowsAdded(ILogger logger, int rows, string indexName);

    [LoggerMessage(Level = LogLevel.Information, Message = "Added index {IndexName} to {TableName}, Time taken: {Time}")]
    public static partial void LogIndexAdded(ILogger logger, string indexName, string tableName, TimeSpan time);

    [LoggerMessage(Level = LogLevel.Information, Message = "Purged {Count} KV entries for index {IndexName}")]
    public static partial void LogIndexEntriesPurged(ILogger logger, int count, string indexName);

    // Schema DDL forwarding

    [LoggerMessage(Level = LogLevel.Debug, Message = "Schema ack to {Endpoint} failed (transport error); will retry on next apply")]
    public static partial void LogSchemaAckFailed(ILogger logger, Exception ex, Uri endpoint);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Schema ack to {Endpoint} timed out; will retry on next apply")]
    public static partial void LogSchemaAckTimedOut(ILogger logger, Exception ex, Uri endpoint);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Forwarding DDL {Operation} to {Endpoint}")]
    public static partial void LogForwardingDdl(ILogger logger, string operation, Uri endpoint);

    // Locking (transaction lock acquisition — Debug level; enable to trace which locks a query takes)

    [LoggerMessage(Level = LogLevel.Debug, Message = "Lock acquired: {Mode} range on '{Bucket}' [{Start} (incl={StartInclusive}) .. {End} (incl={EndInclusive})] tx={TxId}")]
    public static partial void LogRangeLockAcquired(ILogger logger, string mode, string bucket, string start, bool startInclusive, string end, bool endInclusive, string txId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Lock acquired: Exclusive point on '{Key}' tx={TxId}")]
    public static partial void LogPointLockAcquired(ILogger logger, string key, string txId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Lock released: {Mode} range on '{Bucket}' [{Start} .. {End}] tx={TxId}")]
    public static partial void LogRangeLockReleased(ILogger logger, string mode, string bucket, string start, string end, string txId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Transaction {Outcome}: tx={TxId}, released {PointLocks} exclusive point lock(s)")]
    public static partial void LogTransactionFinalized(ILogger logger, string outcome, string txId, int pointLocks);

    // SQL parser cache

    [LoggerMessage(Level = LogLevel.Debug, Message = "SQL parser cache sweep: {Count} entries, {Hits} hits, {Misses} misses, {Evictions} evictions (cumulative)")]
    public static partial void LogParserCacheSweep(ILogger logger, int count, long hits, long misses, long evictions);
}
