/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Threading;

namespace CamusDB.Extensions;

/// <summary>When a trigger fires relative to the row write.</summary>
public enum TriggerTiming
{
    /// <summary>Before the row is validated and written. NEW can be changed.</summary>
    Before = 0,

    /// <summary>After the statement wrote all its rows. The rows are final.</summary>
    After = 1
}

/// <summary>The statement kind that fired a trigger.</summary>
public enum TriggerOperation
{
    /// <summary>A row was inserted. OLD is absent.</summary>
    Insert = 0,

    /// <summary>A row was updated. OLD and NEW are both present.</summary>
    Update = 1,

    /// <summary>A row was deleted. NEW is absent.</summary>
    Delete = 2
}

/// <summary>
/// What a trigger receives for one row event.
/// </summary>
public interface ITriggerContext
{
    /// <summary>The trigger name from <c>CREATE TRIGGER</c>.</summary>
    string TriggerName { get; }

    /// <summary>The name of the table the trigger is on.</summary>
    string TableName { get; }

    /// <summary>BEFORE or AFTER.</summary>
    TriggerTiming Timing { get; }

    /// <summary>INSERT, UPDATE or DELETE.</summary>
    TriggerOperation Operation { get; }

    /// <summary>The row before the change, for UPDATE and DELETE. <c>null</c> for INSERT.</summary>
    IRow? Old { get; }

    /// <summary>
    /// The row after the change, for INSERT and UPDATE. <c>null</c> for DELETE. It is writable
    /// (<see cref="IWritableRow.IsWritable"/>) only in a BEFORE trigger.
    /// </summary>
    IWritableRow? New { get; }

    /// <summary>Runs SQL inside the statement's transaction. See <see cref="ISqlSession"/>.</summary>
    ISqlSession Sql { get; }

    /// <summary>
    /// Signals that the statement was cancelled. Cancellation is cooperative: the server cannot
    /// stop a trigger that does not observe this token. In this version the server does not cancel a
    /// statement that writes, so the token does not fire yet; observe it anyway, so a later server can
    /// stop long work cleanly.
    /// </summary>
    CancellationToken CancellationToken { get; }
}
