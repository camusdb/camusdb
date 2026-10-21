/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs;

/// <summary>
/// A write to a coordinator job record was refused because the record carries a newer schema-leader
/// term than the writer's: the schema leadership moved while the writer drove the job, and the next
/// leader took the job over. The writer must stop driving; the job belongs to its successor, and the
/// statement that started it answers from the job's outcome
/// (<see cref="SchemaChangeCoordinator.RunJobAsync"/>).
/// </summary>
public sealed class CoordinatorJobTakenOverException : CamusDBException
{
    /// <summary>The term the refused writer drove the job under.</summary>
    public long WriterTerm { get; }

    /// <summary>The term stored on the record, the successor's.</summary>
    public long OwnerTerm { get; }

    public CoordinatorJobTakenOverException(string tableName, string elementName, long writerTerm, long ownerTerm)
        : base(
            CamusDBErrorCodes.InvalidInternalOperation,
            $"The schema-change job for '{tableName}.{elementName}' is owned by the schema leader of term {ownerTerm}; " +
            $"this node drove it under term {writerTerm} and the leadership moved meanwhile")
    {
        WriterTerm = writerTerm;
        OwnerTerm = ownerTerm;
    }
}

/// <summary>
/// The definite answer of a staged schema change whose driver lost the schema leadership and handed
/// the job over to the next leader, which did not bring the element to its target: the successor
/// rolled the change back, with the recorded reason when it was seen, or it is still driving the job
/// after the wait. The statement that started the change fails with this; its compensation does not
/// run, because the change is not its to undo any more. The <see cref="CamusDBException.Code"/> is the
/// recorded failure's own code when the successor recorded one, so a client sees the same error it
/// would have seen from an uninterrupted statement.
/// </summary>
public sealed class SchemaChangeHandedOverException : CamusDBException
{
    public SchemaChangeHandedOverException(string code, string message) : base(code, message)
    {
    }
}
