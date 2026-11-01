
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models;

public enum AlterIndexOperation
{
    AddIndex,
    AddUniqueIndex,
    AddPrimaryKey,
    ModifyIndex,
    DropIndex,
    DropPrimaryKey,
    RenameIndex,

    /// <summary>
    /// <c>CREATE INDEX ... USING GIN</c> / <c>CREATE INVERTED INDEX</c>: a GIN index over one jsonb
    /// column (<see cref="Catalogs.Models.IndexType.Inverted"/>). Appended: the value travels in
    /// forwarded DDL requests.
    /// </summary>
    AddInvertedIndex,
}
