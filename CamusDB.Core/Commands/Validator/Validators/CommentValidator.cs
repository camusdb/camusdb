/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsValidator.Validators;

/// <summary>
/// Shape and bound checks for <see cref="CommentTicket"/>. Deliberately does <b>not</b> check that
/// the target table/column/index exists — that resolution happens at execution against the opened
/// schema, so the caller gets the same not-found errors as every other DDL statement.
/// </summary>
internal sealed class CommentValidator : ValidatorBase
{
    public void Validate(CommentTicket ticket)
    {
        if (string.IsNullOrWhiteSpace(ticket.DatabaseName))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Database name is required");

        if (ticket.Target != CommentTarget.Database && string.IsNullOrWhiteSpace(ticket.TableName))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Table name is required");

        if (ticket.Target is CommentTarget.Column or CommentTarget.Index &&
            string.IsNullOrWhiteSpace(ticket.ElementName))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"{(ticket.Target == CommentTarget.Column ? "Column" : "Index")} name is required");

        // Null is legal — it is how a comment is removed — so only a present value is checked.
        ValidateCommentLength(ticket.Comment, "Comment");
        ValidateCommentIsRepresentable(ticket.Comment, "Comment");
    }
}
