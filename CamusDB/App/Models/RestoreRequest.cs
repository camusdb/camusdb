/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.App.Models;

/// <summary>
/// Body for the restore endpoint. <see cref="TargetTimeMs"/> is Unix epoch milliseconds; <c>0</c> (the
/// default) means "latest recoverable point in the chain".
/// </summary>
public sealed class RestoreRequest
{
    public string? LeafBackupId { get; set; }

    public string? TargetDir { get; set; }

    public long TargetTimeMs { get; set; }
}
