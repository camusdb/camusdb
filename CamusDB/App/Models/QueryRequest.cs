
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.App.Models;

public sealed class QueryRequest
{
    public long TxnIdPT { get; set; }

    public uint TxnIdCounter { get; set; }

    public string? DatabaseName { get; set; }

    public string? TableName { get; set; }

    public string? IndexName { get; set; }

    public List<QueryFilter>? Filters { get; set; }

    public List<QueryOrderBy>? OrderBy { get; set; }
}
