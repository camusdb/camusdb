
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Controllers.Indexes;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class IndexSaver
{
    private readonly IndexUniqueSaver indexUniqueSaver;    

    private readonly IndexUniqueOffsetSaver indexUniqueOffsetSaver;

    public IndexSaver()
    {
        indexUniqueSaver = new(this);        
        indexUniqueOffsetSaver = new(this);
    }

    public async Task Save(SaveOffsetIndexTicket ticket)
    {
        await indexUniqueOffsetSaver.Save(ticket).ConfigureAwait(false);
    }

    public async Task Save(SaveIndexTicket ticket)
    {
        await indexUniqueSaver.Save(ticket).ConfigureAwait(false);
    }

    public async Task Remove(RemoveUniqueIndexTicket ticket)
    {
        await indexUniqueSaver.Remove(ticket).ConfigureAwait(false);        
    }

    public async Task Remove(RemoveUniqueOffsetIndexTicket ticket)
    {
        await indexUniqueOffsetSaver.Remove(ticket).ConfigureAwait(false);
    }
}
