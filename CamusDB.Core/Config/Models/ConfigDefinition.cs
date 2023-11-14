
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Config.Models;

public class ConfigDefinition
{
    public string DataDir { get; set; } = "";

    public int BufferPoolSize { get; set; } = -1;
}
