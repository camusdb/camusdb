
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

    public string Mode { get; set; } = "standalone";

    public string NodeName { get; set; } = "";

    public string RaftHost { get; set; } = "localhost";

    public int RaftPort { get; set; } = 7070;

    public int InitialPartitions { get; set; } = 1;

    public List<string> Peers { get; set; } = [];

    public bool IsClusterMode => Mode == "cluster" || Peers.Count > 0;
}
