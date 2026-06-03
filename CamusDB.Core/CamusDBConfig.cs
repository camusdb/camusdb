
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core;

public static class CamusDBConfig
{
    /// <summary>
    /// The directory where the database files and directories will be stored.
    /// </summary>
    public static string DataDirectory = Path.GetFullPath("Data");

    /// <summary>
    /// The maximum number of pages held on each bucket
    /// </summary>
    public static int BufferPoolSize = 65536 / Environment.ProcessorCount;

    /// <summary>
    /// The internal name used to identify primary key indices.
    /// This name should only be changed in a new installation. Changing it after
    /// having databases with tables and data can cause unexpected problems.
    /// </summary>
    public const string PrimaryKeyInternalName = "~pk";
}
