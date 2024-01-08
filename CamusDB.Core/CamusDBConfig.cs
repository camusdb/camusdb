
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;

namespace CamusDB.Core;

public static class CamusDBConfig
{
    /// <summary>
    /// The directory where the database files and directories will be stored.
    /// </summary>
    public static string DataDirectory = Path.GetFullPath("Data");

    #region bufferpool    

    public const int PageSize = 4096;

    /// <summary>
    /// Number of buffer pool buckets
    /// </summary>
    public static int NumberBuckets = Environment.ProcessorCount; 

    /// <summary>
    /// The maximum number of pages held on each bucket
    /// </summary>
    public static int BufferPoolSize = 65536 / Environment.ProcessorCount;

    #endregion

    #region GC

    /// <summary>
    /// The maximum percentage of pages per bucket allowed before starting the release process.
    /// </summary>
    public static float GCMaxPercentToStartPagesRelease = 0.8f;

    /// <summary>
    /// The maximum percentage of pages that are released in each release period.
    /// </summary>
    public static float GCPercentToReleasePerCycle = 0.05f;

    /// <summary>
    /// The interval in seconds in which expired pages will be checked for.
    /// </summary>
    public static int GCPagesIntervalSeconds = 60;

    /// <summary>
    /// The interval in seconds at which old expired MVCC versions will be released.
    /// </summary>
    public static int GCIndexIntervalSeconds = 60;

    #endregion

    #region system schema

    /// <summary>
    /// The internal name used to identify primary key indices.
    /// This name should only be changed in a new installation.Changing it after
    /// having databases with tables and data can cause unexpected problems.
    /// </summary>
    public const string PrimaryKeyInternalName = "~pk";

    public const string InformationSchemaInternalName = "information_schema";

    #endregion   

    #region keys
    public readonly static byte[] SchemaKey = Encoding.Unicode.GetBytes("schema");

    public readonly static byte[] SystemKey = Encoding.Unicode.GetBytes("system");    
    #endregion
}
