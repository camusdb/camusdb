
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Security;
using System.Threading;
using System.Runtime.CompilerServices;

namespace CamusDB.Core.Util.ObjectIds;

/**
 * ObjectId generation is inspired in the MongoId generator
 * 
 * The 12-byte ObjectId value consists of:
 * 
 * a 4-byte timestamp value, representing the ObjectId's creation, measured in seconds since the Unix epoch
 * a 5-byte random value generated once per process. This random value is unique to the machine and process.
 * a 3-byte incrementing counter, initialized to a random value
 * 
 * https://docs.mongodb.com/manual/reference/method/ObjectId/
 */
public sealed class ObjectIdGenerator
{
    private static readonly int __staticMachine = (GetMachineHash() + GetAppDomainId()) & 0x00ffffff;

    private static readonly short __staticPid = GetPid();

    private static int __staticIncrement = (new System.Random()).Next();

    private static int processId;

    private static int currentDomainId;

    private static int machineHash;

    private static readonly DateTime epoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static int GetCurrentProcessId()
    {
        if (processId == 0)
            processId = Environment.ProcessId;
        return processId;
    }

    private static int GetAppDomainId()
    {
        if (currentDomainId == 0)
            currentDomainId = AppDomain.CurrentDomain.Id;
        return currentDomainId;
    }

    private static int GetMachineHash()
    {
        if (machineHash == 0)
        {
            string machineName = GetMachineName();
            machineHash = 0x00ffffff & machineName.GetHashCode(); // use first 3 bytes of hash
        }
        return machineHash;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string GetMachineName()
    {
        return Environment.MachineName;
    }

    private static short GetPid()
    {
        try
        {
            return (short)GetCurrentProcessId(); // use low order two bytes only
        }
        catch (SecurityException)
        {
            return 0;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int GetTimestampFromDateTime(DateTime dateTime)
    {
        return (int)((dateTime.ToUniversalTime() - epoch).TotalSeconds);
    }    

    public static ObjectIdValue Generate()
    {
        int pid = __staticPid;
        int machine = __staticMachine;
        int timestamp = GetTimestampFromDateTime(DateTime.UtcNow);
        int increment = Interlocked.Increment(ref __staticIncrement) & 0x00ffffff; // only use low order 3 bytes

        if ((__staticMachine & 0xff000000) != 0)
            throw new ArgumentOutOfRangeException("machine", "The machine value must be between 0 and 16777215 (it must fit in 3 bytes).");

        if ((increment & 0xff000000) != 0)
            throw new ArgumentOutOfRangeException("increment", "The increment value must be between 0 and 16777215 (it must fit in 3 bytes).");

        int _a = timestamp;
        int _b = (machine << 8) | (((int)pid >> 8) & 0xff);
        int _c = ((int)pid << 24) | increment;        

        return new ObjectIdValue(a: _a, b: _b, c: _c);
    }    
}

