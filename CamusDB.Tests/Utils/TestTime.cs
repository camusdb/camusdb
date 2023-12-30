
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Threading.Tasks;
using CamusDB.Core.Util.Time;
using NUnit.Framework;

namespace CamusDB.Tests.Utils;

public class TestTime
{
    [Test]
    public async Task TestParseSimpleSelect()
    {
        HybridLogicalClock hlc = new();

        HLCTimestamp t1 = await hlc.SendOrLocalEvent();
        HLCTimestamp t2 = await hlc.SendOrLocalEvent();
        HLCTimestamp t3 = await hlc.SendOrLocalEvent();
        HLCTimestamp t4 = await hlc.SendOrLocalEvent();

        await Task.Delay(1000);

        HLCTimestamp t5 = await hlc.SendOrLocalEvent();
        HLCTimestamp t6 = await hlc.SendOrLocalEvent();
        HLCTimestamp t7 = await hlc.SendOrLocalEvent();
        HLCTimestamp t8 = await hlc.SendOrLocalEvent();

        HLCTimestamp[] events = new[] { t1, t5, t6, t7, t2, t3, t4, t8 };

        Array.Sort(events);

        for (int i = 0; i < events.Length; i++)
            Console.WriteLine(events[i]);
    }
}
