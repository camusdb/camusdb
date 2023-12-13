
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
        HybridLogicalClock x = new();

        Timestamp t1 = x.SendOrLocalEvent();
        Timestamp t2 = x.SendOrLocalEvent();
        Timestamp t3 = x.SendOrLocalEvent();
        Timestamp t4 = x.SendOrLocalEvent();

        await Task.Delay(1000);

        Timestamp t5 = x.SendOrLocalEvent();
        Timestamp t6 = x.SendOrLocalEvent();
        Timestamp t7 = x.SendOrLocalEvent();
        Timestamp t8 = x.SendOrLocalEvent();

        Timestamp[] events = new[] { t1, t5, t6, t7, t2, t3, t4, t8 };

        Array.Sort(events);

        for (int i = 0; i < events.Length; i++)
            Console.WriteLine(events[i]);
    }
}
