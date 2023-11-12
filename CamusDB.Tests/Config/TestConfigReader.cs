
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using CamusDB.Core.Config;

namespace CamusDB.Tests.Config;

public class TestConfigReader
{
	public TestConfigReader()
	{
	}

    [Test]
    [NonParallelizable]
    public void TestReader()
    {
        string yml = "buffer_pool_size: 1000";

        var configReader = new ConfigReader();
        var configDefinition = configReader.Read(yml);

        Assert.AreEqual(1000, configDefinition.BufferPoolSize);
    }
}

