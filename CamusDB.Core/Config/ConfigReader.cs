

using CamusDB.Core.Config.Models;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace CamusDB.Core.Config;

public class ConfigReader
{
	public ConfigReader()
	{
        
    }

    public ConfigDefinition Read(string yml)
    {
        IDeserializer deserializer = new DeserializerBuilder()
            .WithNamingConvention(UnderscoredNamingConvention.Instance) 
            .Build();
        
        return deserializer.Deserialize<ConfigDefinition>(yml);
    }
}

