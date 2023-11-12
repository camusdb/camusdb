
using System;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace CamusDB.Core.Config;

public class ConfigDefinition
{
    public int BufferPoolSize { get; set; } = -1;
}

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

