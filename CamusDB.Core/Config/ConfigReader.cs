

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

        ConfigDefinition config = deserializer.Deserialize<ConfigDefinition>(yml) ?? new ConfigDefinition();

        // Fail fast on a malformed config rather than producing confusing behaviour later
        // (e.g. a zero ack timeout that makes the two-version gate give up instantly, or
        // an http_peers list that silently disables the explicit forwarding map).
        config.Validate();

        return config;
    }
}

