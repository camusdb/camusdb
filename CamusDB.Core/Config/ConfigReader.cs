
using System.Collections;
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
        ValidateUnknownKahunaKeys(yml);

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

  private static void ValidateUnknownKahunaKeys(string yml)
    {
        if (string.IsNullOrWhiteSpace(yml))
            return;

        IDeserializer raw = new DeserializerBuilder()
            .WithNamingConvention(UnderscoredNamingConvention.Instance)
            .Build();

        Dictionary<string, object>? root = raw.Deserialize<Dictionary<string, object>>(yml);
        if (root is null || !root.TryGetValue("kahuna", out object? kahunaRaw) || kahunaRaw is null)
            return;

        if (kahunaRaw is not IDictionary kahunaDict)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidConfig,
                "'kahuna' must be a mapping of option names to values");

        foreach (object key in kahunaDict.Keys)
        {
            string name = key.ToString() ?? "";
            if (!KahunaOptionsConfig.AllowedYamlKeys.Contains(name))
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidConfig,
                    $"Unknown 'kahuna' option '{name}'; allowed keys: " +
                    string.Join(", ", KahunaOptionsConfig.AllowedYamlKeys.OrderBy(k => k)));
            }
        }
    }
}
