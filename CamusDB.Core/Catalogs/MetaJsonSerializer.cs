
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace CamusDB.Core.Catalogs;

internal static class MetaJsonSerializer
{
    internal static byte[] Serialize<T>(T value, JsonTypeInfo<T> typeInfo)
    {
        return JsonSerializer.SerializeToUtf8Bytes(value, typeInfo);
    }

    internal static T Deserialize<T>(ReadOnlySpan<byte> buffer, JsonTypeInfo<T> typeInfo)
    {
        T? value = JsonSerializer.Deserialize(buffer, typeInfo);
        return value ?? throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"Failed to decode meta value '{typeof(T).Name}'");
    }

    internal static T DeserializeCompat<T>(ReadOnlySpan<byte> buffer, JsonTypeInfo<T> typeInfo)
    {
        try
        {
            return Deserialize(buffer, typeInfo);
        }
        catch (JsonException)
        {
            string legacyJson = Encoding.Unicode.GetString(buffer);
            T? value = JsonSerializer.Deserialize(legacyJson, typeInfo);
            return value ?? throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"Failed to decode legacy meta value '{typeof(T).Name}'");
        }
    }

    internal static string DecodeJsonTextCompat(ReadOnlySpan<byte> buffer)
    {
        try
        {
            using JsonDocument document = JsonDocument.Parse(buffer.ToArray());
            return Encoding.UTF8.GetString(buffer);
        }
        catch (JsonException)
        {
            return Encoding.Unicode.GetString(buffer);
        }
    }
}
