/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json;
using System.Text.Json.Nodes;

using NUnit.Framework;

namespace CamusDB.Grpc.Client.Tests;

/// <summary>
/// The client side of jsonb: a JSON document, element or node binds as <c>json_value</c> with its
/// text unchanged (exact numbers included), a string stays a string, and a jsonb cell reads back as
/// its text.
/// </summary>
[TestFixture]
public sealed class TestCamusValueJsonb
{
    [Test]
    public void JsonTypes_BindAsJsonValue_WithTheirText()
    {
        using JsonDocument document = JsonDocument.Parse("{\"n\": 12345678901234567890.50, \"a\": [true]}");

        Value fromDocument = CamusValue.From(document);
        Assert.AreEqual(Value.KindOneofCase.JsonValue, fromDocument.KindCase);
        Assert.AreEqual("{\"n\": 12345678901234567890.50, \"a\": [true]}", fromDocument.JsonValue, "the digits travel unchanged");

        Assert.AreEqual(Value.KindOneofCase.JsonValue, CamusValue.From(document.RootElement.GetProperty("a")).KindCase);
        Assert.AreEqual("[true]", CamusValue.From(document.RootElement.GetProperty("a")).JsonValue);

        Value fromNode = CamusValue.From(new JsonObject { ["k"] = 1 });
        Assert.AreEqual(Value.KindOneofCase.JsonValue, fromNode.KindCase);
        Assert.AreEqual("{\"k\":1}", fromNode.JsonValue);

        Assert.AreEqual(Value.KindOneofCase.StringValue, CamusValue.From("{\"k\": 1}").KindCase, "a string is never guessed to be a document");
    }

    [Test]
    public void TryGetJson_ReadsADocumentOnly()
    {
        Assert.IsTrue(CamusValue.TryGetJson(new Value { JsonValue = "{\"a\": 1}" }, out string json));
        Assert.AreEqual("{\"a\": 1}", json);

        Assert.IsFalse(CamusValue.TryGetJson(new Value { StringValue = "{\"a\": 1}" }, out _));
        Assert.IsFalse(CamusValue.TryGetJson(new Value { NullValue = NullValue.Unset }, out _));
    }
}
