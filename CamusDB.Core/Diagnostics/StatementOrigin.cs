/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Diagnostics;

/// <summary>
/// Where a statement came from: the transport that carried it, the client connection it arrived
/// on, and the client's address. <c>SHOW QUERIES</c> reports these columns, and
/// <c>SHOW CONNECTIONS</c> joins its rows to the query rows through <see cref="Connection"/>.
///
/// <para><b>It travels as an ambient value, not on the ticket.</b> The host sets
/// <see cref="Current"/> once per request, before any controller or gRPC method runs, and the
/// statement funnels in the engine read it when they register a statement with
/// <see cref="QueryActivityRegistry"/>. A ticket field would have to be set at every one of the
/// fifteen-plus places the transports build an <c>ExecuteSQLTicket</c>, and a site that forgot it
/// would report an anonymous statement with no error to show for it. The value is an
/// <see cref="AsyncLocal{T}"/>, so it flows into every await below the request and never leaks to a
/// concurrent request.</para>
///
/// <para>An engine driven with no host behind it — the tests, a background job — sees null and
/// reports the transport as <see cref="EmbeddedTransport"/>.</para>
/// </summary>
/// <param name="Transport">
/// Short name of the client protocol: <c>http</c>, <c>grpc</c>, <c>grpc-stream</c> for an op on a
/// <c>BatchExecute</c> stream, or <c>wasm</c>.
/// </param>
/// <param name="Connection">The client connection the request arrived on, or null when the host does not track one.</param>
/// <param name="ClientAddress">The client's IP address as text, or null when the transport has none.</param>
public sealed record StatementOrigin(string Transport, ClientConnection? Connection, string? ClientAddress)
{
    /// <summary>The transport name reported for a statement that no host request carried.</summary>
    public const string EmbeddedTransport = "embedded";

    private static readonly AsyncLocal<StatementOrigin?> current = new();

    /// <summary>
    /// The origin of the statement that runs in this async flow, or null when no host request set one.
    ///
    /// <para>Set it in the method whose awaits lead to the statement, not inside a helper that
    /// returns first: an <see cref="AsyncLocal{T}"/> assignment made inside an awaited async method
    /// is undone when that method returns.</para>
    /// </summary>
    public static StatementOrigin? Current
    {
        get => current.Value;
        set => current.Value = value;
    }
}
